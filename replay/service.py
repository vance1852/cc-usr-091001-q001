"""调度回放服务：编排接入、版本化回放、封存与追溯。

生命周期：

    register_capability → create_shift → ingest（可多次）
        → replay(as_of=交班时刻) 得到“当时视图” V0
        → ingest(迟到补传片段) → replay() 得到“最新视图” V1
        → seal_shift() 把 V0 钉为签认版本
        → 封存后补录仍可追加，但只能产生 post_seal 新版本，
          已签认版本一个字节都不会变（库内触发器 + 内容哈希双重保证）。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .engine import (
    Capability,
    Fact,
    diff_replays,
    normalize,
    parse_dt,
    reconstruct,
)
from .hashing import sha12


class ReplayError(RuntimeError):
    """业务可预期错误，code 供调用方分支处理。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ReplayService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        from .db import open_db
        self.conn = open_db(db_path)

    # ------------------------------------------------------------------ 基础

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "ReplayService":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @staticmethod
    def _content_hash(event: dict[str, Any]) -> str:
        """对规范化后的业务内容取哈希（不含接收审计列）。"""
        clone = {k: v for k, v in event.items() if k != "received_at"}
        return sha12(clone)

    # ------------------------------------------------------------ 能力档案

    def register_capability(self, spec: dict[str, Any]) -> str:
        cap = Capability.from_dict(spec)
        content = cap.to_dict()
        h = sha12(content)
        try:
            self.conn.execute(
                "INSERT INTO capability_profiles(profile_id, content_hash, spec_json, created_at)"
                " VALUES(?,?,?,?)",
                (cap.profile_id, h, json.dumps(content, ensure_ascii=False), _now()),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            row = self.conn.execute(
                "SELECT content_hash FROM capability_profiles WHERE profile_id=?",
                (cap.profile_id,),
            ).fetchone()
            if row["content_hash"] != h:
                raise ReplayError(
                    "PROFILE_CONFLICT",
                    f"能力档案 {cap.profile_id} 已存在且内容不同，档案不可覆盖",
                )
        return cap.profile_id

    def _load_capability(self, profile_id: str) -> Capability:
        row = self.conn.execute(
            "SELECT spec_json FROM capability_profiles WHERE profile_id=?", (profile_id,)
        ).fetchone()
        if row is None:
            raise ReplayError("UNKNOWN_PROFILE", f"未知能力档案: {profile_id}")
        return Capability.from_dict(json.loads(row["spec_json"]))

    # ---------------------------------------------------------------- 班次

    def create_shift(
        self,
        shift_id: str,
        profile_id: str,
        starts_at: str,
        ends_at: str,
        handover_at: str,
    ) -> dict[str, Any]:
        self._load_capability(profile_id)  # 未知档案直接抛 UNKNOWN_PROFILE
        try:
            self.conn.execute(
                "INSERT INTO shifts(shift_id, profile_id, starts_at, ends_at, handover_at,"
                " sealed, created_at) VALUES(?,?,?,?,?,0,?)",
                (shift_id, profile_id, starts_at, ends_at, handover_at, _now()),
            )
            self.conn.commit()
        except sqlite3.IntegrityError as exc:
            raise ReplayError("SHIFT_EXISTS", f"班次 {shift_id} 已存在") from exc
        return self.get_shift(shift_id)

    def get_shift(self, shift_id: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM shifts WHERE shift_id=?", (shift_id,)).fetchone()
        if row is None:
            raise ReplayError("UNKNOWN_SHIFT", f"未知班次: {shift_id}")
        return dict(row)

    def seal_shift(self, shift_id: str, handover_at: str | None = None) -> dict[str, Any]:
        """封存班次：以交班时刻重建并钉死签认版本。

        封存是单向操作。签认版本之后永不可改、不可删；后续补录的遥测
        只能另派 post_seal 版本。
        """
        shift = self.get_shift(shift_id)
        if shift["sealed"]:
            raise ReplayError("SHIFT_SEALED", f"班次 {shift_id} 已封存，不可重复封存")
        ho = handover_at or shift["handover_at"]
        signed = self.replay(shift_id, as_of=ho, note="交班签认版本")
        try:
            self.conn.execute(
                "UPDATE shifts SET sealed=1, sealed_at=?, signed_version_id=?,"
                " handover_at=? WHERE shift_id=? AND sealed=0",
                (_now(), signed["version_id"], ho, shift_id),
            )
            self.conn.commit()
        except sqlite3.IntegrityError as exc:
            raise ReplayError("SHIFT_SEALED", "封存被库内保护拒绝") from exc
        return {"shift_id": shift_id, "signed_version_id": signed["version_id"], "sealed_at": _now()}

    # ---------------------------------------------------------------- 接入

    def ingest(self, shift_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
        """幂等接入一批现场事件。

        * 同一 event_id + 同一业务内容再次到达 ⇒ 记为重送，只累加审计计数，
          不产生任何新版本、不改变任何结论；
        * 同一 event_id 但业务内容不同 ⇒ CONTENT_CONFLICT，整批拒绝；
        * 未知扩展字段原样保留。
        """
        self.get_shift(shift_id)  # 班次必须存在
        new_ids: list[str] = []
        dup_ids: list[str] = []
        conflicts: list[dict[str, Any]] = []

        # 批内先自检：同批内同号异义也是协议事故
        seen: dict[str, str] = {}
        prepared: list[tuple[dict[str, Any], Fact, str]] = []
        for raw in events:
            fact = normalize(raw)
            if fact.kind not in ("dispatch", "telemetry"):
                raise ReplayError("UNKNOWN_KIND", f"不支持的事件类型 {fact.kind}: {fact.event_id}")
            h = self._content_hash(raw)
            if fact.event_id in seen and seen[fact.event_id] != h:
                conflicts.append({"event_id": fact.event_id, "problem": "batch_self_conflict"})
            seen.setdefault(fact.event_id, h)
            prepared.append((raw, fact, h))
        if conflicts:
            raise ReplayError("CONTENT_CONFLICT", f"批内同号异义: {conflicts}")

        batch_id = sha12([shift_id, _now(), [p[2] for p in prepared]])
        for raw, fact, h in prepared:
            existing = self.conn.execute(
                "SELECT content_hash FROM facts WHERE shift_id=? AND event_id=?",
                (shift_id, fact.event_id),
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != h:
                    conflicts.append({"event_id": fact.event_id, "problem": "content_changed"})
                    continue
                dup_ids.append(fact.event_id)
                self.conn.execute(
                    "UPDATE facts SET last_seen_at=?, resend_count=resend_count+1"
                    " WHERE shift_id=? AND event_id=?",
                    (_now(), shift_id, fact.event_id),
                )
                continue

            first_rx = fact.received_at.isoformat() if fact.received_at else fact.occurred_at.isoformat()
            try:
                self.conn.execute(
                    "INSERT INTO facts(event_id, shift_id, kind, sequence, occurred_at,"
                    " first_received_at, last_seen_at, resend_count, payload_json, extra_json,"
                    " content_hash) VALUES(?,?,?,?,?,?,?,0,?,?,?)",
                    (
                        fact.event_id, shift_id, fact.kind, fact.sequence,
                        fact.occurred_at.isoformat(), first_rx, _now(),
                        json.dumps(fact.payload, ensure_ascii=False, default=str),
                        json.dumps(fact.extra, ensure_ascii=False, default=str), h,
                    ),
                )
                new_ids.append(fact.event_id)
            except sqlite3.IntegrityError:
                # 并发进程抢先写入：按重送/冲突复核
                existing = self.conn.execute(
                    "SELECT content_hash FROM facts WHERE shift_id=? AND event_id=?",
                    (shift_id, fact.event_id),
                ).fetchone()
                if existing and existing["content_hash"] == h:
                    dup_ids.append(fact.event_id)
                else:
                    conflicts.append({"event_id": fact.event_id, "problem": "content_changed"})

        if conflicts:
            self.conn.rollback()
            raise ReplayError("CONTENT_CONFLICT", f"同号事件内容不一致，整批未入库: {conflicts}")

        fingerprint = sha12([shift_id, [p[2] for p in prepared]])
        self.conn.execute(
            "INSERT INTO ingest_batches(batch_id, shift_id, ingested_at, event_count,"
            " new_count, duplicate_count, raw_fingerprint) VALUES(?,?,?,?,?,?,?)",
            (batch_id, shift_id, _now(), len(prepared), len(new_ids), len(dup_ids), fingerprint),
        )
        self.conn.commit()
        return {
            "batch_id": batch_id,
            "shift_id": shift_id,
            "received": len(prepared),
            "new": new_ids,
            "duplicates": dup_ids,
            "duplicate_count": len(dup_ids),
        }

    def list_facts(self, shift_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event_id, kind, sequence, occurred_at, first_received_at, last_seen_at,"
            " resend_count, content_hash, payload_json, extra_json"
            " FROM facts WHERE shift_id=? ORDER BY occurred_at, sequence",
            (shift_id,),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d.pop("payload_json"))
            d["extra"] = json.loads(d.pop("extra_json"))
            out.append(d)
        return out

    # ---------------------------------------------------------------- 回放

    def _load_facts(self, shift_id: str) -> list[tuple[Fact, str]]:
        rows = self.conn.execute(
            "SELECT * FROM facts WHERE shift_id=? ORDER BY occurred_at, sequence", (shift_id,)
        ).fetchall()
        result: list[tuple[Fact, str]] = []
        for r in rows:
            payload = json.loads(r["payload_json"])
            extra = json.loads(r["extra_json"])
            raw = {"event_id": r["event_id"], "kind": r["kind"], "sequence": r["sequence"],
                   "occurred_at": r["occurred_at"], **payload, **extra}
            if r["first_received_at"] and r["first_received_at"] != r["occurred_at"]:
                raw["received_at"] = r["first_received_at"]
            result.append((normalize(raw), r["content_hash"]))
        return result

    def replay(
        self,
        shift_id: str,
        as_of: str | datetime | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """内容寻址回放：同一份输入集合永远得到同一个 version_id。

        重放顺序、接收时刻不同但可见集合相同 ⇒ 同一版本（幂等）。
        """
        shift = self.get_shift(shift_id)
        as_of_dt = parse_dt(as_of) if isinstance(as_of, str) else as_of
        cap = self._load_capability(shift["profile_id"])
        all_facts = self._load_facts(shift_id)

        as_of_key = as_of_dt.timestamp() if as_of_dt else float("inf")
        visible = [(f, h) for (f, h) in all_facts if f.received_key <= as_of_key + 1e-9]
        late = [(f, h) for (f, h) in all_facts if f.received_key > as_of_key + 1e-9]

        profile_hash_row = self.conn.execute(
            "SELECT content_hash FROM capability_profiles WHERE profile_id=?",
            (shift["profile_id"],),
        ).fetchone()
        profile_hash = profile_hash_row["content_hash"]
        facts_hash = sha12(sorted(h for _, h in visible))
        # 迟到集也进指纹：补传后重建同一交盘点会派生新版本（判定不变、时间线多迟到标注）
        late_hash = sha12(sorted(h for _, h in late))
        inputs_fingerprint = sha12(
            [shift_id, profile_hash, facts_hash, late_hash,
             as_of_dt.isoformat() if as_of_dt else None]
        )

        existing = self.conn.execute(
            "SELECT version_id, result_json FROM replay_versions WHERE inputs_fingerprint=?",
            (inputs_fingerprint,),
        ).fetchone()
        if existing is not None:
            return {**json.loads(existing["result_json"]),
                    "version_id": existing["version_id"], "deduped": True}

        # 全部事实交给引擎：判定只采用 as_of 前到站者，迟到者在时间线留痕
        view = reconstruct([f for f, _ in all_facts], cap, as_of=as_of_dt)
        result_hash = sha12(view)
        version_id = "ver_" + sha12([inputs_fingerprint, result_hash])
        post_seal = 1 if shift["sealed"] else 0
        stored = {**view, "note": note, "result_hash": result_hash,
                  "post_seal": bool(post_seal)}

        self.conn.execute(
            "INSERT INTO replay_versions(version_id, shift_id, as_of, profile_hash, facts_hash,"
            " inputs_fingerprint, result_hash, result_json, created_at, post_seal)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (version_id, shift_id, as_of_dt.isoformat() if as_of_dt else None,
             profile_hash, facts_hash, inputs_fingerprint, result_hash,
             json.dumps(stored, ensure_ascii=False), _now(), post_seal),
        )
        for f, h in all_facts:
            self.conn.execute(
                "INSERT OR IGNORE INTO version_facts(version_id, event_id, content_hash, visible)"
                " VALUES(?,?,?,?)",
                (version_id, f.event_id, h,
                 1 if f.received_key <= as_of_key + 1e-9 else 0),
            )
        for d in view["decisions"]:
            self.conn.execute(
                "INSERT INTO version_decisions(version_id, event_id, sequence, occurred_at,"
                " power_kw, status, reason, effective_power_kw, arrival_delay_s,"
                " soc_basis_event_id, constraint_hits_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (version_id, d["event_id"], d["sequence"], d["occurred_at"], d["power_kw"],
                 d["status"], d["reason"], d["effective_power_kw"],
                 d["arrival_delay_s"],
                 d["data_basis"]["soc_event_id"],
                 json.dumps(d["constraint_hits"], ensure_ascii=False)),
            )
        try:
            self.conn.commit()
        except sqlite3.IntegrityError:
            # 并发的相同回放已落库：内容寻址保证结论一致，直接取回
            self.conn.rollback()
            row = self.conn.execute(
                "SELECT version_id, result_json FROM replay_versions WHERE inputs_fingerprint=?",
                (inputs_fingerprint,),
            ).fetchone()
            return {**json.loads(row["result_json"]), "version_id": row["version_id"], "deduped": True}

        return {**stored, "version_id": version_id, "deduped": False}

    def list_versions(self, shift_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT version_id, as_of, result_hash, facts_hash, created_at, post_seal,"
            " inputs_fingerprint FROM replay_versions WHERE shift_id=?"
            " ORDER BY created_at, rowid",
            (shift_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_version(self, version_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT result_json FROM replay_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise ReplayError("UNKNOWN_VERSION", f"未知回放版本: {version_id}")
        return {**json.loads(row["result_json"]), "version_id": version_id}

    def latest_version(self, shift_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT version_id FROM replay_versions WHERE shift_id=? ORDER BY rowid DESC LIMIT 1",
            (shift_id,),
        ).fetchone()
        if row is None:
            raise ReplayError("NO_VERSION", f"班次 {shift_id} 尚无回放版本")
        return self.get_version(row["version_id"])

    def signed_version(self, shift_id: str) -> dict[str, Any]:
        shift = self.get_shift(shift_id)
        if not shift["signed_version_id"]:
            raise ReplayError("NOT_SEALED", f"班次 {shift_id} 尚未封存签认")
        return self.get_version(shift["signed_version_id"])

    # ---------------------------------------------------------------- 对比

    def diff_versions(self, then_version_id: str, now_version_id: str) -> dict[str, Any]:
        then_view = self.get_version(then_version_id)
        now_view = self.get_version(now_version_id)
        diff = diff_replays(then_view, now_view)
        diff["then_version_id"] = then_version_id
        diff["now_version_id"] = now_version_id
        return diff

    def signed_vs_latest(self, shift_id: str) -> dict[str, Any]:
        """一键回答：补传旧数据是否改写了交班签认结论。"""
        shift = self.get_shift(shift_id)
        if not shift["signed_version_id"]:
            raise ReplayError("NOT_SEALED", "班次未封存，无签认版本可比")
        latest = self.latest_version(shift_id)
        diff = self.diff_versions(shift["signed_version_id"], latest["version_id"])
        diff["signed_version_id"] = shift["signed_version_id"]
        diff["latest_version_id"] = latest["version_id"]
        diff["signed_conclusion_intact"] = not diff["signed_conclusion_changed"]
        return diff

    # ---------------------------------------------------------------- 追溯

    def trace_decision(self, shift_id: str, event_id: str) -> dict[str, Any]:
        """沿一条功率指令穿过所有版本：结论、依据遥测、命中的约束。

        再往下钻一层：版本采用的每条事实带内容哈希，可与 facts 台账
        逐字节比对——证明当时结论建立在哪一版数据之上。
        """
        rows = self.conn.execute(
            "SELECT vd.version_id, vd.sequence, vd.occurred_at, vd.power_kw, vd.status,"
            " vd.reason, vd.effective_power_kw, vd.arrival_delay_s,"
            " vd.soc_basis_event_id, vd.constraint_hits_json, rv.as_of, rv.created_at,"
            " rv.post_seal FROM version_decisions vd"
            " JOIN replay_versions rv ON rv.version_id=vd.version_id"
            " WHERE vd.event_id=? AND rv.shift_id=?"
            " ORDER BY rv.rowid",
            (event_id, shift_id),
        ).fetchall()
        if not rows:
            raise ReplayError("UNKNOWN_DECISION", f"班次 {shift_id} 中找不到指令 {event_id}")
        versions = []
        for r in rows:
            versions.append({
                "version_id": r["version_id"],
                "as_of": r["as_of"],
                "created_at": r["created_at"],
                "post_seal": bool(r["post_seal"]),
                "status": r["status"],
                "reason": r["reason"],
                "power_kw": r["power_kw"],
                "effective_power_kw": r["effective_power_kw"],
                "arrival_delay_s": r["arrival_delay_s"],
                "soc_basis_event_id": r["soc_basis_event_id"],
                "constraint_hits": json.loads(r["constraint_hits_json"]),
                "data_version": self._version_fact_hashes(r["version_id"]),
            })
        return {"shift_id": shift_id, "event_id": event_id, "across_versions": versions}

    def _version_fact_hashes(self, version_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event_id, content_hash, visible FROM version_facts"
            " WHERE version_id=? ORDER BY event_id",
            (version_id,),
        ).fetchall()
        return [{"event_id": r["event_id"], "content_hash": r["content_hash"],
                 "visible": bool(r["visible"])} for r in rows]

    def verify_version(self, version_id: str) -> dict[str, Any]:
        """用版本钉住的事实集重算引擎结果，与封存结果逐字节对账。

        版本事实清单（version_facts）本身只追加，重算时只取该清单内的
        事实——事后补录的迟到数据再多，也不进入这次对账。
        """
        row = self.conn.execute(
            "SELECT shift_id, as_of, profile_hash, result_json, result_hash"
            " FROM replay_versions WHERE version_id=?",
            (version_id,),
        ).fetchone()
        if row is None:
            raise ReplayError("UNKNOWN_VERSION", f"未知回放版本: {version_id}")
        shift = self.get_shift(row["shift_id"])
        cap = self._load_capability(shift["profile_id"])
        pinned_rows = self.conn.execute(
            "SELECT event_id, content_hash FROM version_facts WHERE version_id=?",
            (version_id,),
        ).fetchall()
        pinned = {r["event_id"]: r["content_hash"] for r in pinned_rows}
        all_facts = self._load_facts(row["shift_id"])
        pinned_facts: list[Fact] = []
        tampered: list[str] = []
        for fact, h in all_facts:
            if fact.event_id in pinned:
                if pinned[fact.event_id] != h:
                    tampered.append(fact.event_id)
                pinned_facts.append(fact)
        as_of_dt = parse_dt(row["as_of"]) if row["as_of"] else None
        view = reconstruct(pinned_facts, cap, as_of=as_of_dt)
        recomputed = sha12(view)
        stored = json.loads(row["result_json"])
        later_added = [
            f.event_id for f, _ in all_facts if f.event_id not in pinned
        ]
        return {
            "version_id": version_id,
            "stored_result_hash": row["result_hash"],
            "recomputed_result_hash": recomputed,
            "matches": recomputed == row["result_hash"],
            "pinned_fact_count": len(pinned),
            "facts_added_after_version": later_added,
            "tampered_facts": tampered,
            "stored_note": stored.get("note"),
        }
