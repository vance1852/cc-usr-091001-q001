"""SQLite 持久化：事实只增不改，版本不可变，封存班次冻结。

WORM 由数据库触发器强制执行（应用层绕过也会被 ABORT）：
events / shifts / shift_seals / replay_versions / version_events /
decisions / constraint_hits 均禁止 UPDATE 与 DELETE。
封存后不允许以"不晚于封存时刻的知识截止线"为同一班次再派生
as_of 不超过班次结束的竞争版本——当班结论只能被新版本对照，不能被替换。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .models import RULE_VERSION, CapabilityParams, Decision, LatestReading
from .protocol import Event, canonical_json, parse_event
from .reconstruct import MaterializedView, materialize
from .timeutil import iso, now_utc, parse_ts

SCHEMA_VERSION = "1"

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    measure TEXT,
    power_kw REAL,
    soc_percent REAL,
    payload_json TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_redeliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    same_as_first INTEGER NOT NULL,
    noted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shifts (
    shift_id TEXT PRIMARY KEY,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    start_unix INTEGER NOT NULL,
    end_unix INTEGER NOT NULL,
    params_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shift_seals (
    shift_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    as_of TEXT NOT NULL,
    sealed_at TEXT NOT NULL,
    sealed_at_unix INTEGER NOT NULL,
    summary_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_versions (
    version_id TEXT PRIMARY KEY,
    shift_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    parent_version_id TEXT,
    as_of TEXT NOT NULL,
    knowledge_cutoff TEXT NOT NULL,
    as_of_unix INTEGER NOT NULL,
    knowledge_cutoff_unix INTEGER NOT NULL,
    rule_version TEXT NOT NULL,
    params_json TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    decisions_hash TEXT NOT NULL,
    summary_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (shift_id, as_of_unix, knowledge_cutoff_unix)
);

CREATE TABLE IF NOT EXISTS version_events (
    version_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    role TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    PRIMARY KEY (version_id, event_id)
);

CREATE TABLE IF NOT EXISTS decisions (
    version_id TEXT NOT NULL,
    dispatch_event_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    effective_at TEXT NOT NULL,
    command_kw REAL NOT NULL,
    reasons_json TEXT NOT NULL,
    sources_json TEXT NOT NULL,
    PRIMARY KEY (version_id, dispatch_event_id)
);

CREATE TABLE IF NOT EXISTS constraint_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT NOT NULL,
    dispatch_event_id TEXT NOT NULL,
    code TEXT NOT NULL,
    passed INTEGER NOT NULL,
    blocking INTEGER NOT NULL,
    message TEXT NOT NULL,
    numbers_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hits_version ON constraint_hits(version_id, dispatch_event_id);
CREATE INDEX IF NOT EXISTS idx_versions_shift ON replay_versions(shift_id);
"""

# 每张事实/版本表都禁止 UPDATE 与 DELETE
_WORM_TABLES = (
    "events",
    "event_redeliveries",
    "shifts",
    "shift_seals",
    "replay_versions",
    "version_events",
    "decisions",
    "constraint_hits",
)


def _trigger_sql() -> str:
    parts = []
    for table in _WORM_TABLES:
        parts.append(
            f"CREATE TRIGGER IF NOT EXISTS {table}_worm_upd BEFORE UPDATE ON {table}\n"
            "BEGIN SELECT RAISE(ABORT, '" + table + " is append-only (WORM)'); END;"
        )
        parts.append(
            f"CREATE TRIGGER IF NOT EXISTS {table}_worm_del BEFORE DELETE ON {table}\n"
            "BEGIN SELECT RAISE(ABORT, '" + table + " is append-only (WORM)'); END;"
        )
    # 封存后：知识截止线不晚于封存时刻、as_of 不晚于班次结束的版本不得再插入，
    # 防止有人用"当时知道的同样事实"重建一个竞争版本顶替签认结论。
    parts.append(
        """
CREATE TRIGGER IF NOT EXISTS no_rival_under_seal BEFORE INSERT ON replay_versions
WHEN EXISTS (
    SELECT 1 FROM shift_seals s
    JOIN shifts sh ON sh.shift_id = s.shift_id
    WHERE s.shift_id = NEW.shift_id
      AND NEW.as_of_unix <= sh.end_unix
      AND NEW.knowledge_cutoff_unix <= s.sealed_at_unix
)
BEGIN SELECT RAISE(ABORT, 'shift sealed: on-shift view is frozen, late data can only fork a version with a later cutoff'); END;
"""
    )
    return "\n".join(parts)


def genesis_hash(shift_id: str) -> str:
    return hashlib.sha256(f"genesis:{shift_id}".encode("utf-8")).hexdigest()


class StorageError(RuntimeError):
    pass


class NotFound(StorageError):
    pass


class Storage:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._lock = threading.RLock()

    # ---------- 连接与建库 ----------
    @classmethod
    def open(cls, path: str | Path) -> "Storage":
        path = str(path)
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_DDL)
        conn.executescript(_trigger_sql())
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.commit()
        store = cls(conn)
        # 重启即校验：链尾被篡改会在打开时暴露
        store.verify_all_chains()
        return store

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---------- 事实录入（按 event_id 幂等） ----------
    def add_events(self, raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        with self._lock:
            for raw in raw_events:
                event = parse_event(raw)
                now = iso(now_utc())
                row = self._conn.execute(
                    "SELECT raw_hash FROM events WHERE event_id=?", (event.event_id,)
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        "INSERT INTO events(event_id, kind, sequence, occurred_at, received_at,"
                        " measure, power_kw, soc_percent, payload_json, raw_hash, first_seen_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            event.event_id, event.kind, event.sequence, event.occurred_at,
                            event.received_at, event.measure, event.power_kw, event.soc_percent,
                            canonical_json(event.as_payload()), event.raw_hash, now,
                        ),
                    )
                    results.append({"event_id": event.event_id, "status": "new", "raw_hash": event.raw_hash})
                else:
                    same = row["raw_hash"] == event.raw_hash
                    self._conn.execute(
                        "INSERT INTO event_redeliveries(event_id, received_at, payload_json,"
                        " raw_hash, same_as_first, noted_at) VALUES(?,?,?,?,?,?)",
                        (
                            event.event_id, event.received_at, canonical_json(event.as_payload()),
                            event.raw_hash, 1 if same else 0, now,
                        ),
                    )
                    if same:
                        results.append(
                            {"event_id": event.event_id, "status": "duplicate", "raw_hash": event.raw_hash}
                        )
                    else:
                        results.append(
                            {
                                "event_id": event.event_id,
                                "status": "payload_conflict",
                                "message": "重送载荷与首条不一致，保留首条，冲突记入 event_redeliveries",
                                "stored_raw_hash": row["raw_hash"],
                                "received_raw_hash": event.raw_hash,
                            }
                        )
            self._conn.commit()
        return results

    def get_event(self, event_id: str) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFound(f"事件不存在: {event_id}")
        redeliveries = [
            dict(r)
            for r in self._conn.execute(
                "SELECT received_at, raw_hash, same_as_first, noted_at"
                " FROM event_redeliveries WHERE event_id=? ORDER BY id",
                (event_id,),
            ).fetchall()
        ]
        return {
            "event": json.loads(row["payload_json"]),
            "raw_hash": row["raw_hash"],
            "first_seen_at": row["first_seen_at"],
            "redeliveries": redeliveries,
        }

    # ---------- 班次 ----------
    def create_shift(
        self, shift_id: str, start_at: str, end_at: str, params: CapabilityParams | None = None
    ) -> dict[str, Any]:
        params = params or CapabilityParams()
        parse_ts(start_at)
        if parse_ts(end_at) <= parse_ts(start_at):
            raise StorageError("班次结束时间必须晚于开始时间")
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO shifts(shift_id, start_at, end_at, start_unix, end_unix,"
                    " params_json, created_at) VALUES(?,?,?,?,?,?,?)",
                    (shift_id, start_at, end_at, int(parse_ts(start_at).timestamp()),
                     int(parse_ts(end_at).timestamp()), params.canonical(), iso(now_utc())),
                )
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                raise StorageError(f"班次已存在: {shift_id}") from exc
        return {"shift_id": shift_id, "start_at": start_at, "end_at": end_at, "params": asdict(params)}

    def _load_shift(self, shift_id: str) -> sqlite3.Row:
        row = self._conn.execute("SELECT * FROM shifts WHERE shift_id=?", (shift_id,)).fetchone()
        if row is None:
            raise NotFound(f"班次不存在: {shift_id}")
        return row

    def get_shift(self, shift_id: str) -> dict[str, Any]:
        row = self._load_shift(shift_id)
        seal = self._conn.execute(
            "SELECT * FROM shift_seals WHERE shift_id=?", (shift_id,)
        ).fetchone()
        return {
            "shift_id": shift_id,
            "start_at": row["start_at"],
            "end_at": row["end_at"],
            "params": json.loads(row["params_json"]),
            "sealed": dict(seal) if seal else None,
        }

    # ---------- 版本派生 ----------
    def _load_shift_events(self, shift_row: sqlite3.Row) -> list[Event]:
        # 不过滤班次起点：爬坡前值等读数可能发生在上一班，是否采用由物化器
        # 按 occurred_at <= as_of 与 received_at <= cutoff 决定。
        rows = self._conn.execute(
            "SELECT payload_json FROM events ORDER BY occurred_at, sequence"
        ).fetchall()
        return [parse_event(json.loads(r["payload_json"])) for r in rows]

    @staticmethod
    def _decision_body(decision: Decision) -> list[Any]:
        hits = sorted(
            (
                [h.code, 1 if h.passed else 0, 1 if h.blocking else 0, h.message, h.numbers]
                for h in decision.hits
            ),
            key=lambda x: x[0],
        )

        def reading(kind: str) -> list[Any] | None:
            r: LatestReading | None = decision.sources.get(kind)
            if r is None:
                return None
            return [r.event_id, r.sequence, r.occurred_at, r.received_at, r.value, r.raw_hash, r.age_s]

        return [
            decision.dispatch_event_id,
            decision.outcome,
            decision.effective_at,
            decision.command_kw,
            decision.reasons,
            {"power": reading("power"), "soc": reading("soc")},
            hits,
        ]

    def _build_summary(
        self,
        *,
        shift_id: str,
        parent_hash: str,
        view: MaterializedView,
        decisions: list[Decision],
        params: CapabilityParams,
    ) -> tuple[str, str]:
        hash_rows = {
            r["event_id"]: r["raw_hash"]
            for r in self._conn.execute(
                f"SELECT event_id, raw_hash FROM events WHERE event_id IN ({','.join('?' for _ in view.adopted)})",
                tuple(view.adopted),
            ).fetchall()
        } if view.adopted else {}
        adopted = sorted(
            [eid, role, hash_rows[eid]] for eid, role in view.adopted.items()
        )
        body = {
            "shift_id": shift_id,
            "as_of": view.as_of,
            "knowledge_cutoff": view.knowledge_cutoff,
            "rule_version": RULE_VERSION,
            "params": asdict(params),
            "adopted": adopted,
            "decisions": sorted(
                (self._decision_body(d) for d in decisions), key=lambda x: x[0]
            ),
        }
        decisions_hash = hashlib.sha256(
            canonical_json({"decisions": body["decisions"]}).encode("utf-8")
        ).hexdigest()
        summary = hashlib.sha256(
            (parent_hash + canonical_json(body)).encode("utf-8")
        ).hexdigest()
        return decisions_hash, summary

    def create_version(
        self, shift_id: str, as_of: str, knowledge_cutoff: str
    ) -> dict[str, Any]:
        """以 (shift, as_of, knowledge_cutoff) 为幂等键派生不可变回放版本。"""
        parse_ts(as_of)
        parse_ts(knowledge_cutoff)
        as_of_unix = int(parse_ts(as_of).timestamp())
        cutoff_unix = int(parse_ts(knowledge_cutoff).timestamp())
        with self._lock:
            existing = self._conn.execute(
                "SELECT version_id FROM replay_versions"
                " WHERE shift_id=? AND as_of_unix=? AND knowledge_cutoff_unix=?",
                (shift_id, as_of_unix, cutoff_unix),
            ).fetchone()
            if existing:
                return self.get_version(existing["version_id"])

            shift_row = self._load_shift(shift_id)
            params = CapabilityParams.from_dict(json.loads(shift_row["params_json"]))
            ordinal = (
                self._conn.execute(
                    "SELECT COALESCE(MAX(ordinal), 0) + 1 AS n FROM replay_versions WHERE shift_id=?",
                    (shift_id,),
                ).fetchone()["n"]
            )
            version_id = f"v-{shift_id}-{ordinal:03d}"
            parent = self._conn.execute(
                "SELECT version_id, summary_hash FROM replay_versions"
                " WHERE shift_id=? ORDER BY ordinal DESC LIMIT 1",
                (shift_id,),
            ).fetchone()
            parent_id = parent["version_id"] if parent else None
            parent_hash = parent["summary_hash"] if parent else genesis_hash(shift_id)

            events = self._load_shift_events(shift_row)
            view = materialize(version_id, as_of, knowledge_cutoff, params, events)
            decisions = view.decisions()
            decisions_hash, summary_hash = self._build_summary(
                shift_id=shift_id,
                parent_hash=parent_hash,
                view=view,
                decisions=decisions,
                params=params,
            )

            try:
                self._conn.execute(
                    "INSERT INTO replay_versions(version_id, shift_id, ordinal, parent_version_id,"
                    " as_of, knowledge_cutoff, as_of_unix, knowledge_cutoff_unix,"
                    " rule_version, params_json, prev_hash,"
                    " decisions_hash, summary_hash, created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        version_id, shift_id, ordinal, parent_id, as_of, knowledge_cutoff,
                        as_of_unix, cutoff_unix,
                        RULE_VERSION, params.canonical(), parent_hash, decisions_hash,
                        summary_hash, iso(now_utc()),
                    ),
                )
                self._conn.executemany(
                    "INSERT INTO version_events(version_id, event_id, role, raw_hash)"
                    " VALUES(?,?,?,?)",
                    [
                        (version_id, eid, role, self._event_hash(eid))
                        for eid, role in sorted(view.adopted.items())
                    ],
                )
                for d in decisions:
                    self._conn.execute(
                        "INSERT INTO decisions(version_id, dispatch_event_id, outcome,"
                        " effective_at, command_kw, reasons_json, sources_json)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (
                            version_id, d.dispatch_event_id, d.outcome, d.effective_at,
                            d.command_kw, json.dumps(d.reasons, ensure_ascii=False),
                            canonical_json(
                                {
                                    kind: (asdict(r) if r is not None else None)
                                    for kind, r in d.sources.items()
                                }
                            ),
                        ),
                    )
                    self._conn.executemany(
                        "INSERT INTO constraint_hits(version_id, dispatch_event_id, code,"
                        " passed, blocking, message, numbers_json) VALUES(?,?,?,?,?,?,?)",
                        [
                            (
                                version_id, d.dispatch_event_id, h.code,
                                1 if h.passed else 0, 1 if h.blocking else 0,
                                h.message, canonical_json(h.numbers),
                            )
                            for h in d.hits
                        ],
                    )
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                raise StorageError(f"版本写入失败（班次可能已封存）: {exc}") from exc
        return self.get_version(version_id)

    def _event_hash(self, event_id: str) -> str:
        return self._conn.execute(
            "SELECT raw_hash FROM events WHERE event_id=?", (event_id,)
        ).fetchone()["raw_hash"]

    # ---------- 封存 ----------
    def seal_shift(self, shift_id: str, sealed_at: str | None = None) -> dict[str, Any]:
        """以班次结束为 as_of、封存时刻为知识截止线生成当班版本并冻结。"""
        sealed_at = sealed_at or iso(now_utc())
        parse_ts(sealed_at)
        with self._lock:
            shift_row = self._load_shift(shift_id)
            existing = self._conn.execute(
                "SELECT * FROM shift_seals WHERE shift_id=?", (shift_id,)
            ).fetchone()
            if existing:
                return dict(existing)
            if parse_ts(sealed_at) < parse_ts(shift_row["end_at"]):
                raise StorageError("不能在班次结束前封存")
            version = self.create_version(shift_id, shift_row["end_at"], sealed_at)
            self._conn.execute(
                "INSERT INTO shift_seals(shift_id, version_id, as_of, sealed_at, sealed_at_unix,"
                " summary_hash) VALUES(?,?,?,?,?,?)",
                (
                    shift_id, version["version_id"], shift_row["end_at"],
                    sealed_at, int(parse_ts(sealed_at).timestamp()), version["summary_hash"],
                ),
            )
            self._conn.commit()
        return self.get_shift(shift_id)["sealed"]

    # ---------- 读取 ----------
    def list_versions(self, shift_id: str) -> list[dict[str, Any]]:
        self._load_shift(shift_id)
        rows = self._conn.execute(
            "SELECT v.*, EXISTS(SELECT 1 FROM shift_seals s WHERE s.version_id=v.version_id) AS is_sealed"
            " FROM replay_versions v WHERE shift_id=? ORDER BY ordinal",
            (shift_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def _version_row(self, version_id: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM replay_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"版本不存在: {version_id}")
        return row

    def get_version(self, version_id: str) -> dict[str, Any]:
        row = self._version_row(version_id)
        sealed = self._conn.execute(
            "SELECT 1 FROM shift_seals WHERE version_id=?", (version_id,)
        ).fetchone()
        return {
            "version_id": version_id,
            "shift_id": row["shift_id"],
            "ordinal": row["ordinal"],
            "parent_version_id": row["parent_version_id"],
            "as_of": row["as_of"],
            "knowledge_cutoff": row["knowledge_cutoff"],
            "rule_version": row["rule_version"],
            "params": json.loads(row["params_json"]),
            "prev_hash": row["prev_hash"],
            "decisions_hash": row["decisions_hash"],
            "summary_hash": row["summary_hash"],
            "created_at": row["created_at"],
            "sealed": bool(sealed),
        }

    def get_timeline(self, version_id: str) -> dict[str, Any]:
        version = self.get_version(version_id)
        decisions: list[dict[str, Any]] = []
        for d in self._conn.execute(
            "SELECT * FROM decisions WHERE version_id=? ORDER BY effective_at, dispatch_event_id",
            (version_id,),
        ).fetchall():
            hits = [
                {
                    "code": h["code"],
                    "passed": bool(h["passed"]),
                    "blocking": bool(h["blocking"]),
                    "message": h["message"],
                    "numbers": json.loads(h["numbers_json"]),
                }
                for h in self._conn.execute(
                    "SELECT * FROM constraint_hits WHERE version_id=? AND dispatch_event_id=?"
                    " ORDER BY id",
                    (version_id, d["dispatch_event_id"]),
                ).fetchall()
            ]
            decisions.append(
                {
                    "dispatch_event_id": d["dispatch_event_id"],
                    "outcome": d["outcome"],
                    "effective_at": d["effective_at"],
                    "command_kw": d["command_kw"],
                    "reasons": json.loads(d["reasons_json"]),
                    "hits": hits,
                }
            )
        adopted = {
            r["event_id"]: {"role": r["role"], "raw_hash": r["raw_hash"]}
            for r in self._conn.execute(
                "SELECT * FROM version_events WHERE version_id=?", (version_id,)
            ).fetchall()
        }
        return {"version": version, "adopted_events": adopted, "decisions": decisions}

    def trace_decision(self, version_id: str, dispatch_event_id: str) -> dict[str, Any]:
        """沿一条功率决定追到：所用数据版本、原始读数与逐条约束命中项。"""
        timeline = self.get_timeline(version_id)
        decision = next(
            (d for d in timeline["decisions"] if d["dispatch_event_id"] == dispatch_event_id), None
        )
        if decision is None:
            raise NotFound(f"版本 {version_id} 中无指令 {dispatch_event_id}")
        drow = self._conn.execute(
            "SELECT sources_json FROM decisions WHERE version_id=? AND dispatch_event_id=?",
            (version_id, dispatch_event_id),
        ).fetchone()
        sources = json.loads(drow["sources_json"])
        enriched = {}
        for kind, reading in sources.items():
            if reading is None:
                enriched[kind] = None
                continue
            event = self.get_event(reading["event_id"])
            enriched[kind] = {**reading, "payload": event["event"], "raw_hash": reading["raw_hash"]}
        return {
            "version": timeline["version"],
            "dispatch_event_id": dispatch_event_id,
            "decision": decision,
            "used_readings": enriched,
        }

    # ---------- 版本差异 ----------
    def diff_versions(self, from_version: str, to_version: str) -> dict[str, Any]:
        a = self.get_timeline(from_version)
        b = self.get_timeline(to_version)
        a_ids = set(a["adopted_events"])
        b_ids = set(b["adopted_events"])
        a_dec = {d["dispatch_event_id"]: d for d in a["decisions"]}
        b_dec = {d["dispatch_event_id"]: d for d in b["decisions"]}
        changed = []
        for cid in sorted(set(a_dec) | set(b_dec)):
            da, db = a_dec.get(cid), b_dec.get(cid)
            if da is None or db is None:
                changed.append({"dispatch_event_id": cid, "change": "appeared" if da is None else "vanished",
                                "from": da and da["outcome"], "to": db and db["outcome"]})
                continue
            ra = {h["code"]: h for h in da["hits"]}
            rb = {h["code"]: h for h in db["hits"]}
            hit_changes = []
            for code in sorted(set(ra) | set(rb)):
                if ra.get(code, {}).get("passed") != rb.get(code, {}).get("passed"):
                    hit_changes.append(
                        {
                            "code": code,
                            "from_passed": ra.get(code, {}).get("passed"),
                            "to_passed": rb.get(code, {}).get("passed"),
                            "from_message": ra.get(code, {}).get("message"),
                            "to_message": rb.get(code, {}).get("message"),
                        }
                    )
            if da["outcome"] != db["outcome"] or da["reasons"] != db["reasons"] or hit_changes:
                changed.append(
                    {
                        "dispatch_event_id": cid,
                        "change": "changed",
                        "from_outcome": da["outcome"],
                        "to_outcome": db["outcome"],
                        "from_reasons": da["reasons"],
                        "to_reasons": db["reasons"],
                        "constraint_flips": hit_changes,
                    }
                )
        return {
            "from_version": self.get_version(from_version),
            "to_version": self.get_version(to_version),
            "events_added": sorted(b_ids - a_ids),
            "events_removed": sorted(a_ids - b_ids),
            "decisions_changed": changed,
            "conclusions_hash_changed": a["version"]["decisions_hash"] != b["version"]["decisions_hash"],
        }

    # ---------- 封存未改写证明 ----------
    def proof_untampered(self, sealed_version: str, to_version: str) -> dict[str, Any]:
        sealed = self.get_version(sealed_version)
        if not sealed["sealed"]:
            raise StorageError(f"{sealed_version} 不是封存版本")
        diff = self.diff_versions(sealed_version, to_version)
        seal_row = self._conn.execute(
            "SELECT * FROM shift_seals WHERE version_id=?", (sealed_version,)
        ).fetchone()
        return {
            "seal": dict(seal_row),
            "sealed_version_summary_hash": sealed["summary_hash"],
            "sealed_version_decisions_hash": sealed["decisions_hash"],
            "chain": self.verify_chain(sealed["shift_id"]),
            "comparison": {
                "to_version": to_version,
                "events_added_in_later": diff["events_added"],
                "events_removed_in_later": diff["events_removed"],
                "decisions_changed": diff["decisions_changed"],
                "conclusions_hash_changed": diff["conclusions_hash_changed"],
            },
            "worm_protection": {
                "tables": list(_WORM_TABLES),
                "note": "上述表上有 BEFORE UPDATE/DELETE 触发器，任何篡改尝试被 SQLite ABORT",
            },
            "conclusion": (
                "封存版本完整且与封存时一致；后到数据只产生新版本，未改写签认结论"
                if self.verify_chain(sealed["shift_id"])["ok"]
                and sealed["summary_hash"] == seal_row["summary_hash"]
                else "校验失败：封存结论可能被篡改"
            ),
        }

    # ---------- 哈希链校验 ----------
    def verify_chain(self, shift_id: str) -> dict[str, Any]:
        """从原始事实重算班次下每个版本，核对采用集合、决定与哈希链。"""
        shift_row = self._load_shift(shift_id)
        params = CapabilityParams.from_dict(json.loads(shift_row["params_json"]))
        events = self._load_shift_events(shift_row)
        rows = self._conn.execute(
            "SELECT * FROM replay_versions WHERE shift_id=? ORDER BY ordinal", (shift_id,)
        ).fetchall()
        prev_hash = genesis_hash(shift_id)
        checked = []
        ok = True
        problems: list[str] = []
        for row in rows:
            vid = row["version_id"]
            view = materialize(
                vid, row["as_of"], row["knowledge_cutoff"], params, events
            )
            decisions = view.decisions()
            decisions_hash, summary_hash = self._build_summary(
                shift_id=shift_id,
                parent_hash=prev_hash,
                view=view,
                decisions=decisions,
                params=params,
            )
            stored_adopted = {
                r["event_id"]: r["role"]
                for r in self._conn.execute(
                    "SELECT event_id, role FROM version_events WHERE version_id=?", (vid,)
                ).fetchall()
            }
            problems_v = []
            if row["prev_hash"] != prev_hash:
                problems_v.append("prev_hash 断链")
            if row["summary_hash"] != summary_hash:
                problems_v.append("summary_hash 重算不一致")
            if row["decisions_hash"] != decisions_hash:
                problems_v.append("decisions_hash 重算不一致")
            if stored_adopted != view.adopted:
                problems_v.append("采用事件集合与重算不符")
            if row["rule_version"] != RULE_VERSION:
                problems_v.append("rule_version 不一致")
            ok_v = not problems_v
            ok = ok and ok_v
            problems.extend(f"{vid}: {p}" for p in problems_v)
            checked.append(
                {
                    "version_id": vid,
                    "summary_hash": row["summary_hash"],
                    "recomputed_hash": summary_hash,
                    "ok": ok_v,
                }
            )
            prev_hash = row["summary_hash"]
        seal = self._conn.execute(
            "SELECT * FROM shift_seals WHERE shift_id=?", (shift_id,)
        ).fetchone()
        if seal is not None:
            if seal["summary_hash"] != next(
                (c["summary_hash"] for c in checked if c["version_id"] == seal["version_id"]), None
            ):
                ok = False
                problems.append(f"封存哈希与版本 {seal['version_id']} 不一致")
        return {"shift_id": shift_id, "ok": ok, "versions": checked, "problems": problems}

    def verify_all_chains(self) -> dict[str, Any]:
        rows = self._conn.execute("SELECT shift_id FROM shifts ORDER BY shift_id").fetchall()
        reports = [self.verify_chain(r["shift_id"]) for r in rows]
        return {"ok": all(r["ok"] for r in reports), "shifts": reports}

    def worm_attempt(self, table: str) -> str:
        """自检工具：尝试篡改一张 WORM 表，返回数据库给出的拒绝原因。"""
        if table not in _WORM_TABLES:
            raise ValueError(f"未知表: {table}")
        row = self._conn.execute(f"SELECT rowid FROM {table} LIMIT 1").fetchone()
        if row is None:
            return "表为空，无可尝试的行"
        # 选一个真实列做 no-op 更新；BEFORE UPDATE 触发器无论值是否变化都会触发
        col = {
            "events": "event_id",
            "event_redeliveries": "event_id",
            "shifts": "shift_id",
            "shift_seals": "shift_id",
            "replay_versions": "version_id",
            "version_events": "version_id",
            "decisions": "version_id",
            "constraint_hits": "version_id",
        }[table]
        try:
            self._conn.execute(
                f"UPDATE {table} SET {col}={col} WHERE rowid=?", (row["rowid"],)
            )
            return "未被拦截（异常！）"
        except sqlite3.IntegrityError as exc:
            return str(exc)
        finally:
            self._conn.rollback()
