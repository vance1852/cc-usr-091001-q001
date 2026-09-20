"""SQLite 持久层：只追加（append-only）的事件与版本台账。

不可变性不靠“自觉”——由触发器在库内强制执行：

* 事实表、版本表、能力档案一律禁止 UPDATE/DELETE；
* 班次一旦封存（sealed=1），触发器直接拒绝相关的一切新写入，
  迟到数据连库都进不来，更谈不上改写结论；
* 唯一允许的 UPDATE 是事实重到时刷新 last_seen_at / resend_count。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capability_profiles(
    profile_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shifts(
    shift_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES capability_profiles(profile_id),
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    handover_at TEXT NOT NULL,
    sealed INTEGER NOT NULL DEFAULT 0,
    sealed_at TEXT,
    signed_version_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_batches(
    batch_id TEXT PRIMARY KEY,
    shift_id TEXT NOT NULL REFERENCES shifts(shift_id),
    ingested_at TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    new_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    raw_fingerprint TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS facts(
    event_id TEXT NOT NULL,
    shift_id TEXT NOT NULL REFERENCES shifts(shift_id),
    kind TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    first_received_at TEXT,
    last_seen_at TEXT NOT NULL,
    resend_count INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    extra_json TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (shift_id, event_id)
);

CREATE TABLE IF NOT EXISTS replay_versions(
    version_id TEXT PRIMARY KEY,
    shift_id TEXT NOT NULL REFERENCES shifts(shift_id),
    as_of TEXT,
    profile_hash TEXT NOT NULL,
    facts_hash TEXT NOT NULL,
    inputs_fingerprint TEXT NOT NULL,
    result_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    post_seal INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_facts(
    version_id TEXT NOT NULL REFERENCES replay_versions(version_id),
    event_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    visible INTEGER NOT NULL,
    PRIMARY KEY (version_id, event_id)
);

CREATE TABLE IF NOT EXISTS version_decisions(
    version_id TEXT NOT NULL REFERENCES replay_versions(version_id),
    event_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    power_kw REAL NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    effective_power_kw REAL,
    arrival_delay_s REAL,
    soc_basis_event_id TEXT,
    constraint_hits_json TEXT NOT NULL,
    PRIMARY KEY (version_id, event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_versions_inputs
    ON replay_versions(shift_id, inputs_fingerprint);
CREATE INDEX IF NOT EXISTS ix_decisions_event ON version_decisions(event_id);

-- 能力档案：只追加
CREATE TRIGGER IF NOT EXISTS trg_profile_no_upd BEFORE UPDATE ON capability_profiles
BEGIN SELECT RAISE(ABORT, 'capability profiles are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_profile_no_del BEFORE DELETE ON capability_profiles
BEGIN SELECT RAISE(ABORT, 'capability profiles are append-only'); END;

-- 班次台账：只追加；封存后禁止任何改动
CREATE TRIGGER IF NOT EXISTS trg_shift_no_del BEFORE DELETE ON shifts
BEGIN SELECT RAISE(ABORT, 'shifts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_shift_sealed_no_upd BEFORE UPDATE ON shifts
WHEN OLD.sealed = 1
BEGIN SELECT RAISE(ABORT, 'SHIFT_SEALED: shift is frozen and cannot change'); END;

-- 事实：禁止删除；重到只允许刷新审计列。
-- 封存后仍可补录新事实（append-only 台账不断），但既有事实一个字节不能改，
-- 已签认版本也绝不重算——补录只能派生 post_seal 新版本。
CREATE TRIGGER IF NOT EXISTS trg_facts_no_del BEFORE DELETE ON facts
BEGIN SELECT RAISE(ABORT, 'facts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_facts_business_immutable BEFORE UPDATE ON facts
WHEN NEW.content_hash != OLD.content_hash
  OR NEW.event_id != OLD.event_id
  OR NEW.shift_id != OLD.shift_id
  OR NEW.kind != OLD.kind
  OR NEW.sequence != OLD.sequence
  OR NEW.occurred_at != OLD.occurred_at
  OR NEW.first_received_at IS NOT OLD.first_received_at
  OR NEW.payload_json != OLD.payload_json
  OR NEW.extra_json != OLD.extra_json
BEGIN SELECT RAISE(ABORT, 'fact business content is immutable; only audit columns may refresh'); END;

-- 回放版本：只追加。封存后派生的新版本由服务层打 post_seal=1 标记，
-- 触发器保证任何版本一经写入即不可改、不可删。
CREATE TRIGGER IF NOT EXISTS trg_version_no_upd BEFORE UPDATE ON replay_versions
BEGIN SELECT RAISE(ABORT, 'replay versions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_version_no_del BEFORE DELETE ON replay_versions
BEGIN SELECT RAISE(ABORT, 'replay versions are immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_vfacts_no_upd BEFORE UPDATE ON version_facts
BEGIN SELECT RAISE(ABORT, 'version facts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_vfacts_no_del BEFORE DELETE ON version_facts
BEGIN SELECT RAISE(ABORT, 'version facts are immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_vdec_no_upd BEFORE UPDATE ON version_decisions
BEGIN SELECT RAISE(ABORT, 'version decisions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_vdec_no_del BEFORE DELETE ON version_decisions
BEGIN SELECT RAISE(ABORT, 'version decisions are immutable'); END;
"""


class StorageError(RuntimeError):
    """持久层可预期错误（冲突、封存保护等）。"""


class ShiftSealedError(StorageError):
    pass


def open_db(path: str | Path) -> sqlite3.Connection:
    """打开（或初始化）台账库。可被多个进程并发打开。"""
    path = str(path)
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    return conn
