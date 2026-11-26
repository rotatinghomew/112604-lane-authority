"""进区服务的持久化状态（SQLite）。

设计要点：

* 单车许可互斥由 ``permits`` 上的局部唯一索引保证：同一 ``vehicle`` 只允许一条
  ``pending / armed / cancel_requested`` 许可；跨线程、跨进程都由 SQLite 裁决。
* 设备不确定：许可处于 pending 时，唯一允许的出路是向同一车道设备查证；
  取消只有在设备证明不再可用后才落地，避免“取消超时也照样抬杆”。
* 越线取证：设备可先传过线再传开闸确认，所以回执先落 ``events`` 再处理；许可
  还没到时先搁置，由后台补处理。重复送达、同许可或同车道批次的重复越线不会
  重复交接。
* 每次状态变化都追加 audit，值班可以查“当时是否有权”与“实际发生了什么”。
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterable, Iterator

from .feeds import format_time, parse_time


SCHEMA = """
CREATE TABLE IF NOT EXISTS manifests (
    manifest TEXT NOT NULL,
    revision INTEGER NOT NULL,
    vin TEXT NOT NULL,
    port TEXT NOT NULL,
    from_at TEXT NOT NULL,
    until_at TEXT NOT NULL,
    action TEXT NOT NULL,
    published_at TEXT NOT NULL,
    PRIMARY KEY (manifest, revision, vin)
);
CREATE TABLE IF NOT EXISTS observations (
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    seen_at TEXT NOT NULL,
    candidates TEXT NOT NULL,
    camera TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (lane, pass, sequence)
);
CREATE TABLE IF NOT EXISTS paper_checks (
    check_id TEXT PRIMARY KEY,
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    vin TEXT NOT NULL,
    manifest TEXT NOT NULL,
    revision INTEGER NOT NULL,
    officer TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stations (
    device TEXT PRIMARY KEY,
    lane TEXT NOT NULL,
    port TEXT NOT NULL,
    kind TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS permits (
    permit_id TEXT PRIMARY KEY,
    vehicle TEXT NOT NULL,
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    vin TEXT NOT NULL,
    port TEXT NOT NULL,
    manifest TEXT NOT NULL,
    revision INTEGER NOT NULL,
    armed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL,
    cancel_requested_at TEXT,
    resolution TEXT NOT NULL,
    uncertainty TEXT NOT NULL,
    audit TEXT NOT NULL,
    crossing_id TEXT,
    crossed_at TEXT,
    crossed_pass TEXT,
    crossed_lane TEXT,
    superseded_by TEXT,
    reissued_as TEXT,
    inbound TEXT NOT NULL,
    inbound_at TEXT,
    inbound_reference TEXT,
    inbound_note TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS permits_vehicle ON permits (vehicle);
CREATE INDEX IF NOT EXISTS permits_lane_pass ON permits (lane, pass);
-- 一辆车同一时刻最多一条未交公的许可；跨进程也成立。
CREATE UNIQUE INDEX IF NOT EXISTS permits_open_vehicle
    ON permits (vehicle) WHERE status IN ('pending', 'armed', 'cancel_requested');
CREATE TABLE IF NOT EXISTS crossings (
    crossing_id TEXT PRIMARY KEY,
    permit_id TEXT NOT NULL,
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    vin TEXT NOT NULL,
    crossed_at TEXT,
    verified INTEGER NOT NULL,
    handoff TEXT NOT NULL,
    handoff_at TEXT,
    note TEXT,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    permit_id TEXT NOT NULL,
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    kind TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    crossing_id TEXT,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS events_unapplied ON events (applied);
CREATE TABLE IF NOT EXISTS holds (
    manifest TEXT NOT NULL,
    revision INTEGER NOT NULL,
    action TEXT NOT NULL,
    published_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    rows TEXT NOT NULL,
    PRIMARY KEY (manifest, revision, action)
);
CREATE TABLE IF NOT EXISTS reviews (
    check_id TEXT PRIMARY KEY,
    lane TEXT NOT NULL,
    pass TEXT NOT NULL,
    vin TEXT NOT NULL,
    manifest TEXT NOT NULL,
    revision INTEGER NOT NULL,
    officer TEXT NOT NULL,
    reviewed_at TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    entity TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    source TEXT NOT NULL,
    details TEXT NOT NULL
);
"""

# 未交公的许可主干；设备状态一律以设备回话为准。
OPEN_STATUSES = ("pending", "armed", "cancel_requested")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def load(text: str | None, default: Any = None) -> Any:
    if text is None:
        return default
    return json.loads(text)


class Store:
    """线程安全的 SQLite 状态库。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False, timeout=10.0
        )
        self._connection.row_factory = sqlite3.Row
        self._write_lock = threading.RLock()
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.executescript(SCHEMA)

    def close(self) -> None:
        self._connection.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        # 单连接被多个 HTTP 线程共用：写事务必须串行，读可以并发。
        with self._write_lock:
            connection = self._connection
            if connection.in_transaction:
                # 已在一次写事务里（例如交叉处理调用），直接复用。
                yield connection
                return
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        yield self._connection

    def audit(
        self,
        connection: sqlite3.Connection,
        at: datetime,
        entity: str,
        entity_id: str,
        action: str,
        actor: str,
        source: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO audit (at, entity, entity_id, action, actor, source, details)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (format_time(at), entity, entity_id, action, actor, source, dump(details or {})),
        )

    # ---- 名单 / 观测 / 纸单 / 设备 ----

    def record_manifest_row(self, row: dict[str, Any], at: datetime) -> bool:
        with self.write() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO manifests"
                " (manifest, revision, vin, port, from_at, until_at, action, published_at)"
                " VALUES (:manifest, :revision, :vin, :port, :from_at, :until_at, :action, :published_at)",
                {
                    "manifest": row["manifest"],
                    "revision": row["revision"],
                    "vin": row["vin"],
                    "port": row["port"],
                    "from_at": row["from"],
                    "until_at": row["until"],
                    "action": row["action"],
                    "published_at": row["published_at"],
                },
            )
            if cursor.rowcount:
                self.audit(
                    connection,
                    at,
                    "manifest",
                    f"{row['manifest']}:{row['revision']}",
                    "published",
                    "feed",
                    "manifests",
                    {"vin": row["vin"], "action": row["action"]},
                )
            return bool(cursor.rowcount)

    def latest_manifest_revision_for_vin(
        self, manifest: str, vin: str, at: datetime | None = None
    ) -> int | None:
        revisions = [
            row["revision"]
            for row in self._connection.execute(
                "SELECT revision, published_at FROM manifests WHERE manifest = ? AND vin = ?",
                (manifest, vin),
            )
            if at is None or parse_time(row["published_at"]) <= at
        ]
        return max(revisions) if revisions else None

    def manifest_rows(self) -> Iterable[sqlite3.Row]:
        return self._connection.execute(
            "SELECT * FROM manifests ORDER BY manifest, revision, vin"
        ).fetchall()

    def record_observation(self, row: dict[str, Any], at: datetime) -> bool:
        with self.write() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO observations"
                " (lane, pass, sequence, seen_at, candidates, camera, recorded_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    row["lane"],
                    row["pass"],
                    row["sequence"],
                    row["seen_at"],
                    dump(row["candidates"]),
                    row["camera"],
                    format_time(at),
                ),
            )
            if cursor.rowcount:
                self.audit(
                    connection,
                    at,
                    "observation",
                    f"{row['lane']}:{row['pass']}:{row['sequence']}",
                    "recorded",
                    "camera",
                    "observations",
                    {"candidates": row["candidates"]},
                )
            return bool(cursor.rowcount)

    def observations_for_pass(self, lane: str, pass_id: str) -> list[dict[str, Any]]:
        result = []
        for row in self._connection.execute(
            "SELECT * FROM observations WHERE lane = ? AND pass = ? ORDER BY sequence",
            (lane, pass_id),
        ):
            record = dict(row)
            record["candidates"] = load(record["candidates"], [])
            record["_seen_at"] = parse_time(record["seen_at"])
            result.append(record)
        return result

    def record_paper_check(self, row: dict[str, Any], at: datetime) -> bool:
        with self.write() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO paper_checks"
                " (check_id, lane, pass, vin, manifest, revision, officer, recorded_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["check"],
                    row["lane"],
                    row["pass"],
                    row["vin"],
                    row["manifest"],
                    row["revision"],
                    row["officer"],
                    format_time(at),
                ),
            )
            if cursor.rowcount:
                self.audit(
                    connection,
                    at,
                    "paper_check",
                    row["check"],
                    "recorded",
                    row["officer"],
                    "paper_checks",
                    {"lane": row["lane"], "pass": row["pass"], "vin": row["vin"]},
                )
            return bool(cursor.rowcount)

    def paper_checks(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self._connection.execute(
                "SELECT * FROM paper_checks ORDER BY recorded_at, check_id"
            )
        ]

    def paper_check(self, check_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM paper_checks WHERE check_id = ?", (check_id,)
        ).fetchone()
        return dict(row) if row else None

    def paper_checks_for_pass(self, lane: str, pass_id: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self._connection.execute(
                "SELECT * FROM paper_checks WHERE lane = ? AND pass = ? ORDER BY recorded_at, check_id",
                (lane, pass_id),
            )
        ]

    def record_review(self, check: dict[str, Any], at: datetime, officer: str) -> None:
        with self.write() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO reviews"
                " (check_id, lane, pass, vin, manifest, revision, officer, reviewed_at, status)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'approved')",
                (
                    check["check_id"],
                    check["lane"],
                    check["pass"],
                    check["vin"],
                    check["manifest"],
                    check["revision"],
                    officer,
                    format_time(at),
                ),
            )

    def approved_review(
        self, lane: str, pass_id: str, at: datetime | None = None
    ) -> dict[str, Any] | None:
        """该观察最近一次仍成立的复核。

        复核依据的修订必须仍是清单最新修订；否则复核自动失效，不回退到旧单。
        """
        rows = self._connection.execute(
            "SELECT * FROM reviews WHERE lane = ? AND pass = ? AND status = 'approved'"
            " ORDER BY reviewed_at DESC, check_id DESC",
            (lane, pass_id),
        ).fetchall()
        for row in rows:
            latest = self.latest_manifest_revision_for_vin(
                row["manifest"], row["vin"], at
            )
            if latest == row["revision"]:
                return dict(row)
        return None

    def holds_for_vin(self, vin: str) -> list[dict[str, Any]]:
        return [
            {**dict(row), "rows": load(row["rows"], [])}
            for row in self._connection.execute(
                "SELECT * FROM holds ORDER BY published_at, manifest, revision"
            )
            if vin in (record["vin"] for record in load(row["rows"], []))
        ]

    def record_station(self, row: dict[str, Any], at: datetime) -> bool:
        with self.write() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO stations (device, lane, port, kind) VALUES (?, ?, ?, ?)",
                (row["device"], row["lane"], row["port"], row["kind"]),
            )
            if cursor.rowcount:
                self.audit(
                    connection,
                    at,
                    "station",
                    row["device"],
                    "recorded",
                    "feed",
                    "stations",
                    {"lane": row["lane"], "kind": row["kind"]},
                )
            return bool(cursor.rowcount)

    def station(self, device: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM stations WHERE device = ?", (device,)
        ).fetchone()
        return dict(row) if row else None

    def port_for_lane(self, lane: str) -> str | None:
        row = self._connection.execute(
            "SELECT port FROM stations WHERE lane = ? ORDER BY device LIMIT 1", (lane,)
        ).fetchone()
        return row["port"] if row else None

    # ---- 监管暂停 / 名单更正 ----

    def record_hold(
        self,
        manifest: str,
        revision: int,
        action: str,
        published_at: str,
        rows: list[dict[str, Any]],
        at: datetime,
    ) -> bool:
        with self.write() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO holds (manifest, revision, action, published_at, recorded_at, rows)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (manifest, revision, action, published_at, format_time(at), dump(rows)),
            )
            if cursor.rowcount:
                self.audit(
                    connection,
                    at,
                    "hold",
                    f"{manifest}:{revision}",
                    "recorded",
                    "regulation",
                    "hold_feed",
                    {"action": action, "rows": len(rows)},
                )
            return bool(cursor.rowcount)

    def holds(self) -> list[dict[str, Any]]:
        result = []
        for row in self._connection.execute("SELECT * FROM holds ORDER BY recorded_at, manifest, revision"):
            record = dict(row)
            record["rows"] = load(record["rows"], [])
            result.append(record)
        return result

    # ---- 许可 ----

    def insert_permit(self, permit: dict[str, Any]) -> None:
        self._connection.execute(
            "INSERT INTO permits (permit_id, vehicle, lane, pass, vin, port, manifest, revision,"
            " armed_at, expires_at, status, resolution, uncertainty, audit, inbound, updated_at)"
            " VALUES (:permit_id, :vehicle, :lane, :pass, :vin, :port, :manifest, :revision,"
            " :armed_at, :expires_at, :status, :resolution, :uncertainty, :audit, :inbound, :updated_at)",
            {**permit, "audit": dump(permit.get("audit", [])), "uncertainty": dump(permit.get("uncertainty", []))},
        )

    def permit(self, permit_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM permits WHERE permit_id = ?", (permit_id,)
        ).fetchone()
        return self._permit(row) if row else None

    def permit_holding_vehicle(self, vehicle: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM permits WHERE vehicle = ? AND status IN (?, ?, ?)"
            " ORDER BY armed_at DESC LIMIT 1",
            (vehicle, *OPEN_STATUSES),
        ).fetchone()
        return self._permit(row) if row else None

    def permits_for_lane_pass(self, lane: str, pass_id: str) -> list[dict[str, Any]]:
        return [
            self._permit(row)
            for row in self._connection.execute(
                "SELECT * FROM permits WHERE lane = ? AND pass = ? ORDER BY armed_at DESC",
                (lane, pass_id),
            )
        ]

    def open_permits(self, lane: str | None = None) -> list[dict[str, Any]]:
        if lane is None:
            rows = self._connection.execute(
                "SELECT * FROM permits WHERE status IN (?, ?, ?) ORDER BY armed_at",
                OPEN_STATUSES,
            )
        else:
            rows = self._connection.execute(
                "SELECT * FROM permits WHERE lane = ? AND status IN (?, ?, ?) ORDER BY armed_at",
                (lane, *OPEN_STATUSES),
            )
        return [self._permit(row) for row in rows]

    def permits_for_lane(self, lane: str) -> list[dict[str, Any]]:
        return [
            self._permit(row)
            for row in self._connection.execute(
                "SELECT * FROM permits WHERE lane = ? ORDER BY armed_at", (lane,)
            )
        ]

    def permits(self) -> list[dict[str, Any]]:
        return [
            self._permit(row)
            for row in self._connection.execute("SELECT * FROM permits ORDER BY armed_at")
        ]

    def crossing_for_lane_pass(self, lane: str, pass_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM crossings WHERE lane = ? AND pass = ? ORDER BY received_at LIMIT 1",
            (lane, pass_id),
        ).fetchone()
        return dict(row) if row else None

    def crossing_for_vin(self, vin: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM crossings WHERE vin = ? ORDER BY received_at LIMIT 1", (vin,)
        ).fetchone()
        return dict(row) if row else None

    def latest_revision_published_at(
        self, manifest: str, at: datetime, vin: str | None = None
    ) -> int | None:
        """在 at 之前已发出的清单最新修订号；可限定到某辆车。

        时间在 Python 里比较，避免字符串精度不同影响先后。
        """
        if vin is None:
            rows = self._connection.execute(
                "SELECT revision, published_at FROM manifests WHERE manifest = ?", (manifest,)
            )
        else:
            rows = self._connection.execute(
                "SELECT revision, published_at FROM manifests WHERE manifest = ? AND vin = ?",
                (manifest, vin),
            )
        revisions = [
            row["revision"] for row in rows if parse_time(row["published_at"]) <= at
        ]
        return max(revisions) if revisions else None

    def hold_effective_at(self, vin: str, at: datetime) -> dict[str, Any] | None:
        latest = None
        for hold in self.holds_for_vin(vin):
            if parse_time(hold["published_at"]) <= at:
                latest = hold
        return latest

    @staticmethod
    def _permit(row: sqlite3.Row) -> dict[str, Any]:
        permit = dict(row)
        for field in ("uncertainty", "audit"):
            permit[field] = load(permit[field], [])
        permit["armed_dt"] = parse_time(permit["armed_at"])
        permit["expires_dt"] = parse_time(permit["expires_at"])
        return permit

    def update_permit(self, connection: sqlite3.Connection, permit_id: str, **fields: Any) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{name} = ?" for name in fields)
        connection.execute(
            f"UPDATE permits SET {assignments} WHERE permit_id = ?",
            (*fields.values(), permit_id),
        )

    def append_permit_audit(
        self,
        connection: sqlite3.Connection,
        permit_id: str,
        at: datetime,
        action: str,
        actor: str,
        source: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        row = connection.execute(
            "SELECT audit FROM permits WHERE permit_id = ?", (permit_id,)
        ).fetchone()
        entries = load(row["audit"], []) if row else []
        entries.append(
            {
                "at": format_time(at),
                "action": action,
                "actor": actor,
                "source": source,
                "details": details or {},
            }
        )
        connection.execute(
            "UPDATE permits SET audit = ?, updated_at = ? WHERE permit_id = ?",
            (dump(entries), format_time(at), permit_id),
        )
        self.audit(connection, at, "permit", permit_id, action, actor, source, details)

    # ---- 越线 ----

    def crossing(self, crossing_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM crossings WHERE crossing_id = ?", (crossing_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---- 事件 ----

    def event(self, event_id: str) -> dict[str, Any] | None:
        row = self._connection.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row else None

    def events(self, applied: bool | None = None) -> list[dict[str, Any]]:
        if applied is None:
            rows = self._connection.execute(
                "SELECT * FROM events ORDER BY received_at, event_id"
            )
        else:
            rows = self._connection.execute(
                "SELECT * FROM events WHERE applied = ? ORDER BY received_at, event_id",
                (1 if applied else 0,),
            )
        return [{**dict(row), "payload": load(row["payload"], {})} for row in rows]

    def mark_event_applied(self, connection: sqlite3.Connection, event_id: str) -> None:
        connection.execute("UPDATE events SET applied = 1 WHERE event_id = ?", (event_id,))

    # ---- audit ----

    def audit_log(self, entity: str | None = None, entity_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM audit"
        conditions = []
        arguments: list[Any] = []
        if entity is not None:
            conditions.append("entity = ?")
            arguments.append(entity)
        if entity_id is not None:
            conditions.append("entity_id = ?")
            arguments.append(entity_id)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY id"
        result = []
        for row in self._connection.execute(query, arguments):
            record = dict(row)
            record["details"] = load(record["details"], {})
            result.append(record)
        return result
