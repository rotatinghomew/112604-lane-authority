"""进区服务：观察、许可、撤回、设备回查与人工复核。

服务面向两条进区车道长期运行。所有状态在 SQLite 中，进程重启后继续推进
未完成的许可；同一辆车同一时刻只允许一条车道持有未交公的许可。

许可生命周期（``status``）：

``pending``
    已决定开闸并写向设备，但尚未收到设备确认，或确认与请求不一致。设备可能
    已经执行，因此不能撤回重发，只能向同一车道回查，或等待设备事件。
``armed``
    设备确认许可已收到且仍可用（**不是**越线证据）。
``cancel_requested``
    已向设备请求取消，等待设备给出 cancelled / crossed / unused / expired。
``expired``
    允许时段或设备许可有效期已过且没有越线记录；不能再用。
``cancelled``
    设备证明许可不能再使用（未越线）。
``crossed``
    设备记录了越线；``inbound.handoff`` 说明已交给场内还是留给现场核查。

``superseded`` 形态（``cancelled`` 且 ``superseded_by`` 有值）表示该车改走了
另一条车道；旧许可只作历史。

``resolution`` 说明许可为什么停在当前状态，``uncertainty`` 记录设备不确定的
每一步；``inbound.handoff`` 说明车辆现在该去哪。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import sqlite3
import threading
import uuid
from typing import Any, Callable

from .device import UnknownOutcome
from .feeds import format_time, parse_time
from .manifest import (
    Eligibility,
    ManifestBook,
    evaluate,
    paper_check_usable,
    suspended,
)
from .store import Store, load, utcnow

DEFAULT_PERMIT_MARGIN = timedelta(minutes=2)
CANCEL_TIMEOUT = timedelta(seconds=30)
OPEN_STATUSES = ("pending", "armed", "cancel_requested")


class ServiceError(Exception):
    """请求无法按业务规则执行。"""

    def __init__(self, code: str, detail: str, **extra: Any) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.extra = extra


@dataclass
class ReadingDecision:
    lane: str
    pass_id: str
    disposition: str
    reason: str
    vin: str | None
    eligibility: dict[str, Any] | None
    permit_id: str | None
    waiting_lanes: dict[str, str]
    reading: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "pass": self.pass_id,
            "disposition": self.disposition,
            "reason": self.reason,
            "vin": self.vin,
            "eligibility": self.eligibility,
            "permit": self.permit_id,
            "waiting_lanes": self.waiting_lanes,
            "reading": self.reading,
        }


class EntryService:
    def __init__(
        self,
        store: Store,
        device: Any,
        *,
        clock: Callable[[], datetime] = utcnow,
        permit_margin: timedelta = DEFAULT_PERMIT_MARGIN,
        lane_ports: dict[str, str] | None = None,
    ) -> None:
        self.store = store
        self.device = device
        self.clock = clock
        self.permit_margin = permit_margin
        self.lane_ports = dict(lane_ports or {})
        # 只用于把“判定 + 插入许可”合在一段临界区；跨进程互斥靠唯一索引。
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------

    def now(self) -> datetime:
        return self.clock()

    def book(self, at: datetime | None = None) -> ManifestBook:
        """只看在 ``at`` 时已经发出的修订，回放与实时判定同一口径。"""
        return ManifestBook.from_store(self.store, upto=at)

    def port_for(self, lane: str) -> str | None:
        if lane in self.lane_ports:
            return self.lane_ports[lane]
        return self.store.port_for_lane(lane)

    def _audit(
        self, at: datetime, entity: str, entity_id: str, action: str, actor: str, source: str, details=None
    ) -> None:
        with self.store.write() as connection:
            self.store.audit(connection, at, entity, entity_id, action, actor, source, details or {})

    def _ensure_camera_lane(self, lane: str, camera: str) -> None:
        station = self.store.station(camera)
        if station is None:
            raise ServiceError("unknown_camera", f"未登记的摄像机 {camera}")
        if station["kind"] != "camera":
            raise ServiceError("not_a_camera", f"{camera} 不是摄像机")
        if station["lane"] != lane:
            raise ServiceError(
                "camera_lane_mismatch",
                f"摄像机 {camera} 归属 {station['lane']}，而非 {lane}",
            )

    # ------------------------------------------------------------------
    # 输入：名单、观测、纸单、设备归属
    # ------------------------------------------------------------------

    def ingest_manifest(self, record: dict[str, Any]) -> dict[str, Any]:
        moment = self.now()
        recorded = self.store.record_manifest_row(record, moment)
        stopped = []
        if recorded:
            # 同一 VIN 的新修订一到，旧修订下未使用的许可立即失效。
            book = self.book(moment)
            for permit in self.store.open_permits():
                if permit["manifest"] != record["manifest"] or permit["vin"] != record["vin"]:
                    continue
                if not book.is_latest_for_vin(
                    permit["manifest"], permit["revision"], permit["vin"]
                ):
                    stopped.append(self._stop_permit(permit, moment, actor="feed"))
        return {
            "recorded": recorded,
            "manifest": record["manifest"],
            "revision": record["revision"],
            "vin": record["vin"],
            "stopped": stopped,
        }

    def ingest_observation(self, record: dict[str, Any]) -> dict[str, Any]:
        self._ensure_camera_lane(record["lane"], record["camera"])
        recorded = self.store.record_observation(record, self.now())
        return {
            "recorded": recorded,
            "lane": record["lane"],
            "pass": record["pass"],
            "sequence": record["sequence"],
        }

    def ingest_paper_check(self, record: dict[str, Any]) -> dict[str, Any]:
        recorded = self.store.record_paper_check(record, self.now())
        return {"recorded": recorded, "check": record["check"]}

    def ingest_station(self, record: dict[str, Any]) -> dict[str, Any]:
        recorded = self.store.record_station(record, self.now())
        return {"recorded": recorded, "device": record["device"]}

    # ------------------------------------------------------------------
    # 观测处置
    # ------------------------------------------------------------------

    def evaluate_reading(
        self,
        lane: str,
        pass_id: str,
        *,
        at: datetime | None = None,
        ignore_permit: str | None = None,
    ) -> ReadingDecision:
        """把一次通过批次的读数收敛成结论。

        ``ignore_permit`` 用于改道预检：判断目标批次本身是否成立，而不把待改
        道的旧许可算成“车辆已在别处”。
        """
        moment = at or self.now()
        records = self.store.observations_for_pass(lane, pass_id)
        if not records:
            raise ServiceError("unknown_pass", f"车道 {lane} 没有通过批次 {pass_id} 的读数")
        usable = [row for row in records if row["_seen_at"] <= moment]
        if not usable:
            raise ServiceError("pass_not_yet_seen", "读数时间晚于判定时刻")

        consumed = self.store.crossing_for_lane_pass(lane, pass_id)
        if consumed is not None:
            return ReadingDecision(
                lane, pass_id, "deny", "pass_consumed", consumed["vin"], None, None,
                self._waiting(),
                {"lane": lane, "pass": pass_id, "crossing_id": consumed["crossing_id"]},
            )

        port = self.port_for(lane)
        sets = [set(row["candidates"]) for row in usable]
        narrowed = set.intersection(*sets) if sets else set()
        vin = next(iter(narrowed)) if len(narrowed) == 1 else None
        reading = {
            "lane": lane,
            "pass": pass_id,
            "camera": usable[-1]["camera"],
            "seen_at": usable[-1]["seen_at"],
            "sequence": usable[-1]["sequence"],
            "readings": len(usable),
            "candidates": sorted(narrowed) if vin is None else [vin],
            "vin": vin,
        }

        if vin is not None:
            eligibility = evaluate(self.book(moment), vin, moment, port)
            if suspended(self.store, vin, moment) is not None:
                eligibility = Eligibility(
                    False, "regulatory_hold", eligibility.manifest, eligibility.revision
                )
            reading["eligibility"] = eligibility.as_dict()
            if eligibility.eligible:
                return self._ready_decision(reading, eligibility, ignore_permit)
            return ReadingDecision(
                lane, pass_id, "deny", eligibility.reason, vin,
                eligibility.as_dict(), None, self._waiting(), reading,
            )

        if narrowed:
            reason = "multiple_candidates"
            disposition = "manual"
        else:
            nonempty = [row for row in usable if row["candidates"]]
            reason = "conflicting_readings" if nonempty else "no_candidate"
            disposition = "manual" if nonempty else "hold"
        reading["eligibility"] = None
        return ReadingDecision(
            lane, pass_id, disposition, reason, None, None, None, self._waiting(), reading,
        )

    def _ready_decision(
        self, reading: dict[str, Any], eligibility: Eligibility, ignore_permit: str | None = None
    ) -> ReadingDecision:
        vin = reading["vin"]
        crossing = self.store.crossing_for_vin(vin)
        if crossing is not None:
            # 同一次越线只交接一次：已入场的车不再申请开闸。
            return ReadingDecision(
                reading["lane"], reading["pass"], "existing", "already_crossed", vin,
                eligibility.as_dict(), None, self._waiting(), reading,
            )
        existing = self.store.permit_holding_vehicle(vin)
        if existing is not None and existing["permit_id"] == ignore_permit:
            existing = None
        if existing is not None:
            if existing["lane"] == reading["lane"] and existing["pass"] == reading["pass"]:
                return ReadingDecision(
                    reading["lane"], reading["pass"], "existing", "already_permitted", vin,
                    eligibility.as_dict(), existing["permit_id"], self._waiting(), reading,
                )
            return ReadingDecision(
                reading["lane"], reading["pass"], "wait", "vehicle_permitted_elsewhere", vin,
                eligibility.as_dict(), existing["permit_id"], self._waiting(), reading,
            )
        return ReadingDecision(
            reading["lane"], reading["pass"], "ready", "unique_valid_match", vin,
            eligibility.as_dict(), None, self._waiting(), reading,
        )

    def _waiting(self) -> dict[str, str]:
        return {permit["lane"]: permit["permit_id"] for permit in self.store.open_permits()}

    # ------------------------------------------------------------------
    # 申请与许可
    # ------------------------------------------------------------------

    def _permit_expiry(self, vin: str, moment: datetime, port: str | None) -> datetime:
        """许可有效期 = 当前准入时段的结束 + 短暂缓冲。"""
        entries = [
            entry
            for entry in self.book(moment).latest_entries_for_vin(vin)
            if entry.from_at <= moment < entry.until_at
            and entry.action == "admit"
            and (port is None or entry.port == port)
        ]
        if not entries:
            raise ServiceError("not_eligible", "该车不在有效准入时段")
        newest = max(entry.from_at for entry in entries)
        return max(entry.until_at for entry in entries if entry.from_at == newest) + self.permit_margin

    def request_permit(
        self,
        lane: str,
        pass_id: str,
        *,
        vin: str | None = None,
        actor: str = "lane",
        at: datetime | None = None,
    ) -> dict[str, Any]:
        moment = at or self.now()
        with self._lock:
            decision = self.evaluate_reading(lane, pass_id, at=moment)
            if vin is not None and decision.vin is not None and vin != decision.vin:
                raise ServiceError(
                    "vin_mismatch",
                    f"当前读数唯一结论为 {decision.vin}，不接受 {vin}",
                    decision=decision.as_dict(),
                )
            if decision.disposition == "existing":
                if decision.permit_id is None:
                    raise ServiceError("already_crossed", "该车已有越线交接记录", decision=decision.as_dict())
                return self.permit_status(decision.permit_id, at=moment)
            if decision.disposition == "wait":
                raise ServiceError(
                    "vehicle_elsewhere", "该车已由另一条车道持有许可", decision=decision.as_dict()
                )
            if decision.disposition != "ready":
                approval = self.store.approved_review(lane, pass_id, moment)
                if approval is None:
                    raise ServiceError("not_permitted", decision.reason, decision=decision.as_dict())
                if decision.vin is not None and decision.vin != approval["vin"]:
                    raise ServiceError(
                        "vin_mismatch", "复核车辆与读数不一致", decision=decision.as_dict()
                    )
                if vin is not None and vin != approval["vin"]:
                    raise ServiceError("vin_mismatch", "复核车辆与请求车辆不一致")
                eligibility = evaluate(self.book(moment), approval["vin"], moment, self.port_for(lane))
                if not eligibility.eligible or suspended(self.store, approval["vin"], moment):
                    raise ServiceError(
                        "not_permitted", "复核不能覆盖当前名单状态或监管暂停",
                        decision=decision.as_dict(),
                    )
                decision = ReadingDecision(
                    lane, pass_id, "ready", "paper_review", approval["vin"],
                    eligibility.as_dict(), None, decision.waiting_lanes, decision.reading,
                )
            return self._arm_permit(lane, pass_id, decision.vin, moment, actor=actor)

    def _check_pass_available(self, lane: str, pass_id: str, vin: str) -> None:
        if self.store.crossing_for_lane_pass(lane, pass_id) is not None:
            raise ServiceError("pass_consumed", "该地感批次已经完成一次越线，不能换车使用")
        for permit in self.store.permits_for_lane_pass(lane, pass_id):
            if permit["vin"] != vin:
                raise ServiceError("pass_in_use", "该地感批次已属于另一辆车")

    def _arm_permit(
        self, lane: str, pass_id: str, vin: str, moment: datetime, *, actor: str
    ) -> dict[str, Any]:
        self._check_pass_available(lane, pass_id, vin)
        eligibility = evaluate(self.book(moment), vin, moment, self.port_for(lane))
        if not eligibility.eligible or suspended(self.store, vin, moment):
            raise ServiceError("not_permitted", "当前名单状态或监管暂停不允许开闸")
        expires = self._permit_expiry(vin, moment, self.port_for(lane))
        permit_id = f"perm-{uuid.uuid4().hex[:12]}"
        record = {
            "permit_id": permit_id,
            "vehicle": vin,
            "lane": lane,
            "pass": pass_id,
            "vin": vin,
            "port": self.port_for(lane) or "",
            "manifest": eligibility.manifest or "",
            "revision": eligibility.revision or 0,
            "armed_at": format_time(moment),
            "expires_at": format_time(expires),
            "status": "pending",
            "resolution": "requesting",
            "uncertainty": [],
            "audit": [],
            "inbound": "none",
            "updated_at": format_time(moment),
        }
        try:
            with self.store.write() as connection:
                self.store.insert_permit(record)
                self.store.audit(
                    connection, moment, "permit", permit_id, "requested", actor, "lane",
                    {
                        "lane": lane,
                        "pass": pass_id,
                        "vin": vin,
                        "manifest": eligibility.manifest,
                        "revision": eligibility.revision,
                        "expires_at": format_time(expires),
                    },
                )
        except sqlite3.IntegrityError as error:
            # 另一入口或进程抢先插入了该车的未交公许可。
            holder = self.store.permit_holding_vehicle(vin)
            raise ServiceError(
                "vehicle_elsewhere",
                "该车已由另一条车道持有许可",
                permit=holder["permit_id"] if holder else None,
            ) from error
        return self._deliver_permit(permit_id, moment, actor=actor)

    def _deliver_permit(self, permit_id: str, moment: datetime, *, actor: str) -> dict[str, Any]:
        permit = self.store.permit(permit_id)
        assert permit is not None
        try:
            reply = self.device.arm(permit["lane"], permit)
        except UnknownOutcome as unknown:
            self._record_uncertainty(permit_id, moment, "arm_unknown", unknown.detail, actor)
            return self.permit_status(permit_id, at=moment)
        self._apply_device_reply(permit_id, reply, moment, source="device")
        return self.permit_status(permit_id, at=moment)

    def _record_uncertainty(
        self, permit_id: str, moment: datetime, kind: str, detail: str, actor: str
    ) -> None:
        with self.store.write() as connection:
            row = connection.execute(
                "SELECT uncertainty FROM permits WHERE permit_id = ?", (permit_id,)
            ).fetchone()
            if row is None:
                return
            entries = load(row["uncertainty"], [])
            entries.append({"at": format_time(moment), "kind": kind, "detail": detail})
            connection.execute(
                "UPDATE permits SET uncertainty = ?, resolution = ?, updated_at = ? WHERE permit_id = ?",
                (json.dumps(entries, ensure_ascii=False), "device_unconfirmed", format_time(moment), permit_id),
            )
            self.store.audit(
                connection, moment, "permit", permit_id, kind, actor, "device", {"detail": detail}
            )

    def _apply_device_reply(
        self, permit_id: str, reply: Any, moment: datetime, *, source: str
    ) -> None:
        body = reply.body or {}
        # 只认与请求对应的许可；回话里的许可号对不上时仍按设备不确定处理。
        if reply.status == 200 and body.get("permit") not in (None, permit_id):
            with self.store.write() as connection:
                self.store.append_permit_audit(
                    connection, permit_id, moment, "device_reply_mismatch", "device", source,
                    {"permit_in_reply": body.get("permit")},
                )
            self._record_uncertainty(
                permit_id, moment, "reply_mismatch",
                f"设备回话的许可号为 {body.get('permit')}", "device",
            )
            return
        status = body.get("status")
        with self.store.write() as connection:
            row = connection.execute(
                "SELECT * FROM permits WHERE permit_id = ?", (permit_id,)
            ).fetchone()
            if row is None:
                return
            permit = self.store._permit(row)
            if reply.status == 404 or status == "unused":
                if permit["status"] in OPEN_STATUSES:
                    self.store.update_permit(
                        connection, permit_id,
                        status="cancelled",
                        resolution="unused_confirmed" if permit["status"] == "cancel_requested"
                        else "device_never_received",
                        updated_at=format_time(moment),
                    )
                    self.store.append_permit_audit(
                        connection, permit_id, moment, "cancelled", "device", source, {"evidence": "unused"}
                    )
                return
            if status == "armed":
                if permit["status"] == "pending":
                    self.store.update_permit(
                        connection, permit_id,
                        status="armed", resolution="armed_confirmed", updated_at=format_time(moment),
                    )
                    self.store.append_permit_audit(
                        connection, permit_id, moment, "armed", "device", source, {}
                    )
                return
            if status == "cancelled":
                if permit["status"] in OPEN_STATUSES:
                    self.store.update_permit(
                        connection, permit_id,
                        status="cancelled", resolution="cancelled_confirmed", updated_at=format_time(moment),
                    )
                    self.store.append_permit_audit(
                        connection, permit_id, moment, "cancelled", "device", source, {}
                    )
                return
            if status == "expired":
                if permit["status"] in OPEN_STATUSES:
                    self.store.update_permit(
                        connection, permit_id,
                        status="expired", resolution="device_expired", updated_at=format_time(moment),
                    )
                    self.store.append_permit_audit(
                        connection, permit_id, moment, "expired", "device", source, {}
                    )
                return
            if status == "crossed":
                self._apply_crossing(
                    connection, permit,
                    body.get("crossing_id") or f"{permit['lane']}:{body.get('pass')}:{permit_id}",
                    body.get("pass") or permit["pass"],
                    body.get("lane") or permit["lane"],
                    parse_time(body["crossed_at"]) if body.get("crossed_at") else None,
                    moment, source,
                )
                return
            self.store.append_permit_audit(
                connection, permit_id, moment, "device_reply", "device", source,
                {"status": reply.status, "body": body},
            )

    def _apply_crossing(
        self,
        connection: Any,
        permit: dict[str, Any],
        crossing_id: str,
        crossed_pass: str,
        crossed_lane: str,
        crossed_at: datetime | None,
        moment: datetime,
        source: str,
    ) -> str:
        existing = connection.execute(
            "SELECT handoff FROM crossings WHERE crossing_id = ?", (crossing_id,)
        ).fetchone()
        if existing is not None:
            return existing["handoff"]
        # 同一次物理越线可能换了编号重传；同许可或同车道批次只交接一次。
        other = connection.execute(
            "SELECT crossing_id, handoff FROM crossings WHERE permit_id = ? OR (lane = ? AND pass = ?)",
            (permit["permit_id"], crossed_lane, crossed_pass),
        ).fetchone()
        if other is not None:
            connection.execute(
                "INSERT OR IGNORE INTO crossings"
                " (crossing_id, permit_id, lane, pass, vin, crossed_at, verified, handoff, handoff_at,"
                " note, received_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 0, ?, NULL, ?, ?)",
                (
                    crossing_id, permit["permit_id"], crossed_lane, crossed_pass, permit["vin"],
                    format_time(crossed_at) if crossed_at else None, other["handoff"],
                    f"同一次越线的重复编号，归属 {other['crossing_id']}", format_time(moment),
                ),
            )
            self.store.audit(
                connection, moment, "crossing", crossing_id, "duplicate", "device", source,
                {"permit": permit["permit_id"], "first": other["crossing_id"]},
            )
            return other["handoff"]

        verified, handoff, note = self._verify_crossing(
            permit, crossed_lane, crossed_pass, crossed_at
        )
        connection.execute(
            "INSERT INTO crossings"
            " (crossing_id, permit_id, lane, pass, vin, crossed_at, verified, handoff, handoff_at,"
            " note, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                crossing_id, permit["permit_id"], crossed_lane, crossed_pass, permit["vin"],
                format_time(crossed_at) if crossed_at else None,
                1 if verified else 0, handoff,
                format_time(moment) if handoff == "inbound" else None,
                note, format_time(moment),
            ),
        )
        if permit["status"] == "crossed":
            return handoff
        self.store.update_permit(
            connection, permit["permit_id"],
            status="crossed",
            resolution="crossing_verified" if verified else "crossing_needs_review",
            crossing_id=crossing_id,
            crossed_at=format_time(crossed_at) if crossed_at else None,
            crossed_pass=crossed_pass,
            crossed_lane=crossed_lane,
            inbound=handoff,
            inbound_at=format_time(moment) if handoff == "inbound" else None,
            inbound_reference=crossing_id if handoff == "inbound" else None,
            inbound_note=note,
            updated_at=format_time(moment),
        )
        self.store.append_permit_audit(
            connection, permit["permit_id"], moment, "crossed", "device", source,
            {"crossing_id": crossing_id, "verified": verified, "handoff": handoff, "note": note},
        )
        return handoff

    def _verify_crossing(
        self,
        permit: dict[str, Any],
        crossed_lane: str,
        crossed_pass: str,
        crossed_at: datetime | None,
    ) -> tuple[bool, str, str]:
        if crossed_lane != permit["lane"]:
            return False, "pending_review", "越线车道与许可车道不符"
        if crossed_pass != permit["pass"]:
            return False, "pending_review", "越线批次与许可批次不符"
        if crossed_at is None:
            return False, "pending_review", "缺少越线时间，无法核对许可有效期"
        if crossed_at > permit["expires_dt"]:
            return False, "pending_review", "越线时间晚于许可有效期"
        if crossed_at < permit["armed_dt"]:
            return False, "pending_review", "越线时间早于许可写出时间"
        if permit["status"] in ("cancelled", "expired"):
            # 设备已证明许可停用却仍记下越线：先保留越线，交现场核查。
            return False, "pending_review", "设备先证明许可停用，随后又记录了越线"
        notes: list[str] = []
        if permit["manifest"]:
            latest = self.store.latest_revision_published_at(
                permit["manifest"], crossed_at, permit["vin"]
            )
            if latest is not None and latest > permit["revision"]:
                notes.append("越线发生在名单更正之后，交场内扣留")
            if self.store.hold_effective_at(permit["vin"], crossed_at) is not None:
                notes.append("越线发生在监管暂停之后，交场内扣留")
        return True, "inbound", "；".join(notes)

    # ------------------------------------------------------------------
    # 状态与值班查询
    # ------------------------------------------------------------------

    def permit_status(self, permit_id: str, *, at: datetime | None = None) -> dict[str, Any]:
        permit = self.store.permit(permit_id)
        if permit is None:
            raise ServiceError("unknown_permit", f"未知许可 {permit_id}")
        return self._permit_view(permit, at or self.now())

    def _permit_view(self, permit: dict[str, Any], moment: datetime) -> dict[str, Any]:
        crossing = self.store.crossing(permit["crossing_id"]) if permit["crossing_id"] else None
        return {
            "permit": permit["permit_id"],
            "vehicle": permit["vehicle"],
            "vin": permit["vin"],
            "lane": permit["lane"],
            "pass": permit["pass"],
            "manifest": permit["manifest"],
            "revision": permit["revision"],
            "status": permit["status"],
            "resolution": permit["resolution"],
            "armed_at": permit["armed_at"],
            "expires_at": permit["expires_at"],
            "cancel_requested_at": permit["cancel_requested_at"],
            "superseded_by": permit["superseded_by"],
            "crossing": None if crossing is None else {
                "crossing_id": crossing["crossing_id"],
                "verified": bool(crossing["verified"]),
                "crossed_at": crossing["crossed_at"],
                "handoff": crossing["handoff"],
                "note": crossing["note"],
            },
            "inbound": {
                "handoff": permit["inbound"],
                "at": permit["inbound_at"],
                "reference": permit["inbound_reference"],
                "note": permit["inbound_note"],
            },
            "uncertainty": permit["uncertainty"],
            "audit": permit["audit"],
            "device_query_required": permit["status"] == "pending",
            "waiting_reason": self._waiting_reason(permit),
            "explain": self._explain(permit),
        }

    @staticmethod
    def _waiting_reason(permit: dict[str, Any]) -> str | None:
        """值班员看到的“为什么在等”。"""
        if permit["status"] == "pending":
            return "等待设备证明许可是否已执行（只能向同一车道回查）"
        if permit["status"] == "cancel_requested":
            return "已请求撤回，等待设备证明不能再使用"
        if permit["status"] == "armed":
            return None
        if permit["inbound"] == "pending_review":
            return "越线需现场核查，未作为正常入场"
        return None

    @staticmethod
    def _explain(permit: dict[str, Any]) -> dict[str, Any]:
        destination = {
            "inbound": "inbound",
            "pending_review": "site_review",
        }.get(permit["inbound"], "waiting")
        return {
            "authority": {
                "manifest": permit["manifest"],
                "revision": permit["revision"],
                "can_open_now": permit["status"] in OPEN_STATUSES,
            },
            "actual": {
                "status": permit["status"],
                "resolution": permit["resolution"],
                "crossed": permit["crossing_id"] is not None,
                "crossed_at": permit["crossed_at"],
            },
            "destination": destination,
        }

    def list_permits(self, *, lane: str | None = None) -> list[dict[str, Any]]:
        permits = self.store.permits_for_lane(lane) if lane else self.store.permits()
        return [self._permit_view(permit, self.now()) for permit in permits]

    def lane_status(self, lane: str) -> dict[str, Any]:
        moment = self.now()
        permits = self.store.permits_for_lane(lane)
        open_permits = [permit for permit in permits if permit["status"] in OPEN_STATUSES]
        occupancy = [
            {
                "permit": permit["permit_id"],
                "pass": permit["pass"],
                "vin": permit["vin"],
                "status": permit["status"],
                "expires_at": permit["expires_at"],
            }
            for permit in open_permits
        ]
        return {
            "lane": lane,
            "open": [self._permit_view(permit, moment) for permit in open_permits],
            "occupancy": occupancy,
            "recent": [self._permit_view(permit, moment) for permit in permits[-10:]],
        }

    def waiting(self) -> dict[str, Any]:
        """值班查询：仍占用哪条车道、为什么在等、是否已交场内。"""
        moment = self.now()
        open_permits = self.store.open_permits()
        lanes: dict[str, list[dict[str, Any]]] = {}
        for permit in open_permits:
            lanes.setdefault(permit["lane"], []).append(self._permit_view(permit, moment))
        views = [
            self._permit_view(permit, moment)
            for permit in self.store.permits()
            if permit["inbound"] in ("inbound", "pending_review")
        ]
        return {
            "at": format_time(moment),
            "lanes": lanes,
            "awaiting_device": [
                self._permit_view(permit, moment)
                for permit in open_permits
                if permit["status"] in ("pending", "cancel_requested")
            ],
            "handed_to_site": [view for view in views if view["inbound"]["handoff"] == "inbound"],
            "site_review": [view for view in views if view["inbound"]["handoff"] == "pending_review"],
            "outstanding_permits": len(open_permits),
        }

    # ------------------------------------------------------------------
    # 撤回与改道
    # ------------------------------------------------------------------

    def request_cancel(
        self, permit_id: str, *, actor: str = "supervisor", reason: str = "manual"
    ) -> dict[str, Any]:
        """请求撤回尚未使用的许可。只有设备证明不能用时才算撤回成功。"""
        with self._lock:
            moment = self.now()
            permit = self.store.permit(permit_id)
            if permit is None:
                raise ServiceError("unknown_permit", f"未知许可 {permit_id}")
            if permit["status"] not in OPEN_STATUSES:
                return self.permit_status(permit_id, at=moment)
            with self.store.write() as connection:
                self.store.update_permit(
                    connection, permit_id,
                    status="cancel_requested",
                    resolution="cancel_requested",
                    cancel_requested_at=format_time(moment),
                    updated_at=format_time(moment),
                )
                self.store.append_permit_audit(
                    connection, permit_id, moment, "cancel_requested", actor, "supervisor",
                    {"reason": reason},
                )
            return self._deliver_cancel(permit_id, moment, actor=actor)

    def _deliver_cancel(self, permit_id: str, moment: datetime, *, actor: str) -> dict[str, Any]:
        permit = self.store.permit(permit_id)
        assert permit is not None
        try:
            reply = self.device.cancel(permit["lane"], permit_id)
        except UnknownOutcome as unknown:
            self._record_uncertainty(permit_id, moment, "cancel_unknown", unknown.detail, actor)
            return self.permit_status(permit_id, at=moment)
        self._apply_device_reply(permit_id, reply, moment, source="device")
        return self.permit_status(permit_id, at=moment)

    def switch_lane(
        self, permit_id: str, target_lane: str, pass_id: str, *, actor: str = "dispatcher"
    ) -> dict[str, Any]:
        """把尚未开闸的车从原车道改到空闲车道的另一通过批次。

        只有原车道许可被设备证明不能再使用时才改道；新许可与旧许可互斥，
        且目标车道该批次必须唯一、有效匹配同一辆车。
        """
        with self._lock:
            moment = self.now()
            permit = self.store.permit(permit_id)
            if permit is None:
                raise ServiceError("unknown_permit", f"未知许可 {permit_id}")
            if permit["lane"] == target_lane:
                raise ServiceError("same_lane", "改道目标就是原车道")
            if permit["status"] not in OPEN_STATUSES:
                raise ServiceError("permit_closed", "该许可已结束，不能改道")
            decision = self.evaluate_reading(
                target_lane, pass_id, at=moment, ignore_permit=permit_id
            )
            if decision.vin != permit["vin"]:
                raise ServiceError(
                    "vin_mismatch",
                    f"目标车道读数唯一结论为 {decision.vin}，不是 {permit['vin']}",
                    decision=decision.as_dict(),
                )
            if decision.disposition != "ready":
                raise ServiceError("not_permitted", decision.reason, decision=decision.as_dict())
            cancelled = self.request_cancel(permit_id, actor=actor, reason="switch_lane")
            if cancelled["status"] != "cancelled":
                raise ServiceError(
                    "cancel_not_proven",
                    "设备尚未证明原许可能停用，不能在新车道重发",
                    source_permit=cancelled,
                )
            new_permit = self.request_permit(target_lane, pass_id, actor=actor, at=moment)
            with self.store.write() as connection:
                self.store.update_permit(
                    connection, permit_id, superseded_by=new_permit["permit"],
                    updated_at=format_time(moment),
                )
                self.store.append_permit_audit(
                    connection, permit_id, moment, "switched", actor, "dispatcher",
                    {"target_lane": target_lane, "target_pass": pass_id,
                     "new_permit": new_permit["permit"]},
                )
            return new_permit

    # ------------------------------------------------------------------
    # 设备回查与回执
    # ------------------------------------------------------------------

    def reconcile_permit(self, permit_id: str, *, actor: str = "supervisor") -> dict[str, Any]:
        """向同一车道回查一次；不重发许可，也不换车道。"""
        with self._lock:
            moment = self.now()
            permit = self.store.permit(permit_id)
            if permit is None:
                raise ServiceError("unknown_permit", f"未知许可 {permit_id}")
            if permit["status"] not in OPEN_STATUSES:
                return self.permit_status(permit_id, at=moment)
            try:
                reply = self.device.query(permit["lane"], permit_id)
            except UnknownOutcome as unknown:
                self._record_uncertainty(permit_id, moment, "query_unknown", unknown.detail, actor)
                return self.permit_status(permit_id, at=moment)
            self._apply_device_reply(permit_id, reply, moment, source="device")
            return self.permit_status(permit_id, at=moment)

    def reconcile_lane(self, lane: str, *, actor: str = "supervisor") -> dict[str, Any]:
        results = [
            self.reconcile_permit(permit["permit_id"], actor=actor)
            for permit in self.store.open_permits(lane)
        ]
        return {"lane": lane, "permits": results}

    def receive_event(self, event: dict[str, Any], *, source: str = "device") -> dict[str, Any]:
        """接收设备事件回执。可乱序、可重复；越线先到也能正确归属。

        回执先入库再处理。回执指向的许可还没到（例如开闸确认晚于越线）时
        先记下，等许可出现后由 :meth:`tick` 再补处理，不会丢事件。
        """
        with self._lock:
            moment = self.now()
            event_id = event["event_id"]
            with self.store.write() as connection:
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO events"
                    " (event_id, permit_id, lane, pass, kind, occurred_at, crossing_id, payload,"
                    " received_at, applied)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                    (
                        event_id, event["permit"], event["lane"], event["pass"], event["kind"],
                        event["occurred_at"], event.get("crossing_id"),
                        json.dumps(event, ensure_ascii=False), format_time(moment),
                    ),
                ).rowcount
                if inserted:
                    self.store.audit(
                        connection, moment, "event", event_id, "received", source, "events", event
                    )
            if not inserted:
                return {"event": event_id, "duplicate": True}
            return {"event": event_id, "duplicate": False, **self._apply_event(event, moment)}

    def _mark_applied(self, event_id: str) -> None:
        with self.store.write() as connection:
            self.store.mark_event_applied(connection, event_id)

    def _replay_unapplied_events(self, moment: datetime) -> int:
        """把因许可未到而搁置的回执补处理一遍。"""
        applied = 0
        for record in self.store.events(applied=False):
            outcome = self._apply_event(record["payload"], moment)
            if outcome.get("matched") or outcome.get("terminal"):
                with self.store.write() as connection:
                    self.store.mark_event_applied(connection, record["event_id"])
                applied += 1
        return applied

    def _apply_event(self, event: dict[str, Any], moment: datetime) -> dict[str, Any]:
        permit_id = event["permit"]
        permit = self.store.permit(permit_id)
        kind = event["kind"]
        if kind == "crossed":
            crossing_id = event.get("crossing_id") or f"{event['lane']}:{event['pass']}:{permit_id}"
            with self.store.write() as connection:
                if permit is None:
                    connection.execute(
                        "INSERT OR IGNORE INTO crossings"
                        " (crossing_id, permit_id, lane, pass, vin, crossed_at, verified, handoff,"
                        " handoff_at, note, received_at)"
                        " VALUES (?, ?, ?, ?, ?, ?, 0, 'pending_review', NULL, ?, ?)",
                        (
                            crossing_id, permit_id, event["lane"], event["pass"], "",
                            event["occurred_at"], "许可未知的越线，留现场核查",
                            format_time(moment),
                        ),
                    )
                    self.store.audit(
                        connection, moment, "crossing", crossing_id, "unmatched", "device", "events",
                        {"permit": permit_id},
                    )
                    self.store.mark_event_applied(connection, event["event_id"])
                    return {"matched": False, "terminal": True, "handoff": "pending_review"}
                handoff = self._apply_crossing(
                    connection, permit, crossing_id, event["pass"], event["lane"],
                    parse_time(event["occurred_at"]), moment, "events",
                )
            self._mark_applied(event["event_id"])
            return {"matched": True, "handoff": handoff}
        if permit is None:
            return {"matched": False, "terminal": False}
        if kind == "armed" and permit["status"] == "pending":
            with self.store.write() as connection:
                self.store.update_permit(
                    connection, permit_id,
                    status="armed", resolution="armed_confirmed", updated_at=format_time(moment),
                )
                self.store.append_permit_audit(
                    connection, permit_id, moment, "armed", "device", "events",
                    {"occurred_at": event["occurred_at"]},
                )
            self._mark_applied(event["event_id"])
            return {"matched": True}
        if kind == "expired" and permit["status"] in OPEN_STATUSES:
            with self.store.write() as connection:
                self.store.update_permit(
                    connection, permit_id,
                    status="expired", resolution="device_expired", updated_at=format_time(moment),
                )
                self.store.append_permit_audit(
                    connection, permit_id, moment, "expired", "device", "events",
                    {"occurred_at": event["occurred_at"]},
                )
            self._mark_applied(event["event_id"])
            return {"matched": True}
        self._mark_applied(event["event_id"])
        return {"matched": True}

    # ------------------------------------------------------------------
    # 复核
    # ------------------------------------------------------------------

    def review(
        self,
        lane: str,
        pass_id: str,
        check_id: str,
        *,
        actor: str | None = None,
        at: datetime | None = None,
    ) -> dict[str, Any]:
        """值班员依据该次观察和纸单登记复核。

        复核只用于消除观察的身份歧义：纸单须存在、修订仍是最新、车辆仍可
        入区且无监管暂停。复核本身不直接开闸，也不延长时限。
        """
        moment = at or self.now()
        check = self.store.paper_check(check_id)
        if check is None:
            raise ServiceError("unknown_check", f"未知纸单复核 {check_id}")
        if check["lane"] != lane or check["pass"] != pass_id:
            raise ServiceError("check_mismatch", "纸单复核不属于该车道与该通过批次")
        if actor is not None and actor != check["officer"]:
            raise ServiceError("officer_mismatch", "复核人须为该纸单记录的当班人员")
        records = [
            row
            for row in self.store.observations_for_pass(lane, pass_id)
            if parse_time(row["seen_at"]) <= moment
        ]
        if not records:
            raise ServiceError("unknown_pass", f"车道 {lane} 没有通过批次 {pass_id} 的读数")
        observed = set().union(*(set(row["candidates"]) for row in records))
        # 观察没有读到任何候选时，纸单是该次通过的唯一凭据；读到过候选但不含
        # 纸单车辆，说明两者对不上，不能凭纸单改判。
        if observed and check["vin"] not in observed:
            raise ServiceError("check_vin_not_observed", "纸单车辆不在该次观察的候选里")
        eligibility = paper_check_usable(self.book(moment), check, moment)
        if not eligibility.eligible:
            self._audit(
                moment, "review", check_id, "refused", actor or check["officer"], "paper_checks",
                {"reason": eligibility.reason},
            )
            raise ServiceError(
                "review_not_permitted", eligibility.reason, eligibility=eligibility.as_dict()
            )
        if suspended(self.store, check["vin"], moment) is not None:
            self._audit(
                moment, "review", check_id, "refused", actor or check["officer"], "paper_checks",
                {"reason": "regulatory_hold"},
            )
            raise ServiceError("review_not_permitted", "regulatory_hold")
        self.store.record_review(check, moment, actor or check["officer"])
        self._audit(
            moment, "review", check_id, "approved", actor or check["officer"], "paper_checks",
            {
                "lane": lane, "pass": pass_id, "vin": check["vin"],
                "manifest": check["manifest"], "revision": check["revision"],
            },
        )
        return {
            "check": check_id,
            "lane": lane,
            "pass": pass_id,
            "vin": check["vin"],
            "eligibility": eligibility.as_dict(),
            "permitted": True,
        }

    # ------------------------------------------------------------------
    # 监管暂停 / 名单更正
    # ------------------------------------------------------------------

    def publish_hold(
        self,
        manifest: str,
        revision: int,
        *,
        rows: list[dict[str, Any]],
        published_at: str,
        actor: str = "regulation",
    ) -> dict[str, Any]:
        """收到监管暂停或有效名单更正：停发新许可，停用未使用许可。"""
        moment = self.now()
        self.store.record_hold(manifest, revision, "hold", published_at, rows, moment)
        if parse_time(published_at) > moment:
            # 还没生效的暂停只登记，等生效后由下一轮判定停用。
            return {"manifest": manifest, "revision": revision, "stopped": []}
        held_vins = {row.get("vin") for row in rows if row.get("vin")}
        stopped = []
        for permit in self.store.open_permits():
            # 逐车暂停按 VIN 生效，不受清单号限制；未写明车辆时只停本清单。
            if held_vins:
                if permit["vin"] not in held_vins:
                    continue
            elif permit["manifest"] != manifest:
                continue
            stopped.append(self._stop_permit(permit, moment, actor=actor))
        return {"manifest": manifest, "revision": revision, "stopped": stopped}

    def publish_revision(self, rows: list[dict[str, Any]], *, actor: str = "feed") -> dict[str, Any]:
        """收到同一清单的新修订。旧修订下的未完成许可停止，历史保留。"""
        moment = self.now()
        manifests = {row["manifest"] for row in rows}
        for row in rows:
            self.store.record_manifest_row(row, moment)
        book = self.book(moment)
        stopped = []
        for permit in self.store.open_permits():
            if permit["manifest"] not in manifests:
                continue
            if not book.is_latest_for_vin(permit["manifest"], permit["revision"], permit["vin"]):
                stopped.append(self._stop_permit(permit, moment, actor=actor))
        return {"manifests": sorted(manifests), "stopped": stopped}

    def _stop_permit(self, permit: dict[str, Any], moment: datetime, *, actor: str) -> dict[str, Any]:
        self._audit(
            moment, "permit", permit["permit_id"], "stop_requested", actor, "regulation",
            {"reason": "manifest_or_regulation_changed"},
        )
        return self.request_cancel(permit["permit_id"], actor=actor, reason="regulatory")

    # ------------------------------------------------------------------
    # 后台推进
    # ------------------------------------------------------------------

    def tick(self) -> dict[str, Any]:
        """周期任务：设备回查、过期处理。可安全重复调用。"""
        moment = self.now()
        rechecked = []
        for permit in self.store.open_permits():
            if permit["status"] == "pending":
                rechecked.append(self.reconcile_permit(permit["permit_id"], actor="service"))
            elif permit["status"] == "cancel_requested" and permit["cancel_requested_at"]:
                if moment - parse_time(permit["cancel_requested_at"]) >= CANCEL_TIMEOUT:
                    rechecked.append(self.reconcile_permit(permit["permit_id"], actor="service"))
        expired = self._expire_lapsed(moment)
        stopped = self._stop_for_effective_holds(moment)
        replayed = self._replay_unapplied_events(moment)
        return {
            "at": format_time(moment),
            "reconciled": rechecked,
            "expired": expired,
            "stopped_for_holds": stopped,
            "replayed_events": replayed,
        }

    def _stop_for_effective_holds(self, moment: datetime) -> list[str]:
        """暂停已生效但许可还没停的全补停一遍（例如暂停先于许可登记）。"""
        stopped = []
        for permit in self.store.open_permits():
            if self.store.hold_effective_at(permit["vin"], moment) is not None:
                self._stop_permit(permit, moment, actor="service")
                stopped.append(permit["permit_id"])
        return stopped

    def _expire_lapsed(self, moment: datetime) -> list[str]:
        expired = []
        for permit in self.store.open_permits():
            if permit["status"] != "armed" or moment < permit["expires_dt"]:
                continue
            with self.store.write() as connection:
                self.store.update_permit(
                    connection, permit["permit_id"],
                    status="expired", resolution="permit_expired", updated_at=format_time(moment),
                )
                self.store.append_permit_audit(
                    connection, permit["permit_id"], moment, "expired", "service", "clock", {}
                )
            expired.append(permit["permit_id"])
        return expired
