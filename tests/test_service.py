"""进区服务的业务行为：观察收敛、许可互斥、设备不确定、暂停与越线交接。"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import unittest

from entry_service import EntryService, Store, create_app
from entry_service.device import DeviceReply, ScriptedDeviceClient, UnknownOutcome
from entry_service.service import ServiceError


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"

VIN_A = "LTEST000000000013"
VIN_B = "LTEST000000000027"


def at(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def read_jsonl(name):
    return [
        json.loads(line)
        for line in (SAMPLES / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class Clock:
    def __init__(self, text="2026-11-27T01:15:10Z"):
        self.moment = at(text)

    def __call__(self):
        return self.moment

    def set(self, text):
        self.moment = at(text)


def load_samples(service):
    for line in read_jsonl("stations"):
        service.ingest_station(line)
    for line in read_jsonl("manifests"):
        service.ingest_manifest(line)
    for line in read_jsonl("observations"):
        service.ingest_observation(line)
    for line in read_jsonl("paper_checks"):
        service.ingest_paper_check(line)


def armed(permit):
    return DeviceReply(200, {"permit": permit, "status": "armed"})


def make_service(**kwargs):
    clock = kwargs.pop("clock", None) or Clock()
    device = kwargs.pop("device", None) or ScriptedDeviceClient(
        arm_replies=lambda operation, lane, permit: armed(permit)
    )
    store = kwargs.pop("store", None) or Store()
    service = EntryService(store, device, clock=clock, **kwargs)
    return service, store, device, clock


class ReadingTest(unittest.TestCase):
    def test_repeated_readings_narrow_to_one_vehicle(self):
        service, *_ = make_service()
        load_samples(service)
        decision = service.evaluate_reading("W", "p89")
        # 第 1 次读到两辆，第 2 次只读到 13 号。
        self.assertEqual(decision.disposition, "ready")
        self.assertEqual(decision.vin, VIN_A)
        self.assertEqual(decision.reason, "unique_valid_match")

    def test_ambiguous_or_empty_readings_wait_for_review(self):
        service, *_ = make_service(clock=Clock("2026-11-27T01:14:53Z"))
        load_samples(service)
        decision = service.evaluate_reading("W", "p89")
        self.assertEqual(decision.disposition, "manual")
        self.assertEqual(decision.reason, "multiple_candidates")
        self.assertIsNone(decision.vin)

    def test_empty_observation_holds_gate(self):
        service, *_ = make_service()
        service.ingest_station({"device": "west-2", "lane": "W", "port": "P7", "kind": "camera"})
        service.ingest_observation(
            {
                "lane": "W",
                "pass": "p90",
                "sequence": 1,
                "seen_at": "2026-11-27T01:15:00Z",
                "candidates": [],
                "camera": "west-2",
            }
        )
        decision = service.evaluate_reading("W", "p90")
        self.assertEqual(decision.disposition, "hold")
        self.assertEqual(decision.reason, "no_candidate")

    def test_observation_from_wrong_lane_is_rejected(self):
        service, *_ = make_service()
        service.ingest_station({"device": "east-1", "lane": "E", "port": "P7", "kind": "camera"})
        with self.assertRaises(ServiceError) as raised:
            service.ingest_observation(
                {
                    "lane": "W",
                    "pass": "p1",
                    "sequence": 1,
                    "seen_at": "2026-11-27T01:15:00Z",
                    "candidates": [VIN_A],
                    "camera": "east-1",
                }
            )
        self.assertEqual(raised.exception.code, "camera_lane_mismatch")


class PermitTest(unittest.TestCase):
    def test_only_one_lane_can_hold_the_vehicle(self):
        service, store, device, clock = make_service()
        load_samples(service)
        first = service.request_permit("W", "p89")
        self.assertEqual(first["status"], "armed")

        east = service.evaluate_reading("E", "p10")
        self.assertEqual(east.disposition, "wait")
        self.assertEqual(east.reason, "vehicle_permitted_elsewhere")
        with self.assertRaises(ServiceError) as raised:
            service.request_permit("E", "p10")
        self.assertEqual(raised.exception.code, "vehicle_elsewhere")
        self.assertEqual(len(store.open_permits()), 1)
        self.assertEqual(len(device.arms), 1)

    def test_same_pass_is_reused_not_reissued(self):
        service, store, device, clock = make_service()
        load_samples(service)
        first = service.request_permit("W", "p89")
        second = service.request_permit("W", "p89")
        self.assertEqual(first["permit"], second["permit"])
        self.assertEqual(len(device.arms), 1)

    def test_pass_batch_cannot_be_reassigned_after_crossing(self):
        service, store, device, clock = make_service()
        load_samples(service)
        permit = service.request_permit("E", "p10")  # 13 号车占住 E/p10
        clock.set("2026-11-27T01:16:10Z")
        service.receive_event(
            {
                "event_id": "e-cross-10",
                "permit": permit["permit"],
                "lane": "E",
                "pass": "p10",
                "kind": "crossed",
                "occurred_at": "2026-11-27T01:16:05Z",
                "crossing_id": "e-cross-10",
            }
        )
        # 同一地感批次此后读到 27 号，也不能换车接着开闸。
        service.ingest_observation(
            {
                "lane": "E",
                "pass": "p10",
                "sequence": 8,
                "seen_at": "2026-11-27T01:16:30Z",
                "candidates": [VIN_A],
                "camera": "east-1",
            }
        )
        with self.assertRaises(ServiceError) as raised:
            service.request_permit("E", "p10")
        self.assertEqual(raised.exception.code, "not_permitted")
        self.assertEqual(raised.exception.detail, "pass_consumed")

    def test_latest_revision_blocks_new_permit_and_burns_old_paper(self):
        service, store, device, clock = make_service()
        load_samples(service)
        clock.set("2026-11-27T01:16:00Z")  # 修订 2 是 hold
        decision = service.evaluate_reading("W", "p89")
        self.assertEqual(decision.disposition, "deny")
        self.assertEqual(decision.reason, "held")
        with self.assertRaises(ServiceError) as raised:
            service.review("W", "p89", "W-25", actor="shift-A")
        self.assertEqual(raised.exception.code, "review_not_permitted")
        self.assertEqual(raised.exception.detail, "stale_revision")
        with self.assertRaises(ServiceError):
            service.request_permit("W", "p89")

    def test_old_revision_permit_is_stopped_on_new_revision(self):
        service, store, device, clock = make_service()
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:16:00Z")
        device.cancel_replies = [
            DeviceReply(200, {"permit": permit["permit"], "status": "cancelled"})
        ]
        result = service.publish_revision(
            [row for row in read_jsonl("manifests") if row["revision"] == 2]
        )
        self.assertEqual(result["stopped"][0]["status"], "cancelled")
        after = service.permit_status(permit["permit"])
        self.assertEqual(after["status"], "cancelled")
        # 历史依据仍可查。
        self.assertEqual(after["manifest"], "M16")
        self.assertEqual(after["revision"], 1)


class ReviewTest(unittest.TestCase):
    def test_paper_check_resolves_empty_observation_but_not_hold(self):
        service, store, device, clock = make_service()
        load_samples(service)
        service.ingest_observation(
            {
                "lane": "W",
                "pass": "p90",
                "sequence": 1,
                "seen_at": "2026-11-27T01:15:00Z",
                "candidates": [],
                "camera": "west-2",
            }
        )
        service.ingest_paper_check(
            {
                "check": "W-90",
                "lane": "W",
                "pass": "p90",
                "vin": VIN_A,
                "manifest": "M16",
                "revision": 1,
                "officer": "shift-A",
            }
        )
        service.review("W", "p90", "W-90", actor="shift-A")
        permit = service.request_permit("W", "p90", actor="shift-A")
        self.assertEqual(permit["vin"], VIN_A)
        self.assertEqual(permit["status"], "armed")

    def test_review_cannot_override_hold_revision(self):
        service, store, device, clock = make_service()
        load_samples(service)
        service.ingest_observation(
            {
                "lane": "W",
                "pass": "p90",
                "sequence": 1,
                "seen_at": "2026-11-27T01:15:00Z",
                "candidates": [],
                "camera": "west-2",
            }
        )
        service.ingest_paper_check(
            {
                "check": "W-90",
                "lane": "W",
                "pass": "p90",
                "vin": VIN_A,
                "manifest": "M16",
                "revision": 1,
                "officer": "shift-A",
            }
        )
        clock.set("2026-11-27T01:16:00Z")
        with self.assertRaises(ServiceError) as raised:
            service.review("W", "p90", "W-90", actor="shift-A")
        self.assertEqual(raised.exception.code, "review_not_permitted")
        self.assertEqual(raised.exception.detail, "stale_revision")

    def test_review_cannot_override_regulatory_hold(self):
        service, store, device, clock = make_service()
        load_samples(service)
        service.publish_hold(
            "M16", 1, rows=[{"vin": VIN_A}], published_at="2026-11-27T01:14:00Z"
        )
        with self.assertRaises(ServiceError) as raised:
            service.review("W", "p89", "W-25", actor="shift-A")
        self.assertEqual(raised.exception.detail, "regulatory_hold")

    def test_only_shift_officer_can_review(self):
        service, *_ = make_service()
        load_samples(service)
        with self.assertRaises(ServiceError) as raised:
            service.review("W", "p89", "W-25", actor="shift-B")
        self.assertEqual(raised.exception.code, "officer_mismatch")


class ConcurrencyTest(unittest.TestCase):
    def test_two_lanes_racing_produce_one_permit(self):
        from threading import Barrier, Thread

        service, store, device, clock = make_service()
        load_samples(service)
        barrier = Barrier(2)
        results = []

        def attempt(lane, pass_id):
            barrier.wait()
            try:
                results.append(("ok", service.request_permit(lane, pass_id)))
            except ServiceError as error:
                results.append(("error", error.code))

        threads = [
            Thread(target=attempt, args=("W", "p89")),
            Thread(target=attempt, args=("E", "p10")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        outcomes = sorted(result[0] for result in results)
        self.assertEqual(outcomes, ["error", "ok"])
        self.assertEqual(len(store.open_permits()), 1)
        self.assertEqual(len(device.arms), 1)


class CrossingWindowTest(unittest.TestCase):
    def test_crossing_before_permit_is_requested_still_needs_review(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")  # 01:15:10
        service.receive_event(
            {
                "event_id": "early-cross",
                "permit": permit["permit"],
                "lane": "W",
                "pass": "p89",
                "kind": "crossed",
                "occurred_at": "2026-11-27T01:10:00Z",
                "crossing_id": "w-early",
            }
        )
        status = service.permit_status(permit["permit"])
        self.assertEqual(status["inbound"]["handoff"], "pending_review")
        self.assertIn("早于", status["inbound"]["note"])

    def test_crossing_on_wrong_pass_needs_review(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        service.receive_event(
            {
                "event_id": "wrong-pass",
                "permit": permit["permit"],
                "lane": "W",
                "pass": "p90",
                "kind": "crossed",
                "occurred_at": "2026-11-27T01:16:00Z",
                "crossing_id": "w-wrong-pass",
            }
        )
        status = service.permit_status(permit["permit"])
        self.assertEqual(status["inbound"]["handoff"], "pending_review")
        self.assertFalse(status["crossing"]["verified"])


class HoldTimingTest(unittest.TestCase):
    def test_future_hold_is_registered_then_swept_when_effective(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=lambda operation, lane, permit: DeviceReply(
                200, {"permit": permit, "status": "cancelled"}
            ),
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        result = service.publish_hold(
            "M16", 9, rows=[{"vin": VIN_A}], published_at="2026-11-27T01:20:00Z"
        )
        self.assertEqual(result["stopped"], [])
        self.assertEqual(service.permit_status(permit["permit"])["status"], "armed")
        clock.set("2026-11-27T01:20:05Z")
        swept = service.tick()
        self.assertEqual(swept["stopped_for_holds"], [permit["permit"]])
        self.assertEqual(service.permit_status(permit["permit"])["status"], "cancelled")


class DeviceUncertaintyTest(unittest.TestCase):
    def test_silent_device_stays_pending_and_blocks_second_lane(self):
        device = ScriptedDeviceClient(
            arm_replies=[UnknownOutcome("arm", "连接断开"), armed("x")]
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        self.assertEqual(permit["status"], "pending")
        self.assertTrue(permit["device_query_required"])
        self.assertEqual(permit["uncertainty"][0]["kind"], "arm_unknown")

        with self.assertRaises(ServiceError) as raised:
            service.request_permit("E", "p10")
        self.assertEqual(raised.exception.code, "vehicle_elsewhere")
        # 设备没回话，不允许改道重发。
        with self.assertRaises(ServiceError):
            service.switch_lane(permit["permit"], "E", "p10")
        self.assertEqual(len(device.arms), 1)

    def test_query_recovers_device_state(self):
        device = ScriptedDeviceClient(
            arm_replies=[UnknownOutcome("arm", "连接断开")],
            query_replies=[
                lambda operation, lane, permit: DeviceReply(
                    200, {"permit": permit, "status": "armed"}
                )
            ],
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        recovered = service.reconcile_permit(permit["permit"])
        self.assertEqual(recovered["status"], "armed")
        self.assertEqual(recovered["uncertainty"][0]["kind"], "arm_unknown")

    def test_cancel_needs_device_proof(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=[UnknownOutcome("cancel", "无应答")],
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        cancelled = service.request_cancel(permit["permit"])
        self.assertEqual(cancelled["status"], "cancel_requested")
        self.assertEqual(cancelled["uncertainty"][0]["kind"], "cancel_unknown")
        with self.assertRaises(ServiceError):
            service.switch_lane(permit["permit"], "E", "p10")

    def test_mismatched_reply_is_not_treated_as_armed(self):
        device = ScriptedDeviceClient(
            arm_replies=[DeviceReply(200, {"permit": "someone-elses", "status": "armed"})]
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        self.assertEqual(permit["status"], "pending")
        self.assertEqual(permit["uncertainty"][-1]["kind"], "reply_mismatch")

    def test_tick_expires_unused_armed_permit(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        service, store, device, clock = make_service(device=device, permit_margin=timedelta(0))
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T03:30:00Z")
        result = service.tick()
        self.assertEqual(result["expired"], [permit["permit"]])
        self.assertEqual(service.permit_status(permit["permit"])["status"], "expired")

    def test_crossed_during_cancel_is_kept_and_handed_off(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=[
                lambda operation, lane, permit: DeviceReply(
                    200,
                    {
                        "permit": permit,
                        "status": "crossed",
                        "crossed_at": "2026-11-27T01:16:01Z",
                        "lane": "W",
                        "pass": "p89",
                        "crossing_id": "w-cross-89",
                    },
                )
            ],
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:16:05Z")
        result = service.request_cancel(permit["permit"], reason="regulatory")
        self.assertEqual(result["status"], "crossed")
        self.assertEqual(result["inbound"]["handoff"], "inbound")
        self.assertEqual(result["crossing"]["crossing_id"], "w-cross-89")


class SwitchLaneTest(unittest.TestCase):
    def test_switch_allowed_after_device_cancel(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=lambda operation, lane, permit: DeviceReply(
                200, {"permit": permit, "status": "cancelled"}
            ),
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:15:30Z")
        # 27 号在 E/p11 有读数，但改道的是 13 号；不能把别人安排过来。
        with self.assertRaises(ServiceError) as raised:
            service.switch_lane(permit["permit"], "E", "p11")
        self.assertEqual(raised.exception.code, "vin_mismatch")
        # E/p10 是 13 号。
        new_permit = service.switch_lane(permit["permit"], "E", "p10")
        self.assertEqual(new_permit["lane"], "E")
        self.assertEqual(new_permit["vin"], VIN_A)
        old = store.permit(permit["permit"])
        self.assertEqual(old["status"], "cancelled")
        self.assertEqual(old["superseded_by"], new_permit["permit"])
        self.assertEqual(len(store.open_permits()), 1)


class EventTest(unittest.TestCase):
    def test_crossing_receipt_arrives_before_armed_and_handoff_once(self):
        device = ScriptedDeviceClient(
            arm_replies=[UnknownOutcome("arm", "连接断开")],
            query_replies=[UnknownOutcome("query", "仍断开")],
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:17:00Z")
        crossing_event = {
            "event_id": "e4",
            "permit": permit["permit"],
            "lane": "W",
            "pass": "p89",
            "kind": "crossed",
            "occurred_at": "2026-11-27T01:16:01Z",
            "crossing_id": "w-cross-89",
        }
        armed_event = {
            "event_id": "e3",
            "permit": permit["permit"],
            "lane": "W",
            "pass": "p89",
            "kind": "armed",
            "occurred_at": "2026-11-27T01:15:04Z",
        }
        first = service.receive_event(crossing_event)
        self.assertEqual(first["handoff"], "inbound")
        again = service.receive_event(crossing_event)
        self.assertTrue(again["duplicate"])
        service.receive_event(armed_event)
        status = service.permit_status(permit["permit"])
        self.assertEqual(status["status"], "crossed")
        self.assertEqual(status["inbound"]["handoff"], "inbound")
        # 两条回执都已处理；越线只有一次交接。
        self.assertEqual(len(store.events(applied=True)), 2)
        self.assertEqual(len(store.events(applied=False)), 0)

    def test_crossing_after_revision_change_is_left_for_site_review(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:17:00Z")
        service.receive_event(
            {
                "event_id": "late-cross",
                "permit": permit["permit"],
                "lane": "W",
                "pass": "p89",
                "kind": "crossed",
                "occurred_at": "2026-11-27T01:16:01Z",
                "crossing_id": "w-cross-89",
            }
        )
        status = service.permit_status(permit["permit"])
        # 越线实际发生过，仍保留这次进区，但交现场核查。
        self.assertEqual(status["status"], "crossed")
        self.assertEqual(status["resolution"], "crossing_verified")
        self.assertIn("名单更正", status["inbound"]["note"])

    def test_late_crossing_past_expiry_needs_review(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        service, store, device, clock = make_service(device=device, permit_margin=timedelta(0))
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T05:00:00Z")
        service.receive_event(
            {
                "event_id": "too-late",
                "permit": permit["permit"],
                "lane": "W",
                "pass": "p89",
                "kind": "crossed",
                "occurred_at": "2026-11-27T04:00:01Z",
                "crossing_id": "w-cross-late",
            }
        )
        status = service.permit_status(permit["permit"])
        self.assertEqual(status["status"], "crossed")
        self.assertFalse(status["crossing"]["verified"])
        self.assertEqual(status["inbound"]["handoff"], "pending_review")

    def test_unknown_crossing_goes_to_site_review(self):
        service, store, device, clock = make_service()
        load_samples(service)
        result = service.receive_event(
            {
                "event_id": "orphan",
                "permit": "perm-unknown",
                "lane": "W",
                "pass": "p89",
                "kind": "crossed",
                "occurred_at": "2026-11-27T01:16:01Z",
                "crossing_id": "w-orphan",
            }
        )
        self.assertFalse(result["matched"])
        self.assertEqual(result["handoff"], "pending_review")
        self.assertEqual(store.crossing("w-orphan")["handoff"], "pending_review")


class RegulationTest(unittest.TestCase):
    def test_hold_stops_unused_permit(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=lambda operation, lane, permit: DeviceReply(
                200, {"permit": permit, "status": "cancelled"}
            ),
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        clock.set("2026-11-27T01:16:35Z")
        result = service.publish_hold(
            "M16",
            3,
            rows=[{"vin": VIN_A}],
            published_at="2026-11-27T01:16:30Z",
        )
        self.assertEqual(result["stopped"][0]["status"], "cancelled")
        clock.set("2026-11-27T01:17:00Z")
        decision = service.evaluate_reading("W", "p89")
        self.assertEqual(decision.reason, "regulatory_hold")

    def test_hold_by_vin_stops_permit_under_another_manifest(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            cancel_replies=lambda operation, lane, permit: DeviceReply(
                200, {"permit": permit, "status": "cancelled"}
            ),
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")  # 依据 M16
        clock.set("2026-11-27T01:15:40Z")
        result = service.publish_hold(
            "M17", 5, rows=[{"vin": VIN_A}], published_at="2026-11-27T01:15:30Z"
        )
        self.assertEqual(result["stopped"][0]["status"], "cancelled")
        self.assertEqual(service.permit_status(permit["permit"])["status"], "cancelled")

    def test_hold_seen_by_crossing_is_still_handed_off(self):
        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit),
            query_replies=[
                lambda operation, lane, permit: DeviceReply(
                    200,
                    {
                        "permit": permit,
                        "status": "crossed",
                        "crossed_at": "2026-11-27T01:16:01Z",
                        "lane": "W",
                        "pass": "p89",
                        "crossing_id": "w-cross-89",
                    },
                )
            ],
        )
        service, store, device, clock = make_service(device=device)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        service.publish_hold(
            "M16", 3, rows=[{"vin": VIN_A}], published_at="2026-11-27T01:15:30Z"
        )
        clock.set("2026-11-27T01:17:00Z")
        status = service.reconcile_permit(permit["permit"])
        self.assertEqual(status["status"], "crossed")
        self.assertEqual(status["inbound"]["handoff"], "inbound")
        self.assertIn("监管暂停", status["inbound"]["note"])


class RestartTest(unittest.TestCase):
    def test_pending_permit_survives_restart(self):
        import tempfile
        from pathlib import Path

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "entry.sqlite3"
        clock = Clock()
        device = ScriptedDeviceClient(
            arm_replies=[UnknownOutcome("arm", "连接断开")],
            query_replies=[
                lambda operation, lane, permit: DeviceReply(
                    200, {"permit": permit, "status": "armed"}
                )
            ],
        )
        store = Store(path)
        service = EntryService(store, device, clock=clock)
        load_samples(service)
        permit = service.request_permit("W", "p89")
        store.close()

        reopened = Store(path)
        self.addCleanup(reopened.close)
        clock.set("2026-11-27T01:19:00Z")
        restarted = EntryService(reopened, device, clock=clock)
        waiting = restarted.waiting()
        self.assertEqual(waiting["awaiting_device"][0]["permit"], permit["permit"])
        self.assertEqual(waiting["lanes"]["W"][0]["status"], "pending")
        recovered = restarted.reconcile_permit(permit["permit"])
        self.assertEqual(recovered["status"], "armed")

    def test_open_permit_unique_index_survives_process_boundary(self):
        import tempfile
        from pathlib import Path

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "entry.sqlite3"
        first = Store(path)
        second = Store(path)
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        record = {
            "permit_id": "perm-1",
            "vehicle": VIN_A,
            "lane": "W",
            "pass": "p89",
            "vin": VIN_A,
            "port": "P7",
            "manifest": "M16",
            "revision": 1,
            "armed_at": "2026-11-27T01:15:10Z",
            "expires_at": "2026-11-27T03:02:00Z",
            "status": "armed",
            "resolution": "armed_confirmed",
            "uncertainty": [],
            "audit": [],
            "inbound": "none",
            "updated_at": "2026-11-27T01:15:10Z",
        }
        with first.write() as connection:
            first.insert_permit(record)
        duplicate = dict(record, permit_id="perm-2", lane="E", **{"pass": "p10"})
        import sqlite3

        with self.assertRaises(sqlite3.IntegrityError):
            with second.write() as connection:
                second.insert_permit(duplicate)


class HttpTest(unittest.TestCase):
    def setUp(self):
        from threading import Thread

        device = ScriptedDeviceClient(
            arm_replies=lambda operation, lane, permit: armed(permit)
        )
        self.server = create_app(
            device=device, tick_interval=None, clock=Clock("2026-11-27T01:15:10Z")
        )
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)
        self.port = self.server.server_port

    def _shutdown(self):
        self.server.stop()
        self.thread.join()
        self.server.server_close()

    def call(self, method, path, body=None):
        import http.client

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            payload = json.dumps(body).encode() if body is not None else None
            connection.request(method, path, body=payload)
            response = connection.getresponse()
            return response.status, json.load(response)
        finally:
            connection.close()

    def test_http_round_trip(self):
        for row in read_jsonl("stations"):
            self.assertEqual(self.call("POST", "/stations", row)[0], 200)
        for row in read_jsonl("manifests"):
            self.assertEqual(self.call("POST", "/manifests", row)[0], 200)
        for row in read_jsonl("observations"):
            self.assertEqual(self.call("POST", "/observations", row)[0], 200)
        status, reading = self.call("GET", "/lanes/W/passes/p89")
        self.assertEqual(status, 200)
        self.assertEqual(reading["vin"], VIN_A)
        status, permit = self.call("POST", "/lanes/W/passes/p89/permits", {})
        self.assertEqual(status, 200, permit)
        self.assertEqual(permit["lane"], "W")
        status, error = self.call("POST", "/lanes/E/passes/p10/permits", {})
        self.assertEqual(status, 409)
        self.assertEqual(error["error"], "vehicle_elsewhere")
        status, waiting = self.call("GET", "/waiting")
        self.assertEqual(status, 200)
        self.assertEqual(waiting["lanes"]["W"][0]["permit"], permit["permit"])
        status, unknown = self.call("GET", "/permits/nope")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
