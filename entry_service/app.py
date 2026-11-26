"""进区服务的 HTTP 入口。

进区服务是唯一持有名单、许可和交接状态的地方；车道设备的状态只通过
“回查”和“事件回执”进入，不直接改名单。

============================  ==========================================
``POST /manifests``            登记一条名单记录；列表体则整批按新修订处理
``POST /holds``                监管暂停 / 名单更正，停发并停用未使用许可
``POST /observations``         摄像机读数（lane/pass/sequence 去重）
``POST /paper_checks``         纸单登记
``POST /stations``             摄像机、闸机归属
``GET  /lanes/<lane>/passes/<pass>``           该次观察的结论与处置
``POST /lanes/<lane>/passes/<pass>/permits``   申请开闸
``POST /lanes/<lane>/passes/<pass>/review``    值班复核
``GET  /permits/<id>``         许可详情（权限、实际、去向分开解释）
``POST /permits/<id>/cancel``  撤回
``POST /permits/<id>/reconcile`` 向同一车道设备回查
``POST /permits/<id>/switch``  尚未开闸的车辆改走另一车道
``POST /events``               设备事件回执（可乱序、可重复）
``GET  /waiting``              值班查询：占用哪条车道、为什么等、是否已交场内
``GET  /lanes/<lane>``         单车道的未结束许可
``GET  /audit``                审计记录
``POST /tick``                 手动推进一轮后台任务
============================  ==========================================
"""

from __future__ import annotations

from datetime import datetime
import json
import re
import threading
from typing import Any, Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .device import HttpDeviceClient
from .feeds import FeedFormatError
from .service import EntryService, ServiceError
from .store import Store, format_time, utcnow

ERROR_STATUS = {
    "unknown_permit": 404,
    "unknown_check": 404,
    "unknown_pass": 404,
    "unknown_camera": 404,
    "not_permitted": 409,
    "vehicle_elsewhere": 409,
    "cancel_not_proven": 409,
    "pass_consumed": 409,
    "pass_in_use": 409,
    "permit_closed": 409,
    "review_not_permitted": 403,
    "officer_mismatch": 403,
    "vin_mismatch": 409,
    "check_vin_not_observed": 409,
    "pass_not_yet_seen": 409,
    "camera_lane_mismatch": 400,
    "check_mismatch": 400,
    "same_lane": 400,
    "not_a_camera": 400,
}


class Api:
    """把 HTTP 请求映射到 :class:`EntryService`。"""

    def __init__(self, service: EntryService) -> None:
        self.service = service
        self.routes: list[tuple[str, re.Pattern[str], Callable[..., Any]]] = []
        self._register()

    def route(self, method: str, pattern: str):
        def register(function):
            self.routes.append((method, re.compile("^" + pattern + "$"), function))
            return function

        return register

    def _register(self) -> None:
        service = self.service

        @self.route("POST", r"/manifests")
        def manifests(params, body):
            if isinstance(body, list):
                return 200, service.publish_revision(body)
            return 200, service.ingest_manifest(body)

        @self.route("POST", r"/holds")
        def holds(params, body):
            return 200, service.publish_hold(
                body["manifest"],
                body["revision"],
                rows=body["rows"],
                published_at=body["published_at"],
            )

        @self.route("POST", r"/observations")
        def observations(params, body):
            return 200, service.ingest_observation(body)

        @self.route("POST", r"/paper_checks")
        def paper_checks(params, body):
            return 200, service.ingest_paper_check(body)

        @self.route("POST", r"/stations")
        def stations(params, body):
            return 200, service.ingest_station(body)

        @self.route("GET", r"/lanes/(?P<lane>[^/]+)/passes/(?P<pass_id>[^/]+)")
        def reading(params, body):
            return 200, service.evaluate_reading(params["lane"], params["pass_id"]).as_dict()

        @self.route("POST", r"/lanes/(?P<lane>[^/]+)/passes/(?P<pass_id>[^/]+)/permits")
        def permits(params, body):
            body = body or {}
            return 200, service.request_permit(
                params["lane"], params["pass_id"], vin=body.get("vin")
            )

        @self.route("POST", r"/lanes/(?P<lane>[^/]+)/passes/(?P<pass_id>[^/]+)/review")
        def review(params, body):
            body = body or {}
            return 200, service.review(
                params["lane"], params["pass_id"], body["check"], actor=body.get("officer")
            )

        @self.route("GET", r"/permits")
        def permit_list(params, body):
            return 200, {"permits": service.list_permits()}

        @self.route("GET", r"/permits/(?P<permit_id>[^/]+)")
        def permit(params, body):
            return 200, service.permit_status(params["permit_id"])

        @self.route("POST", r"/permits/(?P<permit_id>[^/]+)/cancel")
        def cancel(params, body):
            body = body or {}
            return 200, service.request_cancel(
                params["permit_id"], reason=body.get("reason", "manual")
            )

        @self.route("POST", r"/permits/(?P<permit_id>[^/]+)/reconcile")
        def reconcile(params, body):
            return 200, service.reconcile_permit(params["permit_id"])

        @self.route("POST", r"/permits/(?P<permit_id>[^/]+)/switch")
        def switch(params, body):
            return 200, service.switch_lane(
                params["permit_id"], body["lane"], body["pass"]
            )

        @self.route("POST", r"/events")
        def events(params, body):
            return 200, service.receive_event(body)

        @self.route("GET", r"/waiting")
        def waiting(params, body):
            return 200, service.waiting()

        @self.route("GET", r"/lanes/(?P<lane>[^/]+)")
        def lane(params, body):
            return 200, service.lane_status(params["lane"])

        @self.route("GET", r"/audit")
        def audit(params, body):
            return 200, {"audit": service.store.audit_log()}

        @self.route("GET", r"/health")
        def health(params, body):
            return 200, {"ok": True, "at": format_time(service.now())}

        @self.route("POST", r"/tick")
        def tick(params, body):
            return 200, service.tick()

    def dispatch(self, method: str, path: str, body: Any) -> tuple[int, Any]:
        path = path.split("?", 1)[0].rstrip("/") or "/"
        path_allowed = False
        for route_method, pattern, function in self.routes:
            match = pattern.match(path)
            if not match:
                continue
            if route_method != method:
                path_allowed = True
                continue
            try:
                return function(match.groupdict(), body)
            except ServiceError as error:
                return ERROR_STATUS.get(error.code, 409), {
                    "error": error.code,
                    "detail": error.detail,
                    **error.extra,
                }
            except (KeyError, TypeError, ValueError, FeedFormatError) as error:
                return 400, {"error": "bad_request", "detail": str(error)}
        if path_allowed:
            return 405, {"error": "method_not_allowed"}
        return 404, {"error": "not_found"}


class Handler(BaseHTTPRequestHandler):
    server_version = "EntryService/1.0"

    def _reply(self, status: int, payload: Any) -> None:
        content = json.dumps(payload, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _handle(self) -> None:
        try:
            size = int(self.headers.get("Content-Length", 0))
        except ValueError:
            self._reply(400, {"error": "bad_request", "detail": "Content-Length 无效"})
            return
        if size > 1_048_576:
            self._reply(413, {"error": "payload_too_large"})
            return
        try:
            body = json.loads(self.rfile.read(size)) if size else None
        except ValueError:
            self._reply(400, {"error": "bad_request", "detail": "请求体不是有效 JSON"})
            return
        try:
            status, payload = self.server.api.dispatch(self.command, self.path, body)
        except Exception:
            # 单个请求的内部错误不能打掉持续运行的服务。
            status, payload = 500, {"error": "internal", "detail": "服务内部错误"}
        self._reply(status, payload)

    do_GET = _handle
    do_POST = _handle

    def log_message(self, fmt, *args):
        return


class ServiceServer(ThreadingHTTPServer):
    """带后台推进线程的 HTTP 服务。"""

    daemon_threads = True

    def __init__(self, address, api: Api, tick_interval: float | None = 2.0) -> None:
        super().__init__(address, Handler)
        self.api = api
        self.tick_interval = tick_interval
        self.service = api.service
        self._stopped = False
        self._ticker_thread = None
        self._stop_event = threading.Event()
        if tick_interval is not None:
            def loop():
                while not self._stopped:
                    self._stop_event.wait(tick_interval)
                    if self._stopped:
                        return
                    try:
                        self.service.tick()
                    except Exception:
                        # 后台推进出错不终止服务，下一轮继续。
                        continue

            self._ticker_thread = threading.Thread(target=loop, daemon=True)
            self._ticker_thread.start()

    def stop(self) -> None:
        self._stopped = True
        if self.tick_interval is not None:
            self._stop_event.set()
        self.shutdown()


def create_app(
    *,
    store: Store | None = None,
    device: Any = None,
    device_url: str | None = None,
    tick_interval: float | None = 2.0,
    clock: Callable[[], datetime] = utcnow,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ServiceServer:
    """装配一个可持续运行的进区服务。

    ``device`` 可直接注入（测试或同进程设备）；否则按 ``device_url`` 使用 HTTP
    客户端。两者都不给时指向本机的对端重放服务。
    """
    store = store or Store()
    if device is None:
        device = HttpDeviceClient(device_url or "http://127.0.0.1:8087")
    service = EntryService(store, device, clock=clock)
    server = ServiceServer((host, port), Api(service), tick_interval=tick_interval)
    return server


__all__ = ["Api", "ServiceServer", "create_app", "ERROR_STATUS"]
