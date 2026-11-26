"""车道闸机的许可接口客户端。

设备一旦记下许可执行就不能收回，所以这里把所有“没收到确定回话”的请求
都作为设备不确定记录下来，交给服务决定后续（只能回查，不能换车道重发）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import socket
from typing import Any
import http.client
import urllib.error
import urllib.request


class UnknownOutcome(Exception):
    """请求可能已到达设备，但没有可用的回话。"""

    def __init__(self, operation: str, detail: str) -> None:
        super().__init__(detail)
        self.operation = operation
        self.detail = detail


@dataclass(frozen=True)
class DeviceReply:
    status: int
    body: dict[str, Any]


@dataclass
class HttpDeviceClient:
    base_url: str
    timeout: float = 5.0
    requests: list[dict[str, Any]] = field(default_factory=list)

    def _request(self, method: str, path: str, payload: Any = None) -> DeviceReply:
        url = self.base_url.rstrip("/") + path
        data = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"} if data is not None else {}
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        self.requests.append({"method": method, "path": path, "payload": payload})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
                return DeviceReply(response.status, json.loads(body) if body else {})
        except urllib.error.HTTPError as error:
            body = error.read()
            try:
                parsed = json.loads(body) if body else {}
            except ValueError:
                parsed = {}
            return DeviceReply(error.code, parsed)
        except (urllib.error.URLError, http.client.HTTPException, socket.timeout, ConnectionError, OSError) as error:
            raise UnknownOutcome(f"{method} {path}", str(error)) from error

    def arm(self, lane: str, permit: dict[str, Any]) -> DeviceReply:
        return self._request("POST", f"/lanes/{lane}/permits", {
            "permit": permit["permit_id"],
            "pass": permit["pass"],
            "vin": permit["vin"],
            "expires_at": permit["expires_at"],
        })

    def query(self, lane: str, permit_id: str) -> DeviceReply:
        return self._request("GET", f"/lanes/{lane}/permits/{permit_id}")

    def cancel(self, lane: str, permit_id: str) -> DeviceReply:
        return self._request("POST", f"/lanes/{lane}/permits/{permit_id}/cancel", {})


@dataclass
class ScriptedDeviceClient:
    """测试用的可编排设备，按车道路径返回回话或抛出 UnknownOutcome。"""

    arm_replies: Any = None
    query_replies: Any = None
    cancel_replies: Any = None
    arms: list[dict[str, Any]] = field(default_factory=list)
    queries: list[dict[str, Any]] = field(default_factory=list)
    cancels: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def _resolve(reply: Any, operation: str, lane: str, permit_id: str) -> DeviceReply:
        if callable(reply):
            reply = reply(operation, lane, permit_id)
        if isinstance(reply, Exception):
            raise reply
        if reply is None:
            raise UnknownOutcome(operation, "没有录制回话")
        return reply

    def _take(self, script: Any, operation: str, lane: str, permit_id: str) -> DeviceReply:
        if callable(script):
            return self._resolve(script, operation, lane, permit_id)
        if isinstance(script, dict):
            return self._resolve(script.get((lane, permit_id)), operation, lane, permit_id)
        if isinstance(script, list):
            if not script:
                raise UnknownOutcome(operation, "没有录制回话")
            return self._resolve(script.pop(0), operation, lane, permit_id)
        raise UnknownOutcome(operation, "没有录制回话")

    def arm(self, lane: str, permit: dict[str, Any]) -> DeviceReply:
        self.arms.append({"lane": lane, "permit": permit["permit_id"], "pass": permit["pass"]})
        return self._take(self.arm_replies, "arm", lane, permit["permit_id"])

    def query(self, lane: str, permit_id: str) -> DeviceReply:
        self.queries.append({"lane": lane, "permit": permit_id})
        return self._take(self.query_replies, "query", lane, permit_id)

    def cancel(self, lane: str, permit_id: str) -> DeviceReply:
        self.cancels.append({"lane": lane, "permit": permit_id})
        return self._take(self.cancel_replies, "cancel", lane, permit_id)
