"""双车道凭车架号进区与闸机不确定交接的进区服务。

面向两条进区车道的持续运行服务：名单按修订生效，观察读数在唯一匹配时才
可申请开闸，许可的设备不确定状态、监管暂停、纸单复核和越线交接都围绕同一
份持久化状态工作。

入口见 :func:`entry_service.app.create_app`（HTTP）与
:class:`entry_service.service.EntryService`（直接调用）。
"""

from .app import Api, ServiceServer, create_app
from .device import DeviceReply, HttpDeviceClient, ScriptedDeviceClient, UnknownOutcome
from .feeds import FeedFormatError, FeedIndex, Format, load_directory, parse_records
from .service import EntryService, ServiceError
from .store import Store

__all__ = [
    "Api",
    "ServiceServer",
    "create_app",
    "DeviceReply",
    "HttpDeviceClient",
    "ScriptedDeviceClient",
    "UnknownOutcome",
    "EntryService",
    "ServiceError",
    "Store",
    "FeedFormatError",
    "FeedIndex",
    "Format",
    "load_directory",
    "parse_records",
]
