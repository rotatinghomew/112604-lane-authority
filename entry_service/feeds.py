"""名单清单、观测读数、纸单复核与设备归属的读取与校验。

封锁了 `samples/formats.json` 中的字段、记录键和引用。记录直接交给进区服务
处理，顺序以文件交付顺序为准，业务发生时间看各记录的时间字段。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable


class FeedFormatError(ValueError):
    """交换资料不符合约定。"""


_TYPES: dict[str, type] = {
    "string": str,
    "int": int,
    "bool": bool,
    "list": list,
    "time": str,
}


def parse_time(value: str) -> datetime:
    """解析带 Z 的 UTC ISO 8601 时间。"""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise FeedFormatError(f"时间须为带 Z 的 UTC ISO 8601：{value!r}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise FeedFormatError(f"无法解析时间：{value!r}") from error
    if parsed.tzinfo != timezone.utc:
        raise FeedFormatError(f"时间须为 UTC：{value!r}")
    return parsed


def format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _select(record: dict[str, Any], fields: Iterable[str]) -> tuple:
    return tuple(record[field] for field in fields)


@dataclass(frozen=True)
class Format:
    """一类交换资料的字段与记录键。"""

    name: str
    key: tuple[str, ...]
    fields: dict[str, str]

    @classmethod
    def from_spec(cls, name: str, spec: dict[str, Any]) -> "Format":
        return cls(name, tuple(spec["key"]), dict(spec["fields"]))

    def validate(self, record: Any, index: int) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise FeedFormatError(f"{self.name}[{index}] 不是对象")
        if tuple(record) != tuple(self.fields):
            raise FeedFormatError(
                f"{self.name}[{index}] 字段不符：期望 {tuple(self.fields)}，实得 {tuple(record)}"
            )
        for field, kind in self.fields.items():
            value = record[field]
            if type(value) is not _TYPES[kind]:
                raise FeedFormatError(
                    f"{self.name}[{index}].{field} 类型不符：期望 {kind}，实得 {type(value).__name__}"
                )
            if kind == "time":
                parse_time(value)
        key = _select(record, self.key)
        if any(value in ("", None) for value in key):
            raise FeedFormatError(f"{self.name}[{index}] 记录键有空值：{key}")
        return record


class FeedIndex:
    """已校验资料的只读集合，保留交付顺序。"""

    def __init__(self, feeds: dict[str, list[dict[str, Any]]]) -> None:
        self.feeds = feeds

    @classmethod
    def build(cls, schema: dict[str, Any], feeds: dict[str, list[Any]]) -> "FeedIndex":
        declared = {name: Format.from_spec(name, spec) for name, spec in schema["feeds"].items()}
        if set(declared) != set(feeds):
            missing = set(declared) - set(feeds)
            extra = set(feeds) - set(declared)
            raise FeedFormatError(f"资料集合不符：缺少 {sorted(missing)}，多出 {sorted(extra)}")
        cleaned: dict[str, list[dict[str, Any]]] = {}
        for name, format_ in declared.items():
            rows = [format_.validate(record, index) for index, record in enumerate(feeds[name])]
            keys = [_select(row, format_.key) for row in rows]
            duplicates = [key for key in keys if keys.count(key) > 1]
            if duplicates:
                raise FeedFormatError(f"{name} 记录键重复：{sorted(set(duplicates))}")
            cleaned[name] = rows
        for source, keys, target, attributes in schema.get("references", []):
            expected = {_select(row, attributes) for row in cleaned[target]}
            for row in cleaned[source]:
                if _select(row, keys) not in expected:
                    raise FeedFormatError(
                        f"{source} {_select(row, keys)} 引用了不存在的 {target} {attributes}"
                    )
        return cls(cleaned)

    def __getitem__(self, name: str) -> list[dict[str, Any]]:
        return self.feeds[name]


def parse_records(line: str, format_: Format, index: int) -> dict[str, Any]:
    try:
        record = json.loads(line)
    except ValueError as error:
        raise FeedFormatError(f"{format_.name}[{index}] 不是有效 JSON") from error
    return format_.validate(record, index)


def load_directory(path: str | Path) -> FeedIndex:
    """读取目录内的 formats.json 与同名 JSONL 资料文件。"""
    root = Path(path)
    schema = json.loads((root / "formats.json").read_text(encoding="utf-8"))
    declared = {name: Format.from_spec(name, spec) for name, spec in schema["feeds"].items()}
    feeds: dict[str, list[dict[str, Any]]] = {}
    for name, format_ in declared.items():
        lines = [
            line
            for line in (root / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        feeds[name] = [parse_records(line, format_, index) for index, line in enumerate(lines)]
    return FeedIndex.build(schema, feeds)
