"""名单与准入判定。

清单按清单号和修订发来，行动为 admit 或 hold，时间区间左闭右开。对某一辆车
来说，同一清单下**包含该车的最新修订**才有效；生产方给该 VIN 出新修订后，
旧修订下的纸单和许可都不再能成立，但历史依据仍留在审计里。

监管暂停独立于清单修订，优先级更高。所有判定只看 ``at`` 时已经发出的记录，
回放和实时处理因此使用同一口径。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .feeds import parse_time
from .store import Store


@dataclass(frozen=True)
class Entry:
    manifest: str
    revision: int
    vin: str
    port: str
    from_at: datetime
    until_at: datetime
    action: str
    published_at: datetime


@dataclass(frozen=True)
class Eligibility:
    eligible: bool
    reason: str
    manifest: str | None = None
    revision: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "reason": self.reason,
            "manifest": self.manifest,
            "revision": self.revision,
        }


class ManifestBook:
    """某一时刻已发出的名单，按清单 / 修订 / VIN 建索引。"""

    def __init__(self, entries: list[Entry]) -> None:
        self._by_manifest: dict[str, dict[int, dict[str, Entry]]] = {}
        for entry in entries:
            self._by_manifest.setdefault(entry.manifest, {}).setdefault(entry.revision, {})[
                entry.vin
            ] = entry

    @classmethod
    def from_store(cls, store: Store, upto: datetime | None = None) -> "ManifestBook":
        entries = []
        for row in store.manifest_rows():
            published = parse_time(row["published_at"])
            if upto is not None and published > upto:
                continue
            entries.append(
                Entry(
                    manifest=row["manifest"],
                    revision=row["revision"],
                    vin=row["vin"],
                    port=row["port"],
                    from_at=parse_time(row["from_at"]),
                    until_at=parse_time(row["until_at"]),
                    action=row["action"],
                    published_at=published,
                )
            )
        return cls(entries)

    def latest_revision_for_vin(self, manifest: str, vin: str) -> int | None:
        revisions = [
            revision
            for revision, rows in self._by_manifest.get(manifest, {}).items()
            if vin in rows
        ]
        return max(revisions) if revisions else None

    def entry(self, manifest: str, revision: int, vin: str) -> Entry | None:
        return self._by_manifest.get(manifest, {}).get(revision, {}).get(vin)

    def is_latest_for_vin(self, manifest: str, revision: int, vin: str) -> bool:
        return self.latest_revision_for_vin(manifest, vin) == revision

    def latest_entries_for_vin(self, vin: str) -> list[Entry]:
        """该 VIN 参与的每份清单里，包含它的最新修订。"""
        result: list[Entry] = []
        for manifest in self._by_manifest:
            revision = self.latest_revision_for_vin(manifest, vin)
            if revision is not None:
                result.append(self._by_manifest[manifest][revision][vin])
        return result

    def manifests_for_vin(self, vin: str) -> set[str]:
        return {
            manifest
            for manifest, revisions in self._by_manifest.items()
            if any(vin in rows for rows in revisions.values())
        }


def evaluate(book: ManifestBook, vin: str, at: datetime, port: str | None) -> Eligibility:
    """判断该车此刻能否凭最新修订入场。

    候选取 ``from_at`` 最晚的一档（更具体的时段优先）；多条同时成立或行动不是
    admit 都不能自动放行。
    """
    candidates = [
        entry
        for entry in book.latest_entries_for_vin(vin)
        if entry.from_at <= at < entry.until_at and (port is None or entry.port == port)
    ]
    if not candidates:
        return Eligibility(False, "out_of_window")
    newest = max(entry.from_at for entry in candidates)
    current = [entry for entry in candidates if entry.from_at == newest]
    if len(current) > 1:
        return Eligibility(False, "conflicting_entries", current[0].manifest, current[0].revision)
    entry = current[0]
    if entry.action != "admit":
        return Eligibility(False, "held", entry.manifest, entry.revision)
    return Eligibility(True, "admit", entry.manifest, entry.revision)


def paper_check_usable(
    book: ManifestBook, check: dict[str, Any], at: datetime
) -> Eligibility:
    """纸单复核只能在其依据的修订仍是该车最新修订时支持一次观察。

    复核无权延长时段、覆盖 hold，也无权在监管暂停下继续放行。
    """
    manifest = check["manifest"]
    revision = check["revision"]
    vin = check["vin"]
    if not book.is_latest_for_vin(manifest, revision, vin):
        return Eligibility(False, "stale_revision", manifest, revision)
    entry = book.entry(manifest, revision, vin)
    if entry is None or not entry.from_at <= at < entry.until_at:
        return Eligibility(False, "out_of_window", manifest, revision)
    if entry.action != "admit":
        return Eligibility(False, "held", manifest, revision)
    return Eligibility(True, "admit", manifest, revision)


def suspended(store: Store, vin: str, at: datetime) -> dict[str, Any] | None:
    """已收到（published_at <= at）且涉及该车的监管暂停。"""
    return store.hold_effective_at(vin, at)
