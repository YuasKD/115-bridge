"""内存版 Arr，实现 ``ArrClient``，仅供测试。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bridge.arr.protocol import ArrAuthError, ArrUnreachable, QueueItemNotFound


@dataclass(frozen=True, slots=True)
class ArrCall:
    method: str
    args: tuple[Any, ...]


@dataclass(slots=True)
class FakeArr:
    name: str = "radarr"
    unreachable: bool = False
    auth_error: bool = False
    queue: dict[str, int] = field(default_factory=dict)  # downloadId（大写）到队列项 id
    calls: list[ArrCall] = field(default_factory=list)
    removed: list[int] = field(default_factory=list)  # remove_and_blocklist 成功的队列项 id

    def add_queue_item(self, info_hash: str, queue_id: int) -> None:
        self.queue[info_hash.upper()] = queue_id

    def calls_to(self, method: str) -> list[ArrCall]:
        return [c for c in self.calls if c.method == method]

    def _check(self) -> None:
        if self.unreachable:
            raise ArrUnreachable(f"{self.name} unreachable")
        if self.auth_error:
            raise ArrAuthError(f"{self.name} returned 401")

    async def find_queue_item(self, info_hash: str) -> int:
        self.calls.append(ArrCall("find_queue_item", (info_hash,)))
        self._check()
        try:
            return self.queue[info_hash.upper()]
        except KeyError:
            raise QueueItemNotFound(info_hash) from None

    async def remove_and_blocklist(self, queue_id: int) -> None:
        self.calls.append(ArrCall("remove_and_blocklist", (queue_id,)))
        self._check()
        for key, qid in list(self.queue.items()):
            if qid == queue_id:
                del self.queue[key]
                self.removed.append(queue_id)
                return
        raise QueueItemNotFound(str(queue_id))
