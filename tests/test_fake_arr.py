from __future__ import annotations

import pytest

from bridge.arr.fake import FakeArr
from bridge.arr.protocol import (
    ArrAuthError,
    ArrClient,
    ArrUnreachable,
    QueueItemNotFound,
)

H = "ab" * 20


async def test_find_and_remove() -> None:
    arr = FakeArr()
    assert isinstance(arr, ArrClient)
    arr.add_queue_item(H, 7)
    assert list(arr.queue) == [H.upper()]
    qid = await arr.find_queue_item(H)
    assert qid == 7
    await arr.remove_and_blocklist(qid)
    assert arr.removed == [7]
    assert arr.calls_to("find_queue_item")[0].args == (H,)
    with pytest.raises(QueueItemNotFound):
        await arr.find_queue_item(H)


async def test_not_found() -> None:
    arr = FakeArr()
    with pytest.raises(QueueItemNotFound):
        await arr.find_queue_item(H)
    with pytest.raises(QueueItemNotFound):
        await arr.remove_and_blocklist(99)
    assert arr.removed == []


async def test_unreachable_and_auth() -> None:
    arr = FakeArr(unreachable=True)
    arr.add_queue_item(H, 1)
    with pytest.raises(ArrUnreachable):
        await arr.find_queue_item(H)
    with pytest.raises(ArrUnreachable):
        await arr.remove_and_blocklist(1)
    arr.unreachable = False
    arr.auth_error = True
    with pytest.raises(ArrAuthError):
        await arr.find_queue_item(H)
    arr.auth_error = False
    assert await arr.find_queue_item(H) == 1
    assert len(arr.calls_to("remove_and_blocklist")) == 1 and arr.removed == []
