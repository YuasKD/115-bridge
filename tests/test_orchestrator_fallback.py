"""T13：Arr 回退的 5 步、重试与告警、client_deleted、与 torrents/delete 的竞态。"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

import test_orchestrator_support as sup
from bridge.arr.fake import FakeArr
from bridge.arr.protocol import QueueItemNotFound
from bridge.cloud.fake import OfflineScenario
from bridge.db.repo import Fallback, TaskState
from bridge.markers import CLEANUP_ERROR_PREFIX
from test_orchestrator_support import Env, H, ff, put_at

DEAD = OfflineScenario("Dead", [ff("a.mkv", 100)], outcome=-1)


@pytest.fixture
async def env(tmp_path: Path) -> AsyncIterator[Env]:
    async with sup.make_env(tmp_path) as e:
        yield e


class SpyArr(FakeArr):
    """在 Arr 被调用的那一刻记录任务的状态，证明「先标 failed 再调 Arr」。"""

    def __init__(self, env: Env, name: str = "radarr") -> None:
        super().__init__(name=name)
        self.env = env
        self.seen: list[tuple[str, TaskState, Fallback, str | None]] = []

    async def find_queue_item(self, info_hash: str) -> int:
        task = await self.env.task(info_hash)
        self.seen.append(("find", task.state, task.fallback, task.error))
        return await super().find_queue_item(info_hash)

    async def remove_and_blocklist(self, queue_id: int) -> None:
        task = await self.env.task(H)
        self.seen.append(("remove", task.state, task.fallback, task.error))
        await super().remove_and_blocklist(queue_id)


async def fallback_of(env: Env, info_hash: str = H) -> Fallback:
    return (await env.task(info_hash)).fallback


# ------------------------------------------------------------ 5 步


async def test_five_steps_in_order(env: Env) -> None:
    spy = SpyArr(env)
    spy.add_queue_item(H, 7)
    env.orch = env.make_orch(arrs={"radarr": spy})
    await env.add_task(DEAD, arr_item=None)
    task = await env.drive_terminal()
    # 步骤 1、2、3：按 category 找到 Arr，用 hash 找队列项，再 remove + blocklist
    assert [c.method for c in spy.calls] == ["find_queue_item", "remove_and_blocklist"]
    assert spy.calls[0].args == (H,)  # 调用方传小写，实现方转大写（不变量 11）
    assert spy.removed == [7]
    # 调 Arr 之前，任务已是 failed、原因已写、回退为 pending（先标 failed 再调 Arr）
    for _, state, fb, error in spy.seen:
        assert state is TaskState.FAILED
        assert fb is Fallback.PENDING
        assert error and "status -1" in error
    # 步骤 4、5：成功后回退完成
    assert task.state is TaskState.FAILED and task.fallback is Fallback.DONE
    assert env.notifier.alerts == []


async def test_category_selects_arr(env: Env) -> None:
    await env.add_task(DEAD, category="tv-sonarr", arr_item=None)
    env.sonarr.add_queue_item(H, 42)
    task = await env.drive_terminal()
    assert task.fallback is Fallback.DONE
    assert env.sonarr.removed == [42]
    assert env.radarr.calls == []


async def test_fallback_done_is_not_repeated(env: Env) -> None:
    await env.add_task(DEAD)
    await env.drive_terminal()
    for _ in range(5):
        await env.orch.tick()
        await env.clock.advance(700)
    assert len(env.radarr.calls) == 2


# ------------------------------------------------------------ Arr 失败：保持 error，10 分钟重试


async def retry_times(env: Env, arr: FakeArr, minutes: int = 65) -> list[float]:
    times: list[float] = []
    for _ in range(minutes):
        n = len(arr.calls_to("find_queue_item"))
        await env.orch.tick()
        if len(arr.calls_to("find_queue_item")) > n:
            times.append(env.clock.monotonic())
        await env.clock.advance(60)
    return times


async def test_arr_unreachable_keeps_error_and_retries_every_10_minutes(env: Env) -> None:
    env.radarr.unreachable = True
    await env.add_task(DEAD)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED and task.fallback is Fallback.PENDING
    assert "arr-unreachable-radarr" in env.notifier.kinds  # 按 Arr 分 kind，不是按任务
    first = len(env.radarr.calls)
    assert first == 1  # 立即尝试一次

    times = await retry_times(env, env.radarr)
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    assert gaps and all(g == 600 for g in gaps)
    task = await env.task()
    assert task.state is TaskState.FAILED and task.fallback is Fallback.PENDING  # 仍是 error
    assert task.retries == {}
    assert env.radarr.removed == []
    assert env.notifier.kinds.count("arr-unreachable-radarr") >= 2

    env.radarr.unreachable = False  # Arr 恢复后的下一个 10 分钟周期成功
    await retry_times(env, env.radarr, minutes=12)
    assert await fallback_of(env) is Fallback.DONE
    assert env.radarr.removed == [7]


async def test_queue_item_not_found_retries_silently_then_gives_up_after_an_hour(
    env: Env,
) -> None:
    await env.add_task(DEAD, arr_item=None)  # Arr 队列里没有
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED and task.fallback is Fallback.PENDING
    assert env.notifier.alerts == []  # 第一次找不到不告警（队列可能还没刷新）
    times = await retry_times(env, env.radarr, minutes=40)
    assert len(times) >= 2 and all(b - a == 600 for a, b in zip(times, times[1:], strict=False))
    assert env.notifier.alerts == []  # 持续找不到期间也只重试，不告警
    assert await fallback_of(env) is Fallback.PENDING

    await retry_times(env, env.radarr, minutes=25)  # 从第一次找不到起满 1 小时
    task = await env.task()
    assert task.state is TaskState.FAILED and task.fallback is Fallback.DONE
    assert env.notifier.kinds == [f"arr-notfound-{H[:8]}"]  # 最终告警，提示手动处理
    assert "手动" in env.notifier.alerts[0][1]
    calls = len(env.radarr.calls)
    await retry_times(env, env.radarr, minutes=25)
    assert len(env.radarr.calls) == calls  # 停止重试
    assert len(env.notifier.alerts) == 1


async def test_queue_item_appearing_before_the_deadline_still_succeeds(env: Env) -> None:
    await env.add_task(DEAD, arr_item=None)
    await env.drive_terminal()
    await retry_times(env, env.radarr, minutes=25)
    env.radarr.add_queue_item(H, 9)
    await retry_times(env, env.radarr, minutes=12)
    assert await fallback_of(env) is Fallback.DONE
    assert env.radarr.removed == [9]
    assert env.notifier.alerts == []


async def test_unreachable_does_not_reset_or_trigger_not_found_giveup(env: Env) -> None:
    """Arr 不可达、Key 错误一直重试并告警，不受 1 小时放弃规则影响。"""
    env.radarr.unreachable = True
    await env.add_task(DEAD)
    await env.drive_terminal()
    await retry_times(env, env.radarr, minutes=130)
    assert await fallback_of(env) is Fallback.PENDING
    assert not any(k.startswith("arr-notfound-") for k in env.notifier.kinds)
    assert env.notifier.kinds.count("arr-unreachable-radarr") >= 10


async def test_arr_auth_error_keeps_pending_and_alerts(env: Env) -> None:
    env.radarr.auth_error = True
    await env.add_task(DEAD)
    task = await env.drive_terminal()
    assert task.fallback is Fallback.PENDING
    assert "arr-auth-radarr" in env.notifier.kinds
    assert task.state is TaskState.FAILED and task.retries == {}


async def test_category_without_arr_alerts_and_stays_pending(env: Env) -> None:
    env.orch = env.make_orch(arrs={})
    await env.add_task(DEAD)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED and task.fallback is Fallback.PENDING
    assert "arr-missing-radarr" in env.notifier.kinds
    assert env.radarr.calls == []


# ------------------------------------------------------------ client_deleted 与竞态


async def test_client_deleted_counts_as_fallback_done_without_alert(env: Env) -> None:
    await put_at(env, H, TaskState.CLOUD_DOWNLOADING)
    await env.repo.transition(H, TaskState.FAILED, "x", env.clock.now())
    await env.repo.set_fallback(H, Fallback.PENDING)
    await env.repo.mark_client_deleted(H)  # Arr 已调过 torrents/delete
    for _ in range(3):
        await env.orch.tick()
        await env.clock.advance(700)
    assert await fallback_of(env) is Fallback.DONE
    assert env.radarr.calls == []  # 不再调 Arr
    assert env.notifier.alerts == []  # 也不告警


async def test_delete_arrives_while_arr_reports_not_found(env: Env) -> None:
    """Arr 已处理完并回调 torrents/delete，队列项自然已不在：视为回退完成，不告警。"""

    class Racing(FakeArr):
        async def find_queue_item(self, info_hash: str) -> int:
            await env.repo.mark_client_deleted(info_hash)
            raise QueueItemNotFound(info_hash)

    env.orch = env.make_orch(arrs={"radarr": Racing(name="radarr")})
    await env.add_task(DEAD, arr_item=None)
    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED  # 不改成 removed
    assert task.fallback is Fallback.DONE and task.client_deleted
    assert env.notifier.alerts == []


async def test_arr_callback_delete_is_handled_by_qbit_without_removing(env: Env) -> None:
    """Arr 的 DELETE queue 同步回调 torrents/delete：failed 任务只清本地，不转 removed。"""
    client = await env.qbit()

    class CallbackArr(FakeArr):
        async def remove_and_blocklist(self, queue_id: int) -> None:
            await super().remove_and_blocklist(queue_id)
            r = await client.post("/torrents/delete", data={"hashes": H, "deleteFiles": "true"})
            assert r.status_code == 200

    arr = CallbackArr(name="radarr")
    arr.add_queue_item(H, 5)
    env.orch = env.make_orch(arrs={"radarr": arr})
    await env.add_task(DEAD, arr_item=None)
    leftover = env.incomplete / H
    leftover.mkdir(parents=True)
    (leftover / "partial").write_bytes(b"x")

    task = await env.drive_terminal()
    assert task.state is TaskState.FAILED  # 先标 failed 再调 Arr，回调只清本地
    assert task.client_deleted is True
    assert task.fallback is Fallback.DONE
    assert not leftover.exists()
    assert arr.removed == [5]
    assert env.notifier.alerts == []
    await client.aclose()


async def test_orphaned_failed_task_is_recovered(env: Env) -> None:
    """转 failed 之后、写 pending 之前崩溃：fallback 仍是 none，下一轮补上并回退。"""
    await put_at(env, H, TaskState.CLOUD_DOWNLOADING)
    await env.repo.transition(H, TaskState.FAILED, "crash", env.clock.now())
    env.radarr.add_queue_item(H, 3)
    assert await fallback_of(env) is Fallback.NONE
    await env.orch.tick()
    assert await fallback_of(env) is Fallback.DONE
    assert env.radarr.removed == [3]


# ------------------------------------------------------------ error 两种内容共存


async def test_cleanup_marker_is_not_overwritten_by_failure_reason(env: Env) -> None:
    await put_at(env, H, TaskState.CLOUD_DOWNLOADING)
    marker = f"{CLEANUP_ERROR_PREFIX}: PermissionError: denied"
    await env.repo.set_error(H, marker)
    env.radarr.add_queue_item(H, 8)
    await env.orch._fail(H, "offline task failed (status -1)")
    task = await env.task()
    assert task.state is TaskState.FAILED
    assert task.error == marker  # 清理失败标记原样保留，等 Arr 重试 delete
    events = await env.repo.list_events(H, limit=1)
    assert "status -1" in events[0].reason  # 失败原因仍在事件里
    await env.orch.tick()
    assert await fallback_of(env) is Fallback.DONE
    assert (await env.task()).error == marker  # Arr 回退本身不写 error，也不把标记当成回退错误


async def test_failure_reason_is_written_when_no_marker(env: Env) -> None:
    await env.add_task(DEAD)
    task = await env.drive_terminal()
    assert task.error and "status -1" in task.error
