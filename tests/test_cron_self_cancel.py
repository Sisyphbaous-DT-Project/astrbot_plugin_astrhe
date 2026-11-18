"""闹钟唤醒后改期或取消自己：不能调用 delete_job 删除正在执行的自己。

AstrBot 4.29.4 起 delete_job 会取消并等待该任务正在执行的那次运行，而工具在单独的
任务里执行，运行中删除自己会互相等待、永远卡住。前半部分用假对象检查分支，
后半部分在子进程里用真实 CronJobManager、SQLite 与 tool_loop_agent 复现整条路径
（卡住时子进程自行退出，不拖住整套测试）。
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from astrhe.platform import PlatformHelper
from astrhe.qq.client import QQClient
from astrhe.qq.rate_limit import RateLimiter
from astrhe.tools.schedule_tool import ScheduleToolImpl
from astrhe.tools.secrecy import PresentResolver
from tests.conftest import FakeContext, FakeEvent, FakeLogger, arun, make_storage

TZ = ZoneInfo("Asia/Shanghai")
GROUP_UMO = "default:GroupMessage:20002"
ADMIN = FakeEvent(umo="default:FriendMessage:10001", sender_id="10001", role="admin")


def _at(hour: int) -> str:
    # 固定在明天，避开深夜免打扰时段
    dt = (datetime.now(TZ) + timedelta(days=1)).replace(hour=hour, minute=0)
    return dt.strftime("%Y-%m-%d %H:%M")


def _impl(storage, ctx):
    qq = QQClient(PlatformHelper(ctx, FakeLogger()))
    return ScheduleToolImpl(
        storage,
        ctx,
        qq,
        RateLimiter(storage, lambda: TZ),
        lambda: TZ,
        PresentResolver(storage, qq),
        lambda: ["10001"],
    )


def _context():
    return FakeContext(
        api_actions={
            "get_group_list": [{"group_id": 20002}],
            "get_friend_list": [{"user_id": 40004}],
            "get_login_info": {"user_id": 99999},
        }
    )


async def _create(impl, storage, hour=12) -> dict:
    result = await impl.run(
        ADMIN,
        action="create",
        target_type="group",
        target_id="20002",
        run_at=_at(hour),
        background="测试",
        purpose="提醒",
        how="发一句话",
    )
    assert "定好了" in result, result
    return max(await storage.alarms.active_alarms(), key=lambda a: a["id"])


def _woken(job_id: str, alarm_id: int = 0) -> FakeEvent:
    """模拟闹钟唤醒事件：extras 与 AstrBot 唤醒时写入的一致。"""
    payload = {"session": GROUP_UMO, "sender_id": "10001"}
    if alarm_id:
        payload["astrhe_alarm_id"] = str(alarm_id)
    return FakeEvent(
        umo=GROUP_UMO,
        sender_id=GROUP_UMO,
        role="admin",
        extras={"cron_payload": payload, "cron_job": {"id": job_id}},
    )


def test_woken_alarm_reschedules_itself_without_deleting_running_job(tmp_path):
    async def run():
        s = await make_storage(tmp_path)
        try:
            ctx = _context()
            impl = _impl(s, ctx)
            alarm = await _create(impl, s)
            old_job = alarm["cron_job_id"]
            result = await impl.run(
                _woken(old_job, alarm["id"]),
                action="edit",
                alarm_id=alarm["id"],
                run_at=_at(13),
            )
            return ctx, old_job, result, await s.alarms.get_alarm(alarm["id"])
        finally:
            await s.close()

    ctx, old_job, result, alarm = arun(run())
    assert result == f"闹钟 #{alarm['id']} 已更新"
    assert old_job not in ctx.cron_manager.deleted
    assert (old_job, {"enabled": False}) in ctx.cron_manager.updated
    assert ctx.cron_manager.jobs[old_job].enabled is False
    assert alarm["cron_job_id"] != old_job
    assert ctx.cron_manager.jobs[alarm["cron_job_id"]].enabled is True


def test_woken_alarm_cancels_itself_by_disabling(tmp_path):
    async def run():
        s = await make_storage(tmp_path)
        try:
            ctx = _context()
            impl = _impl(s, ctx)
            alarm = await _create(impl, s)
            result = await impl.run(
                _woken(alarm["cron_job_id"], alarm["id"]),
                action="cancel",
                alarm_id=alarm["id"],
            )
            return ctx, alarm, result, await s.alarms.get_alarm(alarm["id"])
        finally:
            await s.close()

    ctx, before, result, after = arun(run())
    assert "这次唤醒会正常做完" in result
    assert after["status"] == "cancelled"
    assert before["cron_job_id"] not in ctx.cron_manager.deleted
    assert ctx.cron_manager.jobs[before["cron_job_id"]].enabled is False


def test_woken_alarm_still_deletes_other_jobs(tmp_path):
    async def run():
        s = await make_storage(tmp_path)
        try:
            ctx = _context()
            impl = _impl(s, ctx)
            running = await _create(impl, s, hour=12)
            other = await _create(impl, s, hour=14)
            woken = _woken(running["cron_job_id"], running["id"])
            cancel = await impl.run(woken, action="cancel", alarm_id=other["id"])
            external = await ctx.cron_manager.add_active_job(name="外部任务")
            ext_other = await impl.run(woken, action="cancel", job_id=external.job_id)
            ext_self = await impl.run(
                _woken(external.job_id), action="cancel", job_id=external.job_id
            )
            return ctx, other, cancel, ext_other, ext_self, external
        finally:
            await s.close()

    ctx, other, cancel, ext_other, ext_self, external = arun(run())
    assert cancel == f"闹钟 #{other['id']} 已取消"
    assert other["cron_job_id"] in ctx.cron_manager.deleted
    assert ext_other == f"外部任务 {external.job_id} 已取消"
    assert external.job_id in ctx.cron_manager.deleted
    # 外部任务唤醒后取消自己：同样改为停用
    assert "已停用" in ext_self
    assert ctx.cron_manager.deleted.count(external.job_id) == 1


# ---------------- 真实 AstrBot 路径（子进程） ----------------

ASTRBOT_SOURCE = os.environ.get("ASTRBOT_SOURCE_PATH")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    not ASTRBOT_SOURCE or not Path(ASTRBOT_SOURCE).is_dir(),
    reason="需要 ASTRBOT_SOURCE_PATH 指向 AstrBot 源码",
)
@pytest.mark.parametrize("mode", ["edit", "cancel"])
def test_real_cron_run_can_modify_its_own_alarm(tmp_path, mode):
    try:
        from astrbot.core.cron.events import CronMessageEvent  # noqa: F401
    except Exception:
        pytest.skip(
            "AstrBot 4.30 起旧 CronMessageEvent 已删除（调度与 agent 解耦，"
            "不再有自删死锁）；4.30 的派发与准入由 test_wakeup_compat 覆盖"
        )
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(PROJECT_ROOT), str(ASTRBOT_SOURCE)])
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        [sys.executable, "-m", "tests.test_cron_self_cancel", mode, str(tmp_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    lines = [x for x in proc.stdout.splitlines() if x.startswith("RESULT ")]
    assert lines, proc.stdout[-2000:] + proc.stderr[-4000:]
    result = json.loads(lines[-1][len("RESULT ") :])
    assert not result["stuck"], result
    assert result["run_finished"] and result["second_round"], result
    if mode == "edit":
        # 原任务这次跑完由 AstrBot 自行删除，只剩改期后的新任务
        assert [j["enabled"] for j in result["jobs"]] == [True], result
        assert result["jobs"][0]["id"] == result["alarm"]["cron_job_id"], result
        assert result["jobs"][0]["id"] != result["old_job"], result
        assert result["alarm"]["status"] == "scheduled", result
    else:
        assert result["jobs"] == [], result
        assert result["alarm"]["status"] == "cancelled", result


async def _real_scenario(mode: str, work: Path) -> dict:
    from types import SimpleNamespace

    from astrbot.core.agent.tool import ToolSet
    from astrbot.core.cron.events import CronMessageEvent
    from astrbot.core.cron.manager import CronJobManager
    from astrbot.core.db.sqlite import SQLiteDatabase
    from astrbot.core.message.message_event_result import MessageChain
    from astrbot.core.platform.message_session import MessageSession
    from astrbot.core.provider.entities import LLMResponse
    from astrbot.core.provider.provider import Provider
    from astrbot.core.star.context import Context as RealContext

    from astrhe.tools.base import AstrHeFunctionTool

    state = {"second_round": False, "run_finished": False}

    class ScriptProvider(Provider):
        """第一轮让模型调用 astrhe_schedule，第二轮给出结束语。"""

        def __init__(self, tool_args):
            super().__init__({"id": "fake", "type": "fake", "enable": True}, {})
            self.rounds = 0
            self.tool_args = tool_args

        async def text_chat(self, **kwargs):
            raise NotImplementedError

        async def text_chat_stream(self, **kwargs):
            self.rounds += 1
            if self.rounds == 1:
                resp = LLMResponse(
                    role="assistant",
                    result_chain=MessageChain().message(""),
                    tools_call_args=[self.tool_args],
                    tools_call_name=["astrhe_schedule"],
                    tools_call_ids=["call-1"],
                )
                resp.is_chunk = False
                yield resp
            else:
                state["second_round"] = True
                yield LLMResponse(
                    role="assistant", result_chain=MessageChain().message("办完了")
                )

        def get_current_key(self):
            return "k"

        def get_models(self):
            return ["m"]

        def set_key(self, key):
            pass

    db = SQLiteDatabase(str(work / "data_v4.db"))
    await db.initialize()
    cron = CronJobManager(db)
    storage = await make_storage(work)
    ctx = _context()
    ctx.cron_manager = cron
    impl = _impl(storage, ctx)
    alarm = await _create(impl, storage)
    old_job = alarm["cron_job_id"]

    tool = AstrHeFunctionTool(
        name="astrhe_schedule",
        description="定时",
        parameters={
            "type": "object",
            "properties": {
                "action": {"type": "string"},
                "alarm_id": {"type": "integer"},
                "run_at": {"type": "string"},
            },
        },
        fn=impl.run,
    )
    args = {"action": mode, "alarm_id": alarm["id"]}
    if mode == "edit":
        args["run_at"] = _at(13)
    provider = ScriptProvider(args)

    async def get_provider_by_id(_):
        return provider

    pm = SimpleNamespace(
        llm_tools=SimpleNamespace(func_list=[]), get_provider_by_id=get_provider_by_id
    )
    real_ctx = RealContext(
        asyncio.Queue(),
        {},
        None,
        pm,
        ctx.platform_manager,
        None,
        None,
        ctx.persona_manager,
        None,
        None,
        cron,
    )

    async def run_active(job, start_time=None):
        # 与 AlarmTracker.on_agent_begin 一致：一次性闹钟开始执行就停用平台任务
        await cron.update_job(job.job_id, enabled=False)
        session = MessageSession.from_str(job.payload["session"])
        event = CronMessageEvent(
            context=real_ctx,
            session=session,
            message=job.payload["note"],
            extras={
                "cron_payload": job.payload,
                "cron_job": {
                    "id": job.job_id,
                    "run_started_at": start_time.isoformat(),
                },
            },
            message_type=session.message_type,
        )
        event.role = "admin"  # 发起人在 admins_id 里，与 _woke_main_agent 一致
        await real_ctx.tool_loop_agent(
            event=event,
            chat_provider_id="fake",
            prompt="醒来",
            tools=ToolSet(tools=[tool]),
            max_steps=50,
            tool_call_timeout=60,
            stream=True,
        )
        state["run_finished"] = True

    cron._run_active_agent_job = run_active
    task = asyncio.create_task(cron._run_job(old_job))
    done, _ = await asyncio.wait({task}, timeout=20)
    if task not in done:
        return {"stuck": True}
    task.result()
    jobs = [{"id": j.job_id, "enabled": j.enabled} for j in await db.list_cron_jobs()]
    current = await storage.alarms.get_alarm(alarm["id"])
    await storage.close()
    if cron.scheduler.running:
        cron.scheduler.shutdown(wait=False)
    return {
        "stuck": False,
        **state,
        "old_job": old_job,
        "jobs": jobs,
        "alarm": {"status": current["status"], "cron_job_id": current["cron_job_id"]},
    }


async def _main(mode: str, work: Path) -> None:
    result = await _real_scenario(mode, work)
    print("RESULT " + json.dumps(result, ensure_ascii=False), flush=True)
    # 卡住时事件循环里还挂着互相等待的任务，收尾会一直等下去，所以直接退出
    os._exit(0)


if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1], Path(sys.argv[2])))
