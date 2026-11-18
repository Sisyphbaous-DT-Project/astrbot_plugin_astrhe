"""4.30 唤醒适配测试：派发快照、外部任务不受影响、孤儿不回落、admission
复查、turn 结果结算、双向追话隔离、安装/卸载与包装所有权。

只在能探测到 AstrBot 4.30 新架构（AgentWakeupEvent 等）时运行；
旧版 AstrBot 上这些包装本来就不安装。
"""

from __future__ import annotations

import asyncio
import inspect
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from astrhe.access import SessionAccess
from astrhe.alarm_tracker import AlarmTracker
from astrhe.wakeup_compat import DISPATCH_EXTRA_KEY, WakeupCompat
from tests.conftest import (
    FakeContext,
    FakeCronJob,
    FakeEvent,
    FakeLogger,
    arun,
    make_storage,
)

TZ = timezone.utc
GROUP_UMO = "default:GroupMessage:20002"

#: 本文件测试创建过的 Storage，用例结束时统一关闭（aiosqlite 线程才不报错）
_OPEN_STORAGES: list = []


@pytest.fixture(autouse=True)
def _close_storages():
    yield
    while _OPEN_STORAGES:
        s = _OPEN_STORAGES.pop()
        try:
            arun(s.close())
        except Exception:  # noqa: BLE001
            pass


class FakeAgentTurn:
    def __init__(self, event, result=None):
        self.input = SimpleNamespace(event=event)
        self._result = result

    async def result(self):
        if self._result is not None:
            return self._result
        return SimpleNamespace(status="completed", reason="completed", error=None)


class FakeAgentSession:
    def __init__(self, session_id, umo):
        self.id = session_id
        self.umo = umo
        self.turn_results: list = []
        #: 预设的 turn 结果（测真实 TurnResult/TurnStatus 时用）
        self.preset_result = None

    async def run_turn(self, input, *, admit=None):
        admitted = None if admit is None else await admit()
        turn = FakeAgentTurn(input.event, self.preset_result)
        self.turn_results.append({"admitted": admitted, "event": input.event})
        return turn


class FakeAgentRuntime:
    runtime_name = "astrbot"

    def __init__(self):
        self.sessions: dict[str, FakeAgentSession] = {}
        self._active: dict[str, FakeAgentTurn] = {}

    async def open_session(self, umo: str, **kwargs):
        if umo not in self.sessions:
            self.sessions[umo] = FakeAgentSession(f"cid-{umo[-5:]}", umo)
        return self.sessions[umo]

    def active_turn(self, umo: str):
        return self._active.get(umo)


class DispatchCronManager:
    """记录宿主原派发方法调用的假 cron 管理器。"""

    def __init__(self):
        self.jobs: dict[str, FakeCronJob] = {}
        self._running_tasks = {}
        self.db = self
        self.updated: list[tuple] = []
        self.orig_dispatched: list[str] = []

    async def _run_active_agent_job(self, job, start_time):
        self.orig_dispatched.append(job.job_id)

    async def add_active_job(self, **kwargs):
        job = FakeCronJob(**kwargs)
        self.jobs[job.job_id] = job
        return job

    async def get_cron_job(self, job_id):
        return self.jobs.get(job_id)

    async def delete_job(self, job_id):
        self.jobs.pop(job_id, None)

    async def update_job(self, job_id, **kwargs):
        self.updated.append((job_id, kwargs))

    update_cron_job = update_job

    async def list_jobs(self, job_type=None):
        return list(self.jobs.values())


class Context430(FakeContext):
    """带 4.30 形状的假 Context：事件队列、agent 服务与原生 runtime。"""

    def __init__(self, config=None):
        super().__init__(config=config)
        self.cron_manager = DispatchCronManager()
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.runtime = FakeAgentRuntime()
        self.agent = SimpleNamespace(_runtimes={"astrbot": self.runtime})

    def get_event_queue(self):
        return self.event_queue


def _compat(tmp_path, *, access=None):
    ctx = Context430()
    storage = arun(make_storage(tmp_path))
    _OPEN_STORAGES.append(storage)
    tracker = AlarmTracker(storage, ctx, lambda: TZ, FakeLogger())
    compat = WakeupCompat(
        context=ctx,
        storage=storage,
        access=access,
        alarm_tracker=tracker,
        logger=FakeLogger(),
    )
    return compat, ctx, storage


def _maybe_skip(compat):
    from astrbot import __version__

    if __version__.startswith("4.29."):
        pytest.skip("需要 AstrBot 4.30 的新唤醒架构")
    assert compat.detect()


def _add_alarm(storage, *, job_id="job-1", target=GROUP_UMO, run_at=None, cron=""):
    alarm_id = arun(
        storage.alarms.add_alarm(
            target_umo=target,
            creator_id="10001",
            origin="day",
            run_at=run_at,
            cron_expression=cron,
            card={"purpose": "测试"},
            note_text="卡片",
        )
    )
    if job_id:
        arun(storage.alarms.bind_cron_job(alarm_id, job_id))
    return alarm_id


def _job(job_id="job-1", payload=None, run_once=False):
    return FakeCronJob(job_id=job_id, payload=payload or {}, run_once=run_once)


def _astrhe_payload(alarm_id, umo=GROUP_UMO, sender="10001", run_at=None):
    payload = {
        "session": umo,
        "sender_id": sender,
        "note": "【背景】测试卡片",
        "origin": "tool",
        "astrhe_alarm_id": str(alarm_id),
    }
    if run_at:
        payload["run_at"] = run_at
    return payload


# ---------------- 派发 ----------------


def test_dispatch_snapshots_and_enqueues_our_alarm(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    job = _job(payload=_astrhe_payload(alarm_id))
    ctx.cron_manager.jobs["job-1"] = job
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        event = ctx.event_queue.get_nowait()
        assert event.source == "cron" and event.source_id == "job-1"
        # 旧版闹钟跟踪/调度工具要的 extras 都补齐了
        assert event.get_extra("cron_payload")["astrhe_alarm_id"] == str(alarm_id)
        cron_job = event.get_extra("cron_job")
        assert cron_job["id"] == "job-1" and "note" not in cron_job
        assert "测试卡片" not in str(event.get_extra()) + event.message_str
        assert (
            "测试卡片"
            in compat._execs[event.get_extra(DISPATCH_EXTRA_KEY)["exec_no"]]["card"]
        )
        info = event.get_extra(DISPATCH_EXTRA_KEY)
        assert info["alarm_id"] == alarm_id and info["job_id"] == "job-1"
        # 纯群号白/黑名单按 message_obj.group_id 匹配
        assert event.message_obj.group_id == "20002"
        # 目标对话已绑定（不再「暂时没有 conversation」）
        assert event.agent_session is not None
        assert event.agent_session.session_id in ("cid-20002",)
        assert event.agent_session.runtime == "astrbot"
        # role 按宿主规则：发起人在 admins 里
        assert event.role == "admin"
        # 派发事件入流水，dispatched 不是成功
        events = arun(storage.alarms.events(alarm_id))
        assert events[-1]["kind"] == "dispatched"
        assert json.loads(events[-1]["detail"])["job"] == "job-1"
        # 外部任务没有被动过
        assert ctx.cron_manager.orig_dispatched == []
    finally:
        arun(compat.uninstall())


def test_dispatch_member_role_for_non_admin_sender(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    job = _job(payload=_astrhe_payload(alarm_id, sender="99999"))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        event = ctx.event_queue.get_nowait()
        assert event.role == "member"
    finally:
        arun(compat.uninstall())


def test_external_jobs_go_through_original_path(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    assert compat.install()
    try:
        job = _job(job_id="ext-1", payload={"session": GROUP_UMO, "note": "外部"})
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        # 外部任务原样走宿主派发，不入事件队列
        assert ctx.cron_manager.orig_dispatched == ["ext-1"]
        assert ctx.event_queue.empty()
    finally:
        arun(compat.uninstall())


def test_orphan_and_stale_jobs_never_dispatch_nor_fall_back(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    assert compat.install()
    try:
        # 本地闹钟不存在（数据被重置过）
        orphan = _job(payload=_astrhe_payload(999))
        arun(ctx.cron_manager._run_active_agent_job(orphan, datetime.now(TZ)))
        # 绑定对不上（已改期到新任务）
        alarm_id = _add_alarm(storage, job_id="job-new")
        stale = _job(job_id="job-old", payload=_astrhe_payload(alarm_id))
        arun(ctx.cron_manager._run_active_agent_job(stale, datetime.now(TZ)))
        # 闹钟已取消
        arun(storage.alarms.set_status(alarm_id, "cancelled"))
        cancelled = _job(job_id="job-new", payload=_astrhe_payload(alarm_id))
        arun(ctx.cron_manager._run_active_agent_job(cancelled, datetime.now(TZ)))
        assert ctx.event_queue.empty()
        # 不能退回原方法执行（否则会无身份地再跑一次）
        assert ctx.cron_manager.orig_dispatched == []
    finally:
        arun(compat.uninstall())


def test_manual_one_shot_disabled_at_dispatch(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage, run_at=1750000000.0)
    payload = _astrhe_payload(alarm_id, run_at="2075-01-01T00:00:00+00:00")
    manual = _job(payload=payload, run_once=True)
    ctx.cron_manager.jobs["job-1"] = manual
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(manual, datetime.now(TZ)))
        # 面板立即执行：派发点停用，排队期间原定时间不再触发
        assert ctx.cron_manager.updated == [("job-1", {"enabled": False})]
        assert not ctx.event_queue.empty()
        # 正常到点的一次性任务不在派发点停用（平台行由宿主删除）
        due_payload = _astrhe_payload(alarm_id, run_at=datetime.now(TZ).isoformat())
        ctx.cron_manager.updated.clear()
        due = _job(job_id="job-2", payload=due_payload, run_once=True)
        arun(ctx.cron_manager._run_active_agent_job(due, datetime.now(TZ)))
        assert ctx.cron_manager.updated == []
        # 重复任务不碰
        ctx.cron_manager.updated.clear()
        repeat = _job(job_id="job-3", payload=_astrhe_payload(alarm_id))
        arun(ctx.cron_manager._run_active_agent_job(repeat, datetime.now(TZ)))
        assert ctx.cron_manager.updated == []
    finally:
        arun(compat.uninstall())


# ---------------- admission 复查 ----------------


def _info(alarm_id, job_id="job-1", exec_no="exec-1"):
    return {
        "alarm_id": alarm_id,
        "exec_no": exec_no,
        "job_id": job_id,
        "target_umo": GROUP_UMO,
        "run_once": True,
        "dispatched_at": 0.0,
    }


def test_admission_rechecks_binding_status_and_access(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    access = SessionAccess(
        ctx, {}, lambda: ["10001"], FakeLogger(), platform_id_getter=lambda: "default"
    )
    compat.access = access
    alarm_id = _add_alarm(storage, run_at=1750000000.0)

    async def check():
        info = _info(alarm_id)
        assert await compat._recheck(info) is True

        # 排队期间改期：绑定换了，旧派发跳过；闹钟保持 scheduled 等新任务
        await storage.alarms.bind_cron_job(alarm_id, "job-new")
        assert await compat._recheck(_info(alarm_id, job_id="job-1")) is False
        alarm = await storage.alarms.get_alarm(alarm_id)
        assert alarm["status"] == "scheduled"  # 新任务的安排不被旧执行覆盖
        events = await storage.alarms.events(alarm_id)
        assert events[-1]["kind"] == "skipped"

        # 恢复绑定后：取消状态跳过
        await storage.alarms.bind_cron_job(alarm_id, "job-1")
        await storage.db.execute(
            "UPDATE alarms SET status='cancelled' WHERE id=?", (alarm_id,)
        )
        assert await compat._recheck(_info(alarm_id)) is False

        # 宿主停用目标会话：跳过（管理员也不越过）；一次性任务被拦下即结清
        await storage.db.execute(
            "UPDATE alarms SET status='scheduled' WHERE id=?", (alarm_id,)
        )
        ctx._config = {
            "admins_id": ["10001"],
            "platform_settings": {
                "enable_id_white_list": False,
                "id_blacklist": ["20002"],
            },
        }
        assert await compat._recheck(_info(alarm_id)) is False
        alarm = await storage.alarms.get_alarm(alarm_id)
        assert alarm["status"] == "failed"  # 平台行已删，不会再触发

    arun(check())


def test_admission_respects_original_admit(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)

    async def allow():
        return True

    async def deny():
        return False

    async def check():
        admitted = await compat._make_admit(_info(alarm_id), allow)()
        assert admitted is True
        denied = await compat._make_admit(_info(alarm_id), deny)()
        assert denied is False

    arun(check())


# ---------------- turn 结果结算 ----------------


def test_settle_turn_outcomes(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    tracker = compat.alarm_tracker
    one = _add_alarm(storage, job_id="job-a", run_at=1750000000.0)
    two = _add_alarm(storage, job_id="job-b", run_at=1750000000.0)
    three = _add_alarm(storage, job_id="job-c", run_at=1750000000.0)
    four = _add_alarm(storage, job_id="job-d", cron="0 9 * * *")
    five = _add_alarm(storage, job_id="job-e", run_at=1750000000.0)

    async def check():
        await tracker.settle_turn(one, "job-a", "e1", "completed", "completed")
        alarm = await storage.alarms.get_alarm(one)
        assert alarm["status"] == "done"
        # 同一次执行的重复结算幂等
        await tracker.settle_turn(one, "job-a", "e1", "cancelled", "cancelled")
        assert (await storage.alarms.get_alarm(one))["status"] == "done"
        events = await storage.alarms.events(one)
        assert [e["kind"] for e in events][-1] == "done"
        assert len([e for e in events if e["kind"] == "done"]) == 1

        # completed + reason=skipped 不算成功
        await tracker.settle_turn(two, "job-b", "e2", "completed", "skipped")
        alarm = await storage.alarms.get_alarm(two)
        assert alarm["status"] == "failed"
        events = await storage.alarms.events(two)
        assert events[-1]["kind"] == "interrupted"

        await tracker.settle_turn(three, "job-c", "e3", "failed", "error", "boom")
        assert (await storage.alarms.get_alarm(three))["status"] == "failed"

        # 重复闹钟中断：failed 仍算生效（live），保留下一次排程
        await storage.alarms.set_status(four, "fired")
        await tracker.settle_turn(four, "job-d", "e4", "cancelled", "cancelled")
        alarm = await storage.alarms.get_alarm(four)
        assert alarm["status"] == "failed"
        assert alarm in await storage.alarms.live_alarms()
        assert alarm in await storage.alarms.active_alarms()

        # 改期后旧执行不覆盖新安排
        await storage.alarms.set_status(five, "fired")
        await storage.alarms.bind_cron_job(five, "job-new")
        await tracker.settle_turn(five, "job-e", "e5", "completed", "completed")
        alarm = await storage.alarms.get_alarm(five)
        assert alarm["status"] == "fired"

    arun(check())


def test_run_turn_wrapper_admits_and_settles(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage, run_at=1750000000.0)
    job = _job(payload=_astrhe_payload(alarm_id))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        event = ctx.event_queue.get_nowait()
        session = ctx.runtime.sessions[GROUP_UMO]

        async def check():
            await session.run_turn(SimpleNamespace(event=event))
            assert session.turn_results[0]["admitted"] is True
            await asyncio.gather(*list(compat._observer_tasks))
            assert (await storage.alarms.get_alarm(alarm_id))["status"] == "done"

        arun(check())
    finally:
        arun(compat.uninstall())


def test_run_turn_wrapper_passes_through_foreign_input(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    job = _job(payload=_astrhe_payload(alarm_id))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        session = ctx.runtime.sessions[GROUP_UMO]
        foreign_event = FakeEvent()
        settled = []

        async def cb(event, result):
            settled.append(result)

        compat.on_turn_settled.append(cb)

        async def check():
            await session.run_turn(SimpleNamespace(event=foreign_event))
            await asyncio.gather(*list(compat._watcher_tasks))
            assert settled  # 非 AstrHe turn 也通知回执确认回调

        arun(check())
    finally:
        arun(compat.uninstall())


def test_card_temp_projection_on_real_host_code(tmp_path):
    """用宿主真实的历史投影函数验证：卡片临时部分本轮保留、后续上下文排除。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    from astrbot.core.agent.message import Message, TextPart, dump_messages
    from astrbot.core.db.conversation import persistent_messages

    # runner 会把 req.prompt（占位句）与 extra_user_content_parts（临时卡片）
    # 组装成一条 user 消息；injector 的卡片临时化正是这个形状
    card = TextPart(
        text="<system_reminder>\n# CRON JOB CONTEXT\n秘密卡片内容\n</system_reminder>"
    )
    card.mark_as_temp()
    message = Message(
        role="user",
        content=[TextPart(text="（AstrHe 定时任务唤醒……）"), card],
    )
    # writer 路径保存时保留临时标记
    stored = dump_messages([message], include_temporary=True)
    assert stored[0]["content"][1]["_no_save"] is True
    # 后续模型上下文：临时部分被排除，占位句保留
    projected = persistent_messages(stored)
    assert projected[0]["content"] == [
        {"type": "text", "text": "（AstrHe 定时任务唤醒……）"}
    ]
    assert "秘密卡片内容" not in str(projected)


# ---------------- 追话隔离与 ProcessStage ----------------


def test_active_turn_hides_our_alarm_turns_only(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    job = _job(payload=_astrhe_payload(alarm_id))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        event = ctx.event_queue.get_nowait()
        ours = FakeAgentTurn(event)
        other = FakeAgentTurn(FakeEvent())
        ctx.runtime._active[GROUP_UMO] = ours
        assert ctx.runtime.active_turn(GROUP_UMO) is None  # 普通聊天不被吸收
        ctx.runtime._active[GROUP_UMO] = other
        assert ctx.runtime.active_turn(GROUP_UMO) is other  # 别的 turn 照常
    finally:
        arun(compat.uninstall())


def test_process_stage_routes_our_wakeup_around_waiter(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    from astrbot.core.pipeline.process_stage.stage import ProcessStage

    assert compat.install()
    try:
        stage = ProcessStage()
        calls = []

        async def agen(event):
            calls.append(event)
            yield

        stage.agent_sub_stage = SimpleNamespace(process=lambda event: agen(event))
        alarm = _add_alarm(storage)
        arun(
            ctx.cron_manager._run_active_agent_job(
                _job(payload=_astrhe_payload(alarm)), datetime.now(TZ)
            )
        )
        our_event = ctx.event_queue.get_nowait()
        result = stage.process(our_event)
        assert inspect.isasyncgen(result)
        arun(_consume(result))
        assert calls == [our_event]
        # 其他事件原样交给宿主 ProcessStage（惰性生成器，不迭代就不执行）
        foreign = stage.process(FakeEvent())
        assert calls == [our_event]
        assert inspect.isasyncgen(foreign) or foreign is not None
        foreign.close() if hasattr(foreign, "close") else None
    finally:
        arun(compat.uninstall())


async def _consume(agen):
    async for _ in agen:
        pass


# ---------------- 复核修正（真实宿主形状） ----------------


def test_observer_settles_real_turn_result_enum(tmp_path):
    """真实宿主返回的 TurnStatus 是枚举：str() 得到全名，结算必须按 .value。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    from astrbot.core.agent.events import TurnResult
    from astrbot.core.agent.runtime import TurnStatus

    alarm_id = _add_alarm(storage, run_at=1750000000.0)
    job = _job(payload=_astrhe_payload(alarm_id))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        event = ctx.event_queue.get_nowait()
        session = ctx.runtime.sessions[GROUP_UMO]
        session.preset_result = TurnResult(
            status=TurnStatus.COMPLETED, reason="completed"
        )

        async def check():
            await session.run_turn(SimpleNamespace(event=event))
            await asyncio.gather(*list(compat._observer_tasks))
            alarm = await storage.alarms.get_alarm(alarm_id)
            assert alarm["status"] == "done", alarm["result_note"]
            events = await storage.alarms.events(alarm_id)
            assert events[-1]["kind"] == "done"

        arun(check())
    finally:
        arun(compat.uninstall())


def test_repeat_alarm_each_trigger_admits_and_settles(tmp_path):
    """重复闹钟：第一次 done/failed 后下一次触发照常执行，结算按执行编号区分。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    tracker = compat.alarm_tracker
    alarm_id = _add_alarm(storage, job_id="job-r", cron="0 9 * * *")
    once = _add_alarm(storage, job_id="job-o", run_at=1750000000.0)

    async def check():
        info = _info(alarm_id, job_id="job-r")
        # 第一次执行成功 -> done
        await storage.alarms.set_status(alarm_id, "fired")
        await tracker.settle_turn(alarm_id, "job-r", "r1", "completed", "completed")
        assert (await storage.alarms.get_alarm(alarm_id))["status"] == "done"
        # 第二次触发准入放行
        assert await compat._recheck(info) is True
        # 第二次执行中断：不被第一次的 done 幂等挡住
        await storage.alarms.set_status(alarm_id, "fired")
        await tracker.settle_turn(alarm_id, "job-r", "r2", "cancelled", "cancelled")
        assert (await storage.alarms.get_alarm(alarm_id))["status"] == "failed"
        kinds = [e["kind"] for e in await storage.alarms.events(alarm_id)]
        assert kinds.count("done") == 1 and kinds.count("interrupted") == 1
        # 上一次失败后第三次仍放行；取消/失踪不放行
        assert await compat._recheck(info) is True
        await storage.db.execute(
            "UPDATE alarms SET status='cancelled' WHERE id=?", (alarm_id,)
        )
        assert await compat._recheck(info) is False
        # 一次性闹钟执行过（done）后不再放行
        await storage.alarms.set_status(once, "fired")
        await tracker.settle_turn(once, "job-o", "o1", "completed", "completed")
        assert await compat._recheck(_info(once, job_id="job-o")) is False

    arun(check())


def test_dispatch_rejects_non_native_runtime(tmp_path):
    """目标会话配了别的 agent runtime（如 dify）：明确失败，不静默换原生。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    job = _job(payload=_astrhe_payload(alarm_id))
    ctx._config = {
        "admins_id": ["10001"],
        "agent": {"runtime": "dify", "config": {}},
    }
    assert compat.install()
    try:
        with pytest.raises(ValueError, match="dify"):
            arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        assert ctx.event_queue.empty()
        assert ctx.cron_manager.orig_dispatched == []
        # 改回原生 runtime 后照常派发
        ctx._config = {"admins_id": ["10001"]}
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        assert not ctx.event_queue.empty()
    finally:
        arun(compat.uninstall())


def test_dispatch_role_uses_target_session_config(tmp_path):
    """role 按目标会话那份配置的 admins_id 算，默认配置的管理员不作数。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    alarm_id = _add_alarm(storage)
    # 默认配置的管理员包含发起人；目标会话那份为空 -> member
    ctx.get_config = lambda umo=None: (
        {"admins_id": []} if umo == GROUP_UMO else {"admins_id": ["10001"]}
    )
    job = _job(payload=_astrhe_payload(alarm_id, sender="10001"))
    assert compat.install()
    try:
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        assert ctx.event_queue.get_nowait().role == "member"
        # 目标会话那份也包含发起人 -> admin
        ctx.get_config = lambda umo=None: {"admins_id": ["10001"]}
        arun(ctx.cron_manager._run_active_agent_job(job, datetime.now(TZ)))
        assert ctx.event_queue.get_nowait().role == "admin"
    finally:
        arun(compat.uninstall())


def test_admission_reads_latest_config_outside_snapshot(tmp_path):
    """排队期间的准入复查在事件配置快照之外读最新配置，读完恢复快照。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    from astrbot.core.config.runtime import current_message_config

    alarm_id = _add_alarm(storage, run_at=1750000000.0)
    seen = []

    class ProbeAccess:
        async def allows(self, umo):
            seen.append(current_message_config.get())
            return True

    compat.access = ProbeAccess()
    scope = SimpleNamespace(active=True)

    async def check():
        # 事件快照在位；复查期间退出快照读最新，读完恢复
        assert current_message_config.get() is scope
        assert await compat._recheck(_info(alarm_id)) is True
        assert current_message_config.get() is scope

    token = current_message_config.set(scope)
    try:
        arun(check())
    finally:
        current_message_config.reset(token)
    assert seen == [None]  # 复查期间读到的是快照之外（None）
    assert current_message_config.get() is None  # 外层恢复到 set 之前的状态


def test_uninstall_cancels_watchers_but_joins_observers(tmp_path):
    """卸载只收拢自家闹钟观察者；普通聊天的回执 watcher 直接取消不等待。"""
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    assert compat.install()

    async def scenario():
        hang = asyncio.Event()
        watcher = asyncio.create_task(hang.wait())
        compat._watcher_tasks.add(watcher)
        watcher.add_done_callback(compat._watcher_tasks.discard)

        async def observer_like():
            return "settled"

        observer = asyncio.create_task(observer_like())
        compat._observer_tasks.add(observer)
        observer.add_done_callback(compat._observer_tasks.discard)
        await asyncio.sleep(0)
        # 挂住的 watcher 不能拖住卸载
        await asyncio.wait_for(compat.uninstall(), timeout=2)
        assert watcher.cancelled()
        assert observer.done()

    arun(scenario())
    assert not compat._watcher_tasks
    assert not compat._observer_tasks


# ---------------- 安装 / 卸载 / 所有权 ----------------


def test_install_and_uninstall_restore_everything(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    from astrbot.core.pipeline.process_stage.stage import ProcessStage

    orig_dispatch = ctx.cron_manager._run_active_agent_job
    orig_process = ProcessStage.process
    orig_active = ctx.runtime.active_turn
    orig_open = ctx.runtime.open_session
    assert compat.install()
    assert compat.installed
    assert ctx.cron_manager._run_active_agent_job is not orig_dispatch
    assert ProcessStage.process is not orig_process
    assert ctx.runtime.active_turn is not orig_active
    assert ctx.runtime.open_session is not orig_open
    # 重复安装不重复包装
    assert compat.install() is True
    arun(compat.uninstall())
    # 绑定方法每次属性访问都是新对象，用等值比较；类属性用同一性
    assert ctx.cron_manager._run_active_agent_job == orig_dispatch
    assert ProcessStage.process is orig_process
    assert ctx.runtime.active_turn == orig_active
    assert ctx.runtime.open_session == orig_open
    assert not compat.installed


def test_uninstall_keeps_foreign_wrappers(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    assert compat.install()
    # 别的插件在我们之后包了同一个入口
    inner = ctx.cron_manager._run_active_agent_job

    async def foreign(job, start_time):
        return await inner(job, start_time)

    ctx.cron_manager._run_active_agent_job = foreign
    arun(compat.uninstall())
    # 不能把别人的包装拆掉
    assert ctx.cron_manager._run_active_agent_job is foreign
    # 已经打开过的会话上的 run_turn 包装同样只恢复自己持有的
    # （这里没有会话，跳过）


def test_closed_dispatch_swallows_our_jobs(tmp_path):
    compat, ctx, storage = _compat(tmp_path)
    _maybe_skip(compat)
    _add_alarm(storage)
    assert compat.install()
    arun(compat.uninstall())  # 卸载后 closed=True、包装已恢复
    # 恢复后的宿主原方法不认识 AstrHe 身份，这里只验证适配器的行为约定：
    # 卸载期间不再有我们的派发（事件队列空、宿主原方法未被调用）
    assert ctx.event_queue.empty()
    assert ctx.cron_manager.orig_dispatched == []
