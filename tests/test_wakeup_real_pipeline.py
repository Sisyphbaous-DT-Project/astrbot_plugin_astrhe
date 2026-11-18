"""离线全链：宿主调度、原生 runner、插件注册钩子、SQLite writer 都用真实实现。"""

# ruff: noqa: E402
import asyncio
import copy
import importlib.util
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest
from astrbot import __version__

if __version__.startswith("4.29."):
    pytest.skip("4.29 没有事件式唤醒与对话 writer", allow_module_level=True)

from astrbot.builtin_stars.builtin_commands.commands.conversation import (
    ConversationCommands,
)
from astrbot.core import conversation_mgr
from astrbot.core.agent_wakeup import wake_main_agent
from astrbot.core.astr_agent_runtime import AstrAgentRuntime, session as native_session
from astrbot.core.config.default import DEFAULT_CONFIG
from astrbot.core.config.runtime import current_message_config
from astrbot.core.conversation_mgr import ConversationManager
from astrbot.core.cron.manager import CronJobManager
from astrbot.core.db.sqlite import SQLiteDatabase
from astrbot.core.event_bus import EventBus
from astrbot.core.message.components import Plain
from astrbot.core.pipeline import context_utils
from astrbot.core.pipeline.context import PipelineContext
from astrbot.core.pipeline.process_stage.method import star_request
from astrbot.core.pipeline.scheduler import PipelineScheduler
from astrbot.core.pipeline.waking_check import stage as waking
from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.provider.entities import LLMResponse, ProviderRequest
from astrbot.core.provider.func_tool_manager import FunctionToolManager
from astrbot.core.star import base as star_base, star_handler
from astrbot.core.star.context import Context
from astrbot.core.star.register import star_handler as register
from astrbot.core.star.star_tools import StarTools
from astrbot.core.utils import trace as trace_module
from tests.conftest import arun

UMO = "target:GroupMessage:20002"
OTHER = "target:FriendMessage:40004"
CARD = "仅本轮可见的测试卡片"


@asynccontextmanager
async def harness(tmp_path, monkeypatch):
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["provider_settings"].update(
        streaming_response=False, datetime_system_prompt=False
    )
    config["platform_settings"]["unique_session"] = False
    config["admins_id"] = ["10001"]
    queue = asyncio.Queue()
    db = SQLiteDatabase(str(tmp_path / "host.sqlite"))
    await db.initialize()
    mgr = ConversationManager(db)
    monkeypatch.setattr(
        conversation_mgr,
        "sp",
        NS(session_put=AsyncMock(), session_get=AsyncMock(return_value=None)),
    )
    metadata = PlatformMetadata(id="target", name="aiocqhttp", description="离线")
    control = NS(calls=[], responses=[], config=config, queue=queue, db=db, mgr=mgr)

    async def respond(**kwargs):
        control.calls.append(copy.deepcopy(kwargs["contexts"]))
        response = control.responses.pop(0) if control.responses else None
        return (
            await response()
            if callable(response)
            else response or LLMResponse(role="assistant", completion_text="完成")
        )

    provider = NS(
        provider_config={"id": "offline", "modalities": ["text"]},
        get_model=lambda: "offline",
        meta=lambda: NS(type="test"),
        text_chat=respond,
        text_chat_stream=None,
    )
    cron = CronJobManager(db)
    tools = FunctionToolManager()
    ctx = Context.__new__(Context)
    ctx.provider_manager = NS(llm_tools=tools)
    ctx.registered_web_apis = []

    def get_config(umo=None):
        scope = current_message_config.get()
        return scope.config if scope and scope.active and scope.umo == umo else config

    ctx.__dict__.update(
        get_event_queue=lambda: queue,
        get_platform_inst=lambda _: NS(meta=lambda: metadata),
        get_config=get_config,
        astrbot_config_mgr=NS(
            _create_message_config=lambda _: (copy.deepcopy(config), frozenset())
        ),
        conversation_manager=mgr,
        get_using_tts_provider_async=AsyncMock(return_value=None),
        get_llm_tool_manager=lambda: tools,
        send_message=AsyncMock(return_value=True),
        cron_manager=cron,
        get_using_provider_async=AsyncMock(return_value=provider),
        persona_manager=NS(
            resolve_selected_persona=AsyncMock(return_value=(None, None, False))
        ),
        platform_manager=NS(platform_insts=[]),
        kb_manager=None,
    )
    cron.ctx = ctx
    native = AstrAgentRuntime(ctx, agent_configs=MagicMock(), subagents=MagicMock())
    ctx.agent = NS(_runtimes={"astrbot": native})
    registry = star_handler.StarHandlerRegistry()
    maps = {}
    for module in (star_base, waking, star_handler, context_utils, star_request):
        if hasattr(module, "star_map"):
            monkeypatch.setattr(module, "star_map", maps)
    monkeypatch.setattr(star_base, "star_registry", [])
    for module in (register, waking, context_utils):
        monkeypatch.setattr(module, "star_handlers_registry", registry)
    spec = importlib.util.spec_from_file_location(
        "astrhe_pipeline_fixture", Path(__file__).parents[1] / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(StarTools, "get_data_dir", lambda *_: tmp_path / "plugin")
    plugin = module.AstrHe(ctx, {"enabled": True})
    meta = maps[module.__name__]
    meta.name = "astrbot_plugin_astrhe"
    meta.activated = True
    for handler in registry:
        handler.handler = getattr(plugin, handler.handler_name)
    monkeypatch.setattr(native_session, "record_agent_stats", AsyncMock())
    monkeypatch.setattr(native_session.Metric, "upload", AsyncMock())
    monkeypatch.setitem(trace_module.astrbot_config, "trace_enable", False)
    monkeypatch.setattr(
        trace_module, "get_trace_store", lambda *_: NS(append=lambda *a, **kw: True)
    )
    monkeypatch.setattr(trace_module, "_get_log_broker", lambda: None)
    rt = plugin.runtime
    await rt.storage.open()
    compat = rt.wakeup_compat
    assert compat.install()
    scheduler = PipelineScheduler(
        PipelineContext(
            config, NS(context=ctx), "test", agent_runtimes={"astrbot": native}
        )
    )
    await scheduler.initialize()
    bus = EventBus(
        queue, {"test": scheduler}, NS(get_conf_info=lambda _: {"id": "test"})
    )
    dispatcher = asyncio.create_task(bus.dispatch())

    async def drain():
        async with asyncio.timeout(12):
            while (
                not queue.empty()
                or bus._pending_tasks
                or compat._observer_tasks
                or compat._watcher_tasks
            ):
                await asyncio.sleep(0)
                await asyncio.gather(
                    *list(bus._pending_tasks),
                    *list(compat._observer_tasks),
                    *list(compat._watcher_tasks),
                )

    async def add(*, once=False):
        from datetime import datetime, timedelta, timezone

        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        aid = await rt.storage.alarms.add_alarm(
            target_umo=UMO,
            origin="day",
            creator_id="10001",
            run_at=future.timestamp() if once else None,
            cron_expression="" if once else "0 9 * * *",
        )
        job = await cron.add_active_job(
            name="测试安排",
            payload={
                "session": UMO,
                "sender_id": "10001",
                "note": CARD,
                "astrhe_alarm_id": str(aid),
            },
            cron_expression=None if once else "0 9 * * *",
            run_at=future if once else None,
            run_once=once,
            enabled=False,
        )
        await rt.storage.alarms.bind_cron_job(aid, job.job_id)
        return aid, job

    def chat(umo=OTHER, text="你好", request=None):
        session = MessageSession.from_str(umo)
        msg = AstrBotMessage()
        msg.type = session.message_type
        msg.session_id = session.session_id
        msg.message_id = "offline"
        msg.self_id = "90009"
        msg.sender = MessageMember(user_id="40004", nickname="用户")
        msg.message = [Plain(text)]
        msg.message_str = text
        msg.raw_message = {}
        e = AstrMessageEvent(text, msg, metadata, session.session_id)
        e.context_obj = ctx
        e.is_at_or_wake_command = True
        if request is not None:
            e.set_extra("provider_request", request)
        queue.put_nowait(e)
        return e

    control.__dict__.update(
        ctx=ctx,
        cron=cron,
        native=native,
        rt=rt,
        compat=compat,
        bus=bus,
        drain=drain,
        add=add,
        chat=chat,
    )
    try:
        yield control
    finally:
        dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        await compat.uninstall()
        await native.close()
        await cron.shutdown()
        await rt.storage.close()
        await db.engine.dispose()


def send_to(target, call_id):
    return LLMResponse(
        role="assistant",
        tools_call_name=["send_message_to_user"],
        tools_call_args=[
            {
                "session": target,
                "messages": [{"type": "plain", "text": "同一条测试原文"}],
            }
        ],
        tools_call_ids=[call_id],
    )


def test_repeat_and_receipts_through_registered_hooks(tmp_path, monkeypatch):
    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            aid, job = await h.add()
            repo = h.rt.storage.alarms
            for i, target in enumerate((UMO, OTHER, OTHER)):
                h.responses.append(send_to(target, f"call-{i}"))
                await h.cron.run_job_now(job.job_id)
                await h.drain()
            events = await repo.events(aid)
            assert [e["kind"] for e in events] == [
                "dispatched",
                "fired",
                "send_ok",
                "done",
            ] * 3
            for n in (0, 1, 3):
                ids = [json.loads(e["detail"])["exec"] for e in events[n::4]]
                assert len(set(ids)) == 3
                assert ids == [json.loads(e["detail"])["exec"] for e in events[::4]]
            assert len(h.calls) == 6 and CARD in str(h.calls[0])
            cid = await h.mgr.get_curr_conversation_id(UMO)
            conv = await h.db.get_conversation_by_id(cid)
            assert CARD not in str(conv.content)
            assert (await h.rt.storage.sent_receipts.get(1))["status"] == "saved"
            assert not await h.mgr.get_curr_conversation_id(OTHER)
            await wake_main_agent(
                h.ctx,
                umo=OTHER,
                user_prompt="外部任务",
                source="plugin",
                source_id="external",
            )
            await h.drain()
            assert CARD not in str(h.calls[-1]) and "同一条测试原文" not in str(
                h.calls[-1]
            )
            h.chat(request=ProviderRequest(prompt="独立上下文"))
            await h.drain()
            assert "同一条测试原文" not in str(h.calls[-1])
            h.chat()
            await h.drain()
            assert "astrhe-sent#2 astrhe-sent#3" in str(h.calls[-1])
            assert await h.rt.storage.sent_receipts.count("saved") == 3
            dst = await h.db.get_conversation_by_id(
                await h.mgr.get_curr_conversation_id(OTHER)
            )
            assert str(dst.content).count("astrhe-sent#2") == 1
            assert [e["kind"] for e in await repo.events(aid)] == [
                e["kind"] for e in events
            ]

    arun(scenario())


def test_one_shot_queue_cancel_and_reload(tmp_path, monkeypatch):
    from functools import wraps
    from astrbot.core.utils.session_waiter import FILTERS, USER_SESSIONS
    from astrhe.wakeup_compat import WakeupCompat

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            once, job = await h.add(once=True)
            await asyncio.gather(
                h.cron.run_job_now(job.job_id), h.cron.run_job_now(job.job_id)
            )
            await h.drain()
            assert [e["kind"] for e in await h.rt.storage.alarms.events(once)] == [
                "dispatched",
                "fired",
                "done",
            ]
            entered = asyncio.Event()
            release = asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()
                return LLMResponse(role="assistant", completion_text="普通聊天结束")

            h.responses.append(slow)
            h.chat(
                UMO,
                request=ProviderRequest(
                    prompt="普通聊天",
                    conversation=await h.mgr.get_conversation(
                        UMO, await h.mgr.get_curr_conversation_id(UMO)
                    ),
                ),
            )
            await asyncio.wait_for(entered.wait(), 5)
            aid, repeat = await h.add()
            try:
                await h.cron.run_job_now(repeat.job_id)
                async with asyncio.timeout(5):
                    while not any("turn" in i for i in h.compat._execs.values()):
                        await asyncio.sleep(0.002)
                session = await h.native.open_session(UMO)
                inner = session.run_turn

                @wraps(inner)
                async def outer(input, *, admit=None):
                    return await inner(input, admit=admit)

                session.run_turn = outer
                await asyncio.wait_for(h.compat.uninstall(), 3)
                assert session.run_turn is outer and not release.is_set()
                assert (await h.rt.storage.alarms.get_alarm(aid))["status"] == "failed"
            finally:
                release.set()
                await h.drain()
            fresh = WakeupCompat(
                context=h.ctx,
                storage=h.rt.storage,
                access=h.rt.access,
                alarm_tracker=h.rt.alarm_tracker,
                logger=h.rt.logger,
            )
            assert fresh.install()
            await fresh.park_jobs(restore=True)
            try:
                assert (await h.cron.db.get_cron_job(repeat.job_id)).payload[
                    "session"
                ] == UMO
                await h.cron.run_job_now(repeat.job_id)
                await h.drain()
                await asyncio.gather(*fresh._observer_tasks)
                assert (await h.rt.storage.alarms.get_alarm(aid))["status"] == "done"
                from datetime import datetime, timezone

                await h.cron._run_active_agent_job(repeat, datetime.now(timezone.utc))
                # 派发入口返回时立刻取出事件，模拟总线取出后尚未开始流水线。
                e = h.queue.get_nowait()
                assert CARD not in e.message_str
                await fresh.uninstall()
                assert e.is_stopped()
                from astrbot.core.utils.session_waiter import SessionWaiter

                f = NS(filter=lambda _: UMO)
                monkeypatch.setitem(
                    USER_SESSIONS,
                    UMO,
                    NS(
                        session_controller=NS(
                            future=asyncio.get_running_loop().create_future()
                        )
                    ),
                )
                trigger = AsyncMock()
                monkeypatch.setattr(SessionWaiter, "trigger", trigger)
                FILTERS.append(f)
                try:
                    n = len(h.calls)
                    h.queue.put_nowait(e)
                    await h.drain()
                    assert len(h.calls) == n
                    trigger.assert_not_awaited()
                finally:
                    if f in FILTERS:
                        FILTERS.remove(f)
                assert (await h.cron.db.get_cron_job(repeat.job_id)).payload.get(
                    "session"
                ) is None
            finally:
                if fresh.installed:
                    await fresh.uninstall()

    arun(scenario())


def test_reset_new_and_writer_conflict(tmp_path, monkeypatch):
    from astrbot.core.db.conversation import ConversationConflictError
    from tests.test_sent_receipts import _send_call
    from tests.conftest import FakeEvent

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            cid = await h.mgr.new_conversation(OTHER, "target")
            commands = ConversationCommands(h.ctx)
            message = MagicMock()
            message.unified_msg_origin = OTHER
            message.get_platform_id.return_value = "target"

            async def send(text):
                await h.rt.sent_tracker.on_tool_respond(
                    *_send_call(FakeEvent(umo=UMO), OTHER, text)
                )

            await send("重置前")
            await commands.reset(message)
            assert await h.mgr.get_curr_conversation_id(OTHER) == cid
            h.chat()
            await h.drain()
            assert "重置前" not in str(h.calls[-1])
            await send("旧对话专用")
            await commands.new_conv(message)
            assert await h.mgr.get_curr_conversation_id(OTHER) != cid
            h.chat()
            await h.drain()
            assert "旧对话专用" not in str(h.calls[-1])
            await h.mgr.switch_conversation(OTHER, cid)
            stale = await h.mgr.create_event_writer(cid)
            h.chat()
            await h.drain()
            assert "旧对话专用" in str(h.calls[-1])
            assert await h.rt.storage.sent_receipts.count("saved") == 1
            with pytest.raises(ConversationConflictError):
                await h.db.conversation_store.append(
                    cid,
                    [
                        {
                            "type": "message.appended",
                            "payload": {
                                "message": {"role": "user", "content": "过期写入"}
                            },
                        }
                    ],
                    expected_leaf=stale.leaf_event_id,
                )
            await send("再次重置")
            await commands.reset(message)
            h.chat()
            await h.drain()
            assert "再次重置" not in str(h.calls[-1]) and "旧对话专用" not in str(
                h.calls[-1]
            )

    arun(scenario())


def test_unload_drains_only_own_preloaded_cron(tmp_path, monkeypatch):
    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            _, own = await h.add()
            external = await h.cron.add_active_job(
                name="外部安排",
                cron_expression="0 9 * * *",
                payload={"session": OTHER, "note": "外部自己的提示"},
                enabled=False,
            )
            entered = {job.job_id: asyncio.Event() for job in (own, external)}
            release = asyncio.Event()
            update = h.db.update_cron_job

            async def at_snapshot(job_id, **changes):
                if (
                    job_id in entered
                    and changes.get("status") == "running"
                    and not release.is_set()
                ):
                    entered[job_id].set()
                    await release.wait()
                return await update(job_id, **changes)

            monkeypatch.setattr(h.db, "update_cron_job", at_snapshot)
            tasks = [
                asyncio.create_task(h.cron.run_job_now(job.job_id))
                for job in (own, external)
            ]
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(e.wait() for e in entered.values())), 5
                )
                await asyncio.wait_for(h.compat.uninstall(), 5)
                assert not tasks[1].done()
                assert "session" not in (await h.db.get_cron_job(own.job_id)).payload
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)
                await h.drain()
            await h.cron.run_job_now(own.job_id)
            await h.drain()
            assert len(h.calls) == 1 and "外部自己的提示" in str(h.calls[0])
            assert CARD not in str(
                h.calls
            ) and not await h.mgr.get_curr_conversation_id(UMO)
            assert (await h.db.get_cron_job(own.job_id)).status == "failed"
            with pytest.raises(ValueError, match="已暂停"):
                await h.rt.schedule_impl._create_alarm_job(
                    name="卸载前的旧工具",
                    cron_expression="0 9 * * *",
                    payload={"session": UMO, "astrhe_alarm_id": "1"},
                )

    arun(scenario())


def test_missing_native_runtime_parks_only_plugin_alarms(tmp_path, monkeypatch):
    from tests.conftest import FakeEvent
    from tests.test_sent_receipts import _send_call

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            _, own = await h.add()
            await h.compat.uninstall()
            await h.rt.storage.close()
            h.ctx.agent._runtimes.clear()
            await h.rt.initialize()
            try:
                assert not h.compat.installed and h.rt._tools_registered
                cid = await h.mgr.new_conversation(OTHER, "target")
                await h.rt.sent_tracker.on_tool_respond(
                    *_send_call(FakeEvent(umo=UMO), OTHER, "降级期间重置前")
                )
                receipts = h.rt.storage.sent_receipts
                assert len(await receipts.pending_for(OTHER)) == 1
                msg = MagicMock()
                msg.unified_msg_origin = OTHER
                await ConversationCommands(h.ctx).reset(msg)
                assert await h.mgr.get_curr_conversation_id(OTHER) == cid
                assert not await receipts.pending_for(OTHER)
                assert await receipts.reset_cutoff(cid) > 0
                with pytest.raises(ValueError, match="已暂停"):
                    await h.rt.schedule_impl._create_alarm_job(
                        name="新闹钟",
                        cron_expression="0 9 * * *",
                        payload={"session": UMO, "astrhe_alarm_id": "2"},
                    )
                await h.cron.run_job_now(own.job_id)
                assert (
                    "不支持当前执行环境"
                    in (await h.db.get_cron_job(own.job_id)).last_error
                )
                other = await h.cron.add_active_job(
                    name="其他任务",
                    cron_expression="0 9 * * *",
                    payload={"session": OTHER, "note": "外部"},
                    enabled=False,
                )
                await h.cron.run_job_now(other.job_id)
                await h.drain()
                assert len(h.calls) == 1 and CARD not in str(h.calls)
            finally:
                await h.rt.scheduler.stop()

    arun(scenario())


def test_cold_restore_waits_for_host_scheduler(tmp_path, monkeypatch):
    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            aid, job = await h.add(once=True)
            # 预置相当于上次关机前已经启用、随后由插件正常停放的一次性安排。
            await h.db.update_cron_job(job.job_id, enabled=True)
            await h.compat.uninstall()
            cold = CronJobManager(h.db)
            monkeypatch.setattr(h.ctx, "cron_manager", cold)
            try:
                assert h.compat.install() and not hasattr(cold, "ctx")
                await h.compat.park_jobs(restore=True)
                row = await h.db.get_cron_job(job.job_id)
                assert row.enabled and row.payload["session"] == UMO
                assert not cold.scheduler.running
                assert cold.get_next_run_time(job.job_id) is None
                await cold.start(h.ctx)
                assert cold.get_next_run_time(job.job_id) is not None
                await cold.run_job_now(job.job_id)
                await h.drain()
                assert len(h.calls) == 1
                assert (await h.rt.storage.alarms.get_alarm(aid))["status"] == "done"
            finally:
                await h.compat.uninstall()
                await cold.shutdown()

    arun(scenario())


def test_last_wakeup_on_stopped_platform_settles_without_next_event(
    tmp_path, monkeypatch
):
    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            aid, job = await h.add(once=True)
            await h.bus.stop_platform("target")
            await h.cron.run_job_now(job.job_id)
            await h.drain()
            repo = h.rt.storage.alarms
            assert not h.calls and h.ctx.send_message.await_count == 0
            assert not h.rt.alarm_tracker.dispatch_active(job.job_id)
            assert [e["kind"] for e in await repo.events(aid)] == [
                "dispatched",
                "interrupted",
            ]
            assert (await repo.get_alarm(aid))["status"] == "failed"
            second, missing = await h.add(once=True)
            monkeypatch.setattr(h.ctx, "get_platform_inst", lambda _: None)
            await h.cron.run_job_now(missing.job_id)
            assert (await repo.get_alarm(second))["status"] == "failed"
            assert "未找到目标平台" in (await repo.events(second))[-1]["detail"]

    arun(scenario())


def test_active_writer_conflict_preserves_pending_receipt(tmp_path, monkeypatch):
    from tests.test_sent_receipts import _send_call
    from tests.conftest import FakeEvent

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            cid = await h.mgr.new_conversation(OTHER, "target")
            repo = h.rt.storage.sent_receipts
            await h.rt.sent_tracker.on_tool_respond(
                *_send_call(FakeEvent(umo=UMO), OTHER, "等宿主确认的消息")
            )
            from astrbot.core.agent.conversation_events import ConversationEventWriter

            append = h.db.conversation_store.append
            raced = []

            async def write_first(cid_arg, drafts, **kw):
                if (
                    not raced
                    and "astrhe-sent#1" in str(drafts)
                    and "expected_leaf" in kw
                ):
                    raced.append(True)
                    await h.mgr.append_message("另一位 writer 的消息", cid=cid)
                return await append(cid_arg, drafts, **kw)

            assert ConversationEventWriter is type(await h.mgr.create_event_writer(cid))
            monkeypatch.setattr(h.db.conversation_store, "append", write_first)
            h.chat()
            await h.drain()
            assert raced and (await repo.get(1))["status"] == "pending"
            assert "另一位 writer 的消息" in str(
                (await h.db.get_conversation_by_id(cid)).content
            )
            h.chat()
            await h.drain()
            assert (await repo.get(1))["status"] == "saved"
            saved = str((await h.db.get_conversation_by_id(cid)).content)
            assert saved.count("astrhe-sent#1") == 1 and "另一位 writer 的消息" in saved

    arun(scenario())


def test_rebind_with_same_platform_id_and_cancelled_repeat(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from astrbot.dashboard.services.cron_service import CronService

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            repo = h.rt.storage.alarms
            panel = CronService(NS(cron_manager=h.cron))
            aid, job = await h.add(once=True)
            entered = asyncio.Event()
            release = asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()
                return LLMResponse(role="assistant", completion_text="完成")

            h.responses.append(slow)
            await h.cron.run_job_now(job.job_id)
            await asyncio.wait_for(entered.wait(), 5)
            try:
                await panel.update_job(
                    job.job_id,
                    {
                        "run_at": (
                            datetime.now(timezone.utc) + timedelta(days=1)
                        ).isoformat(),
                        "enabled": True,
                    },
                )
            finally:
                release.set()
            await h.drain()
            assert (await repo.get_alarm(aid))["status"] == "scheduled"
            await h.cron.run_job_now(job.job_id)
            await h.drain()
            assert [e["kind"] for e in await repo.events(aid)].count("done") == 2
            rid, repeat = await h.add()
            entered.clear()
            release.clear()
            h.responses.append(slow)
            await h.cron.run_job_now(repeat.job_id)
            await asyncio.wait_for(entered.wait(), 5)
            try:
                event = next(
                    i["event"]()
                    for i in h.compat._execs.values()
                    if i["alarm_id"] == rid
                )
                assert "已取消" in await h.rt.schedule_impl.run(
                    event, "cancel", alarm_id=rid
                )
            finally:
                release.set()
            await h.drain()
            assert (await repo.get_alarm(rid))["status"] == "cancelled"
            assert not any(a["id"] == rid for a in await repo.live_alarms())

    arun(scenario())


def test_gate_rechecked_at_queue_and_before_turn(tmp_path, monkeypatch):
    from datetime import datetime, timezone

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            aid, job = await h.add()
            repo = h.rt.storage.alarms
            h.config["plugin_set"] = []
            await h.cron.run_job_now(job.job_id)
            await h.drain()
            assert not h.calls and not h.compat._execs
            h.config["plugin_set"] = ["*"]
            h.config["provider_settings"]["enable"] = False
            await h.cron.run_job_now(job.job_id)
            await h.drain()
            assert [e["kind"] for e in await repo.events(aid)][
                -1
            ] == "interrupted" and not h.compat._execs
            h.config["provider_settings"]["enable"] = True
            entered = asyncio.Event()
            release = asyncio.Event()

            async def slow():
                entered.set()
                await release.wait()
                return LLMResponse(role="assistant", completion_text="完成")

            h.responses.append(slow)
            await h.cron.run_job_now(job.job_id)
            await asyncio.wait_for(entered.wait(), 5)
            try:
                await h.cron._run_active_agent_job(job, datetime.now(timezone.utc))
                e = h.queue.get_nowait()
                assert CARD not in str(e.get_extra()) + e.message_str
                h.config["plugin_set"] = []
                h.queue.put_nowait(e)
                async with asyncio.timeout(5):
                    while not all("turn" in i for i in h.compat._execs.values()):
                        await asyncio.sleep(0.002)
                h.config["plugin_set"] = ["*"]
                h.config["admins_id"] = []
            finally:
                release.set()
            await h.drain()
            assert e.role == "member" and e.plugins_name is None
            assert (
                CARD in str(h.calls[-1])
                and (await repo.get_alarm(aid))["status"] == "done"
            )
            cid = await h.mgr.get_curr_conversation_id(UMO)
            assert CARD not in str((await h.db.get_conversation_by_id(cid)).content)

    arun(scenario())


def test_panel_cancel_and_edit_while_host_holds_alarm(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from astrbot.dashboard.services.cron_service import CronService

    async def scenario():
        async with harness(tmp_path, monkeypatch) as h:
            panel = CronService(NS(cron_manager=h.cron))
            aid, repeat = await h.add()
            await h.cron._run_active_agent_job(repeat, datetime.now(timezone.utc))
            queued = h.queue.get_nowait()
            await panel.delete_job(repeat.job_id)
            h.queue.put_nowait(queued)
            await h.drain()
            assert (
                not h.calls
                and (await h.rt.storage.alarms.get_alarm(aid))["status"] == "cancelled"
            )
            aid, once = await h.add(once=True)
            await h.cron.update_job(once.job_id, enabled=True)
            entered = asyncio.Event()
            release = asyncio.Event()
            update = h.db.update_cron_job

            async def pause(job_id, **kw):
                if job_id == once.job_id and kw.get("status") == "running":
                    entered.set()
                    await release.wait()
                return await update(job_id, **kw)

            monkeypatch.setattr(h.db, "update_cron_job", pause)
            run = asyncio.create_task(h.cron._run_job(once.job_id))
            try:
                await asyncio.wait_for(entered.wait(), 4)
                await asyncio.wait_for(
                    panel.update_job(
                        once.job_id,
                        {
                            "run_at": (
                                datetime.now(timezone.utc) + timedelta(days=1)
                            ).isoformat()
                        },
                    ),
                    4,
                )
            finally:
                release.set()
                await asyncio.gather(run, return_exceptions=True)
                await h.drain()
            assert not h.calls and (await h.db.get_cron_job(once.job_id)).enabled
            await h.cron.run_job_now(once.job_id)
            await h.drain()
            assert (
                len(h.calls) == 1
                and (await h.rt.storage.alarms.get_alarm(aid))["status"] == "done"
            )
            due_id, due = await h.add(once=True)
            await h.cron.update_job(due.job_id, enabled=True)
            await h.cron._run_job(due.job_id)
            await h.drain()
            assert await h.db.get_cron_job(due.job_id) is None
            assert (await h.rt.storage.alarms.get_alarm(due_id))["status"] == "done"

    arun(scenario())
