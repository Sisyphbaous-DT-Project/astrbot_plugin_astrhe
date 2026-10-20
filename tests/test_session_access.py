"""会话名单：判定规则，以及记录、补拉、复盘、闹钟、注入、发送留存、QQ 小工具各入口。"""

from __future__ import annotations

import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from astrhe import access as access_mod
from astrhe.access import SessionAccess
from astrhe.alarm_tracker import AlarmTracker
from astrhe.backfill import Backfiller
from astrhe.injector import Injector
from astrhe.platform import PlatformHelper
from astrhe.qq.action_tool import QQActionTool
from astrhe.qq.client import QQClient
from astrhe.qq.lookup_tool import UNLISTED_GROUP, QQLookupTool
from astrhe.qq.rate_limit import RateLimiter
from astrhe.recorder import Recorder
from astrhe.sent_tracker import SentTracker
from astrhe.tools.draft_tools import DraftTools
from astrhe.tools.reader_tools import ReaderTools
from astrhe.tools.schedule_tool import ScheduleToolImpl
from astrhe.tools.secrecy import PresentResolver
from astrhe.timeutil import utc_now
from tests.conftest import (
    FakeContext,
    FakeEvent,
    FakeLogger,
    FakeMessageObj,
    Plain,
    arun,
    make_storage,
)
from tests.test_night_pipeline import (
    _build_pipeline,
    _make_handler,
    _planner_handler,
    _reporter_handler,
)

TZ = ZoneInfo("Asia/Shanghai")
ALLOWED = "default:GroupMessage:20002"
UNLISTED = "default:GroupMessage:30003"
ADMIN_PRIVATE = "default:FriendMessage:10001"
STRANGER_PRIVATE = "default:FriendMessage:40004"


def _astrbot_config(enabled=True, ids=()):
    return {
        "timezone": "Asia/Shanghai",
        "admins_id": ["10001"],
        "platform_settings": {
            "enable_id_white_list": enabled,
            "id_whitelist": list(ids),
        },
    }


class RoutedContext(FakeContext):
    """不同会话用不同 AstrBot 配置文件。"""

    def __init__(self, routes, default, **kwargs):
        super().__init__(config=default, **kwargs)
        self.routes = routes

    def get_config(self, umo=None):
        return self.routes.get(umo, self._config) if umo else self._config


def _access(ctx, plugin_config=None, logger=None):
    return SessionAccess(
        ctx,
        plugin_config or {},
        lambda: ["10001"],
        logger or FakeLogger(),
        platform_id_getter=lambda: "default",
    )


def _blacklisted_context():
    """AstrBot 不限制，插件黑名单写了 30003：两个群都在、都有人说话。"""
    ctx = FakeContext(
        api_actions={
            "get_group_list": [{"group_id": 20002}, {"group_id": 30003}],
            "get_login_info": {"user_id": 99999},
        },
        config=_astrbot_config(enabled=False),
    )
    return ctx, _access(ctx, {"session_blacklist": ["30003"]})


# ---------------- 判定规则 ----------------


def test_follows_astrbot_whitelist_when_plugin_whitelist_empty():
    ctx = FakeContext(config=_astrbot_config(ids=["20002"]))
    access = _access(ctx)
    assert access.list_allows(ALLOWED)
    assert not access.list_allows(UNLISTED)
    # 私聊只认完整会话 ID（与 AstrBot 一致）；管理员私聊始终放行
    assert not access.list_allows(STRANGER_PRIVATE)
    assert access.list_allows(ADMIN_PRIVATE)
    # 面板网页对话与 AstrBot 一样豁免白名单
    assert access.list_allows("webchat:FriendMessage:webchat!astrbot!abc")
    # AstrBot 关了白名单或名单为空：不限制
    assert _access(
        FakeContext(config=_astrbot_config(enabled=False, ids=["20002"]))
    ).list_allows(UNLISTED)
    assert _access(FakeContext(config=_astrbot_config(ids=[]))).list_allows(UNLISTED)


def test_astrbot_whitelist_uses_each_session_config():
    ctx = RoutedContext(
        {UNLISTED: _astrbot_config(ids=["default:GroupMessage:20002"])},
        _astrbot_config(enabled=False),
    )
    access = _access(ctx)
    assert access.list_allows(ALLOWED)  # 默认配置没开白名单
    assert not access.list_allows(UNLISTED)  # 这个群分到的配置开了白名单且没列它


def test_plugin_whitelist_overrides_astrbot_and_blacklist_wins():
    ctx = FakeContext(config=_astrbot_config(ids=["20002"]))
    access = _access(
        ctx,
        {
            # 会话隔离写法的会话 ID 也按群号对齐
            "session_whitelist": ["default:GroupMessage:55555_30003", STRANGER_PRIVATE],
            "session_blacklist": [ADMIN_PRIVATE],
        },
    )
    assert access.list_allows(UNLISTED)  # AstrBot 拦着，但插件白名单写了它
    assert not access.list_allows(ALLOWED)  # AstrBot 放行，但插件白名单没写它
    assert access.list_allows(STRANGER_PRIVATE)
    assert not access.list_allows(ADMIN_PRIVATE)  # 黑名单连管理员私聊也拦


def test_astrbot_read_failure_falls_back_to_plugin_lists_with_one_warning():
    ctx = FakeContext(config={"admins_id": ["10001"]})  # 没有 platform_settings
    logger = FakeLogger()
    access = _access(ctx, {"session_blacklist": ["30003"]}, logger)
    assert access.list_allows(ALLOWED)
    assert access.list_allows("default:GroupMessage:20003")
    assert not access.list_allows(UNLISTED)
    assert len(logger.warnings) == 1


def test_astrbot_session_management_is_respected(monkeypatch):
    # 真实 AstrBot 里能找到会话管理模块
    assert access_mod.SessionServiceManager is not None
    assert access_mod.SessionPluginManager is not None
    calls = []

    class Service:
        @staticmethod
        async def is_session_enabled(umo):
            return umo != "default:GroupMessage:60006"

    class Plugins:
        @staticmethod
        async def is_plugin_enabled_for_session(umo, name):
            calls.append((umo, name))
            return umo != "default:GroupMessage:70007"

    monkeypatch.setattr(access_mod, "SessionServiceManager", Service)
    monkeypatch.setattr(access_mod, "SessionPluginManager", Plugins)
    access = _access(FakeContext(config=_astrbot_config(enabled=False)))
    assert arun(access.allows(ALLOWED))
    assert not arun(access.allows("default:GroupMessage:60006"))
    assert not arun(access.allows("default:GroupMessage:70007"))
    assert ("default:GroupMessage:70007", "astrbot_plugin_astrhe") in calls
    # 会话管理只看完整判断；逐条消息的名单判断不查它
    assert access.list_allows("default:GroupMessage:60006")


# ---------------- 记录与补拉 ----------------


def test_recorder_skips_unlisted_sessions(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx = FakeContext(config=_astrbot_config(ids=["20002"]))
            recorder = Recorder(s, FakeLogger(), lambda: True, _access(ctx))
            for gid in ("20002", "30003"):
                # 管理员在名单外的群里说话 AstrBot 会放行，插件也不记
                await recorder.record_event(
                    FakeEvent(
                        umo=f"default:GroupMessage:{gid}",
                        message_obj=FakeMessageObj(session_id=gid, message_id=gid),
                    )
                )
            assert await s.ledger.list_session_umos(0) == [ALLOWED]
        finally:
            await s.close()

    arun(main())


def test_backfill_candidates_drop_unlisted_groups(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            await s.ledger.add_message(
                umo=UNLISTED,
                platform_id="default",
                message_type="GroupMessage",
                session_id="30003",
                sender_id="10001",
                text="以前记下的",
                ts=utc_now() - 60,
                platform_message_id="8001",
            )
            backfiller = Backfiller(
                s, PlatformHelper(ctx, FakeLogger()), FakeLogger(), access
            )
            groups, _ = await backfiller.candidate_sessions(0)
            assert groups == ["20002"]
        finally:
            await s.close()

    arun(main())


def test_pipeline_reviews_and_backfills_only_listed_sessions(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            for umo, sid in ((ALLOWED, "20002"), (UNLISTED, "30003")):
                await s.ledger.add_message(
                    umo=umo,
                    platform_id="default",
                    message_type="GroupMessage",
                    session_id=sid,
                    sender_id="10001",
                    text="今天聊了点事",
                    ts=utc_now() - 3600,
                    platform_message_id=f"m{sid}",
                )
            # 只有一个会话该派汇报员：多派一个就会吃掉后面主 agent 的脚本
            ctx.tool_loop_agent.handlers = [
                _make_handler(_reporter_handler),
                _make_handler(_planner_handler),
                _make_handler(lambda kw: "结论：通过"),
            ]
            pipeline = _build_pipeline(s, ctx, FakeLogger())
            pipeline.access = access
            pipeline.backfiller.access = access
            result = await pipeline.run("manual")
            assert result["status"] == "committed", result
            run = await s.runs.get_run(1)
            assert json.loads(run["stats_json"])["sessions"] == 1
            histories = [p for a, p in ctx.api.calls if a == "get_group_msg_history"]
            assert histories and all(p["group_id"] == 20002 for p in histories)
        finally:
            await s.close()

    arun(main())


# ---------------- 闹钟 ----------------


def _qq(ctx):
    return QQClient(PlatformHelper(ctx, FakeLogger()))


ALARM_CARD = {
    "run_at": "2099-01-02 12:00",
    "background": "约好了",
    "purpose": "提醒",
    "how": "发一句",
}


def test_night_alarm_draft_refuses_unlisted_target(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()

            async def noop(_):
                return ""

            tools = DraftTools(
                s,
                1,
                noop,
                noop,
                qq_client=_qq(ctx),
                tz_getter=lambda: TZ,
                access=access,
            ).build()
            tool = next(t for t in tools if t.name == "astrhe_draft_alarm")
            text = await tool.run(FakeEvent(), target_umo=UNLISTED, **ALARM_CARD)
            assert "草稿未保存" in text and "不在 AstrHe 的会话名单里" in text
            assert await s.runs.get_drafts(1) == []
        finally:
            await s.close()

    arun(main())


def test_day_schedule_admin_allowed_with_warning_member_refused(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            qq = _qq(ctx)
            impl = ScheduleToolImpl(
                s,
                ctx,
                qq,
                RateLimiter(s, lambda: TZ),
                lambda: TZ,
                PresentResolver(s, qq),
                lambda: ["10001"],
                access=access,
            )
            text = await impl.run(
                FakeEvent(umo=ADMIN_PRIVATE, role="admin"),
                action="create",
                target_type="group",
                target_id="30003",
                **ALARM_CARD,
            )
            assert "定好了" in text and "不在 AstrHe 的会话名单里" in text
            text = await impl.run(
                FakeEvent(umo=UNLISTED, sender_id="40004"),
                action="create",
                target_type="current",
                **ALARM_CARD,
            )
            assert text.startswith("闹钟未创建") and "不在 AstrHe 的会话名单里" in text
            assert len(ctx.cron_manager.added) == 1
        finally:
            await s.close()

    arun(main())


def test_reconcile_cancels_unlisted_alarms_except_admin_day_ones(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            ids = {}
            for key, umo, origin, creator in (
                ("night", UNLISTED, "night", "astrhe"),
                ("member", UNLISTED, "day", "40004"),
                ("admin", UNLISTED, "day", "10001"),
                ("listed", ALLOWED, "night", "astrhe"),
            ):
                job = await ctx.cron_manager.add_active_job(name=key, payload={})
                ids[key] = await s.alarms.add_alarm(
                    cron_job_id=job.job_id,
                    target_umo=umo,
                    creator_id=creator,
                    origin=origin,
                    run_at=utc_now() + 86400,
                )
            tracker = AlarmTracker(s, ctx, lambda: TZ, FakeLogger())
            dry_text = await tracker.cancel_unlisted(access, admins=["10001"], dry=True)
            assert "演练" in dry_text and not ctx.cron_manager.deleted

            text = await tracker.cancel_unlisted(access, admins=["10001"])
            status = {
                k: (await s.alarms.get_alarm(v))["status"] for k, v in ids.items()
            }
            assert status == {
                "night": "cancelled",
                "member": "cancelled",
                "admin": "scheduled",
                "listed": "scheduled",
            }
            assert len(ctx.cron_manager.deleted) == 2
            assert f"#{ids['admin']}" in text and "保留" in text
        finally:
            await s.close()

    arun(main())


# ---------------- 夜间读取、白天注入、发送留存 ----------------


def test_reader_tools_hide_unlisted_records(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            for umo, sid in ((ALLOWED, "20002"), (UNLISTED, "30003")):
                await s.ledger.add_message(
                    umo=umo,
                    platform_id="default",
                    message_type="GroupMessage",
                    session_id=sid,
                    sender_id="10001",
                    text="蛋糕的事",
                    ts=utc_now() - 60,
                    platform_message_id=f"m{sid}",
                )
            reader = ReaderTools(s, ctx, lambda: TZ, lambda: (0, 2e10), access=access)
            event = FakeEvent()
            listed = await reader._list_sessions(event)
            assert "20002" in listed and "30003" not in listed
            assert "不在 AstrHe 的会话名单里" in await reader._read_messages(
                event, umo=UNLISTED
            )
            found = await reader._search_messages(event, keyword="蛋糕")
            assert "20002" in found and "30003" not in found
            assert (
                await reader._message_context(event, message_id="m30003")
                == "账本里找不到这条消息"
            )
        finally:
            await s.close()

    arun(main())


def test_injector_leaves_unlisted_sessions_alone(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            await s.plans.add_todo(content="带蛋糕", visible_umos=[ALLOWED, UNLISTED])
            injector = Injector(
                s,
                lambda: TZ,
                PresentResolver(s, _qq(ctx)),
                lambda: True,
                FakeLogger(),
                access=access,
            )
            names = [
                "future_task",
                "astrhe_schedule",
                "astrhe_todo_update",
                "astrhe_qq_act",
            ]

            def request():
                return SimpleNamespace(
                    func_tool=SimpleNamespace(
                        tools=[SimpleNamespace(name=n) for n in names]
                    ),
                    extra_user_content_parts=[],
                )

            req = request()
            await injector.on_llm_request(FakeEvent(umo=UNLISTED), req)
            assert [t.name for t in req.func_tool.tools] == [
                "future_task",
                "astrhe_qq_act",
            ]
            assert req.extra_user_content_parts == []

            req = request()
            await injector.on_llm_request(FakeEvent(umo=ALLOWED), req)
            assert "future_task" not in [t.name for t in req.func_tool.tools]
            assert "带蛋糕" in req.extra_user_content_parts[0].text
        finally:
            await s.close()

    arun(main())


def test_sent_tracker_skips_unlisted_targets(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()
            tracker = SentTracker(
                s,
                ctx,
                PlatformHelper(ctx, FakeLogger()),
                lambda: TZ,
                FakeLogger(),
                lambda: True,
                access=access,
            )
            for umo in (UNLISTED, ALLOWED):
                await tracker.on_tool_respond(
                    FakeEvent(umo=ADMIN_PRIVATE),
                    SimpleNamespace(name="send_message_to_user"),
                    {"messages": [{"type": "plain", "text": "晚上好"}]},
                    SimpleNamespace(
                        content=[SimpleNamespace(text=f"Message sent to session {umo}")]
                    ),
                )
            assert await s.ledger.list_session_umos(0) == [ALLOWED]
        finally:
            await s.close()

    arun(main())


# ---------------- QQ 小工具 ----------------


def test_qq_tools_refuse_unlisted_groups_but_not_current_one():
    ctx, access = _blacklisted_context()
    ctx.api.actions["get_group_member_list"] = [{"user_id": 50005, "card": "小明"}]
    lookup = QQLookupTool(qq_client=_qq(ctx), access=access)
    in_allowed = FakeEvent(umo=ALLOWED)
    assert (
        arun(
            lookup.run(
                in_allowed, action="find_member", group_id="30003", keyword="小明"
            )
        )
        == UNLISTED_GROUP
    )
    groups = arun(lookup.run(in_allowed, action="groups"))
    assert "20002" in groups and "30003" not in groups
    # 就在这个群里时照常可用（AstrBot 已经放行了这条消息）
    assert "50005" in arun(
        lookup.run(
            FakeEvent(umo=UNLISTED),
            action="find_member",
            group_id="30003",
            keyword="小明",
        )
    )

    act = QQActionTool(qq_client=_qq(ctx), rate_limiter=None, access=access)
    assert (
        arun(act.run(in_allowed, action="poke", user_id="50005", group_id="30003"))
        == UNLISTED_GROUP
    )


# ---------------- 终审跟进 ----------------


def test_night_virtual_event_checks_groups_on_real_platform():
    """夜间虚拟事件的平台名是 astrhe，目标群要按真实 QQ 平台拼会话 ID。"""
    from astrhe.night.silent_event import make_silent_event

    ctx = FakeContext(config=_astrbot_config(enabled=False))
    ctx.api.actions["get_group_member_list"] = [{"user_id": 50005, "card": "小明"}]
    night = make_silent_event(ctx, "")
    blocked = QQLookupTool(
        qq_client=_qq(ctx), access=_access(ctx, {"session_blacklist": [UNLISTED]})
    )
    assert arun(night_find(blocked, night, "30003")) == UNLISTED_GROUP
    allowed = QQLookupTool(
        qq_client=_qq(ctx), access=_access(ctx, {"session_whitelist": [ALLOWED]})
    )
    assert "50005" in arun(night_find(allowed, night, "20002"))


def night_find(tool, event, group_id):
    return tool.run(event, action="find_member", group_id=group_id, keyword="小明")


def test_get_msg_checks_the_session_the_message_belongs_to():
    from astrhe.qq.lookup_tool import UNLISTED_MESSAGE

    ctx, access = _blacklisted_context()
    access.config["session_blacklist"].append("default:FriendMessage:40004")
    messages = {
        1: {"message_type": "group", "group_id": 30003, "user_id": 50005},
        2: {"message_type": "group", "group_id": 20002, "user_id": 50005},
        # 私聊里 bot 自己发的：对方看 target_id
        3: {
            "message_type": "private",
            "user_id": 99999,
            "self_id": 99999,
            "target_id": 40004,
        },
    }
    ctx.api.actions["get_msg"] = lambda message_id: messages[message_id]
    lookup = QQLookupTool(qq_client=_qq(ctx), access=access)
    event = FakeEvent(umo=ADMIN_PRIVATE)
    assert arun(lookup.run(event, action="get_msg", message_id="1")) == UNLISTED_MESSAGE
    assert "20002" in arun(lookup.run(event, action="get_msg", message_id="2"))
    assert arun(lookup.run(event, action="get_msg", message_id="3")) == UNLISTED_MESSAGE
    # 就在那个群里时照常可用
    assert "30003" in arun(
        lookup.run(FakeEvent(umo=UNLISTED), action="get_msg", message_id="1")
    )


def test_unique_session_disable_is_checked_by_raw_session_id(tmp_path, monkeypatch):
    """会话隔离开启时 AstrBot 按「发言人_群号」记停用状态。"""
    disabled = {"default:GroupMessage:50005_20002"}

    class Service:
        @staticmethod
        async def is_session_enabled(umo):
            return umo not in disabled

    class Plugins:
        @staticmethod
        async def is_plugin_enabled_for_session(umo, name):
            return True

    monkeypatch.setattr(access_mod, "SessionServiceManager", Service)
    monkeypatch.setattr(access_mod, "SessionPluginManager", Plugins)
    ctx = FakeContext(config=_astrbot_config(enabled=False))
    access = _access(ctx)
    raw = "default:GroupMessage:50005_20002"
    other = "default:GroupMessage:60006_20002"
    assert arun(access.allows(ALLOWED))
    assert not arun(access.allows(raw))

    async def main():
        s = await make_storage(tmp_path)
        try:
            await s.plans.add_todo(content="带蛋糕", visible_umos=[ALLOWED])
            injector = Injector(
                s,
                lambda: TZ,
                PresentResolver(s, _qq(ctx)),
                lambda: True,
                FakeLogger(),
                access=access,
            )
            req = SimpleNamespace(
                func_tool=SimpleNamespace(tools=[SimpleNamespace(name="future_task")]),
                extra_user_content_parts=[],
            )
            await injector.on_llm_request(FakeEvent(umo=raw), req)
            assert [t.name for t in req.func_tool.tools] == ["future_task"]
            assert req.extra_user_content_parts == []
            # 对照：同一个群里没被停用的人照常注入
            await injector.on_llm_request(FakeEvent(umo=other), req)
            assert req.func_tool.tools == [] and req.extra_user_content_parts

            recorder = Recorder(s, FakeLogger(), lambda: True, access)
            event = FakeEvent(
                umo=raw, message_obj=FakeMessageObj(session_id="50005_20002")
            )
            event.set_result(SimpleNamespace(chain=[Plain("好的")]))
            await recorder.record_sent(event)
            assert await s.ledger.list_session_umos(0) == []
            event = FakeEvent(
                umo=other, message_obj=FakeMessageObj(session_id="60006_20002")
            )
            event.set_result(SimpleNamespace(chain=[Plain("好的")]))
            await recorder.record_sent(event)
            assert await s.ledger.list_session_umos(0) == [ALLOWED]
        finally:
            await s.close()

    arun(main())


def test_cancel_failure_is_reported_on_its_own_line(tmp_path):
    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx, access = _blacklisted_context()

            async def broken(job_id):
                raise RuntimeError("平台不可用")

            ctx.cron_manager.delete_job = broken
            alarm_id = await s.alarms.add_alarm(
                cron_job_id="job-x", target_umo=UNLISTED, run_at=utc_now() + 86400
            )
            text = await AlarmTracker(s, ctx, lambda: TZ, FakeLogger()).cancel_unlisted(
                access, admins=["10001"]
            )
            assert (
                "管理员" not in text and "取消失败" in text and f"#{alarm_id}" in text
            )
            assert (await s.alarms.get_alarm(alarm_id))["status"] == "scheduled"
        finally:
            await s.close()

    arun(main())


def test_runtime_wires_one_gate_everywhere_and_cancels_on_start(tmp_path, monkeypatch):
    from astrhe import runtime as runtime_mod
    from astrhe.runtime import AstrHeRuntime

    captured = {}

    class Capture:
        def __init__(self, *args, **kwargs):
            captured.setdefault(type(self).__name__, kwargs)

        def build(self):
            return []

    class ReaderCapture(Capture):
        pass

    class DraftCapture(Capture):
        pass

    class PipelineCapture(Capture):
        async def run(self, mode):
            return {"status": "dry_done"}

    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx = FakeContext(config=_astrbot_config(enabled=False))
            rt = AstrHeRuntime(
                ctx, {"enabled": True, "session_blacklist": ["30003"]}, FakeLogger()
            )
            gate = rt.access
            for part in (
                rt.recorder,
                rt.backfiller,
                rt.schedule_impl,
                rt.reader_tools,
                rt.injector,
                rt.sent_tracker,
            ):
                assert part.access is gate, part
            # 夜间才新建的组件换成记录参数的替身
            monkeypatch.setattr(runtime_mod, "ReaderTools", ReaderCapture)
            monkeypatch.setattr(runtime_mod, "DraftTools", DraftCapture)
            monkeypatch.setattr(runtime_mod, "NightPipeline", PipelineCapture)
            rt._register_tools()
            qq_tools = [
                t for t in ctx.registered_tools if t.name.startswith("astrhe_qq_")
            ]
            assert len(qq_tools) == 2 and all(t.access is gate for t in qq_tools)
            rt._internal_tools_provider("reader", run_id=1)
            rt._internal_tools_provider(
                "draft", run_id=1, request_review_cb=None, commit_cb=None
            )
            await rt.run_night("dry")
            assert captured["ReaderCapture"]["access"] is gate
            assert captured["DraftCapture"]["access"] is gate
            assert captured["PipelineCapture"]["session_access"] is gate

            # 启动时就取消名单外的闹钟，不等零点
            rt.storage = rt.alarm_tracker.storage = s
            job = await ctx.cron_manager.add_active_job(name="x", payload={})
            alarm_id = await s.alarms.add_alarm(
                cron_job_id=job.job_id, target_umo=UNLISTED, run_at=utc_now() + 86400
            )
            await rt.cancel_unlisted_alarms()
            assert (await s.alarms.get_alarm(alarm_id))["status"] == "cancelled"
            assert ctx.cron_manager.deleted == [job.job_id]
        finally:
            await s.close()

    arun(main())


def test_get_msg_self_private_without_target_id(monkeypatch):
    """NapCat 的 get_msg 返回 bot 自己发的私聊时不带 target_id，确定不了发给了谁。"""
    from astrhe.qq.lookup_tool import UNLISTED_MESSAGE

    message = {
        "message_type": "private",
        "user_id": 99999,
        "self_id": 99999,
        "raw_message": "悄悄话",
    }
    ctx = FakeContext(config=_astrbot_config(enabled=False))
    ctx.api.actions["get_msg"] = lambda message_id: message
    event = FakeEvent(umo=ADMIN_PRIVATE)
    restricted = QQLookupTool(
        qq_client=_qq(ctx),
        access=_access(ctx, {"session_blacklist": [STRANGER_PRIVATE]}),
    )
    assert (
        arun(restricted.run(event, action="get_msg", message_id="1"))
        == UNLISTED_MESSAGE
    )
    # 完全没设名单时与以前一样
    open_tool = QQLookupTool(qq_client=_qq(ctx), access=_access(ctx))
    assert "悄悄话" in arun(open_tool.run(event, action="get_msg", message_id="1"))


def test_sent_tracker_checks_raw_isolated_target(tmp_path, monkeypatch):
    """主动发送的目标就是会话隔离后的原始 ID 时，也要按它查 AstrBot 会话管理。"""
    raw = "default:GroupMessage:50005_20002"

    class Service:
        @staticmethod
        async def is_session_enabled(umo):
            return umo != raw

    class Plugins:
        @staticmethod
        async def is_plugin_enabled_for_session(umo, name):
            return True

    monkeypatch.setattr(access_mod, "SessionServiceManager", Service)
    monkeypatch.setattr(access_mod, "SessionPluginManager", Plugins)

    async def main():
        s = await make_storage(tmp_path)
        try:
            ctx = FakeContext(config=_astrbot_config(enabled=False))
            tracker = SentTracker(
                s,
                ctx,
                PlatformHelper(ctx, FakeLogger()),
                lambda: TZ,
                FakeLogger(),
                lambda: True,
                access=_access(ctx),
            )
            for target in (raw, "default:GroupMessage:60006_20002"):
                await tracker.on_tool_respond(
                    FakeEvent(umo=ADMIN_PRIVATE),
                    SimpleNamespace(name="send_message_to_user"),
                    {"messages": [{"type": "plain", "text": f"发给 {target}"}]},
                    SimpleNamespace(
                        content=[
                            SimpleNamespace(text=f"Message sent to session {target}")
                        ]
                    ),
                )
            rows = await s.ledger.get_messages(ALLOWED, 0, 2e10)
            assert [r["text"] for r in rows] == [
                "发给 default:GroupMessage:60006_20002"
            ]
        finally:
            await s.close()

    arun(main())
