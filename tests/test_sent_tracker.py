"""主动发送留存：工具发出的消息进账本、补进 AstrBot 对话历史；分段回复按整条入账。"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from astrhe.recorder import Recorder
from astrhe.sent_tracker import SentTracker, messages_to_text, sent_target
from astrhe.platform import PlatformHelper
from tests.conftest import (
    FakeContext,
    FakeEvent,
    FakeLogger,
    Plain,
    arun,
    make_storage,
)

TZ = ZoneInfo("Asia/Shanghai")
SEND = SimpleNamespace(name="send_message_to_user")
PRIVATE = "default:FriendMessage:10001"
GROUP = "default:GroupMessage:20002"


Event = FakeEvent


class FakeConversations:
    """模拟 AstrBot ConversationManager：每个会话一个当前对话，历史存 JSON 字符串。"""

    def __init__(self, umos):
        self.store = {umo: ("cid-" + umo, "[]") for umo in umos}

    async def get_curr_conversation_id(self, umo):
        item = self.store.get(umo)
        return item[0] if item else None

    async def get_conversation(self, umo, cid):
        item = self.store.get(umo)
        if not item or item[0] != cid:
            return None
        return SimpleNamespace(cid=cid, history=item[1])

    async def update_conversation(self, umo, cid, history=None, **kwargs):
        self.store[umo] = (cid, json.dumps(history, ensure_ascii=False))

    async def new_conversation(self, umo, platform_id=None, content=None, **kwargs):
        self.store[umo] = (
            "cid-new-" + umo,
            json.dumps(content or [], ensure_ascii=False),
        )
        return self.store[umo][0]

    def history(self, umo):
        return json.loads(self.store[umo][1])


def _result(text):
    return SimpleNamespace(content=[SimpleNamespace(text=text)])


def _args(text, session=""):
    args = {
        "messages": [
            {"type": "mention_user", "mention_user_id": "10001"},
            {"type": "plain", "text": text},
        ]
    }
    if session:
        args["session"] = session
    return args


def _tracker(s, ctx):
    tracker = SentTracker(
        s,
        ctx,
        PlatformHelper(ctx, FakeLogger()),
        lambda: TZ,
        FakeLogger(),
        lambda: True,
    )
    tracker.summary_wait_seconds = 2.0
    tracker.summary_poll_interval = 0.02
    return tracker


def test_text_and_target_helpers():
    assert messages_to_text(_args("生日快乐！")["messages"]) == "[@10001]生日快乐！"
    assert messages_to_text([{"type": "image", "url": "x"}]) == "[图片]"
    assert sent_target(_result(f"Message sent to session {GROUP}")) == GROUP
    assert sent_target(_result("error: failed to send message")) == ""
    assert sent_target("Message sent to session bad") == ""


async def _cron_flow(tmp_path):
    from astrbot.core.utils.history_saver import persist_agent_history

    s = await make_storage(tmp_path)
    try:
        ctx = FakeContext()
        ctx.conversation_manager = FakeConversations([PRIVATE, GROUP])
        tracker = _tracker(s, ctx)
        # 唤醒时 AstrBot 读到的旧历史
        await ctx.conversation_manager.update_conversation(
            PRIVATE, "cid-" + PRIVATE, history=[{"role": "user", "content": "早"}]
        )
        stale = SimpleNamespace(
            cid="cid-" + PRIVATE,
            history=json.dumps([{"role": "user", "content": "早"}]),
        )
        started = "2026-10-10T16:00:00.03+00:00"
        from astrbot.core.cron.events import CronMessageEvent
        from astrbot.core.platform.message_session import MessageSession

        # 账本里已有 bot 的真实 QQ；真实唤醒事件的 self_id 是写死的 "astrbot"
        await s.ledger.add_message(
            umo=GROUP,
            platform_id="default",
            message_type="GroupMessage",
            session_id="20002",
            sender_id="99999",
            sender_name="",
            sender_card="",
            text="早上好",
            ts=1.0,
            platform_message_id="m1",
            is_bot=True,
            source="backfill",
        )
        event = CronMessageEvent(
            context=ctx,
            session=MessageSession.from_str(PRIVATE),
            message="唤醒",
            extras={"cron_job": {"name": "零点祝福", "run_started_at": started}},
        )
        assert event.get_self_id() == "astrbot" and event.unified_msg_origin == PRIVATE
        await tracker.on_tool_respond(
            event,
            SEND,
            _args("生日快乐"),
            _result(f"Message sent to session {PRIVATE}"),
        )
        await tracker.on_tool_respond(
            event,
            SEND,
            _args("群里也说一声", GROUP),
            _result(f"Message sent to session {GROUP}"),
        )
        # 账本：两条临时 bot 记录，各在各的会话
        temps = await s.ledger.find_bot_temp_messages(PRIVATE, 0)
        assert [t["text"] for t in temps] == ["[@10001]生日快乐"]
        assert temps[0]["sender_id"] == "99999"
        assert len(await s.ledger.find_bot_temp_messages(GROUP, 0)) == 1

        await tracker.on_agent_done(event)
        await asyncio.sleep(0.1)
        # 总结写回之前，唤醒会话不能先补（否则会被整份写回盖掉）
        assert ctx.conversation_manager.history(PRIVATE) == [
            {"role": "user", "content": "早"}
        ]
        # 别的会话不受唤醒会话写回影响，直接补上
        group_hist = ctx.conversation_manager.history(GROUP)
        assert group_hist[-1] == {
            "role": "assistant",
            "content": "[@10001]群里也说一声",
        }
        assert "零点祝福" in group_hist[-2]["content"]

        # 用 AstrBot 真实的写回函数模拟任务结束：旧历史 + 总结整份写回
        await persist_agent_history(
            ctx.conversation_manager,
            event=SimpleNamespace(unified_msg_origin=PRIVATE),
            req=SimpleNamespace(conversation=stale),
            summary_note=f"[CronJob] 零点祝福:  triggered at {started}, I finished this job",
        )
        await asyncio.wait_for(asyncio.gather(*tracker._tasks), 2)
        hist = ctx.conversation_manager.history(PRIVATE)
        assert hist[-3]["content"].startswith("[CronJob] 零点祝福")
        assert "零点祝福" in hist[-2]["content"] and hist[-2]["role"] == "user"
        assert hist[-1] == {"role": "assistant", "content": "[@10001]生日快乐"}
    finally:
        await s.close()


def test_cron_sends_reach_ledger_and_history_after_summary(tmp_path):
    try:
        from astrbot.core.cron.events import CronMessageEvent  # noqa: F401
        from astrbot.core.utils.history_saver import (  # noqa: F401
            persist_agent_history,
        )
    except Exception:
        pytest.skip(
            "AstrBot 4.30 起旧任务总结与 history_saver 已删除；"
            "4.30 的回执路径由 test_sent_receipts 覆盖"
        )
    arun(_cron_flow(tmp_path))


async def _summary_missing(tmp_path):
    s = await make_storage(tmp_path)
    try:
        ctx = FakeContext()
        ctx.conversation_manager = FakeConversations([PRIVATE])
        tracker = _tracker(s, ctx)
        tracker.summary_wait_seconds = 0.1
        event = Event(
            umo=PRIVATE, extras={"cron_job": {"name": "t", "run_started_at": "x"}}
        )
        await tracker.on_tool_respond(
            event, SEND, _args("在吗"), _result(f"Message sent to session {PRIVATE}")
        )
        await tracker.on_agent_done(event)
        await asyncio.wait_for(asyncio.gather(*tracker._tasks), 2)
        # 等不到总结（例如被同时段的聊天覆盖）也照样补
        assert (
            ctx.conversation_manager.history(PRIVATE)[-1]["content"] == "[@10001]在吗"
        )
    finally:
        await s.close()


def test_cron_history_appended_even_without_summary(tmp_path):
    arun(_summary_missing(tmp_path))


async def _day_flow(tmp_path):
    s = await make_storage(tmp_path)
    try:
        ctx = FakeContext()
        ctx.conversation_manager = FakeConversations([PRIVATE, GROUP])
        tracker = _tracker(s, ctx)
        event = Event(umo=PRIVATE)
        # 发到当前会话：工具调用本身就在对话历史里，不重复补
        await tracker.on_tool_respond(
            event, SEND, _args("图来了"), _result(f"Message sent to session {PRIVATE}")
        )
        assert ctx.conversation_manager.history(PRIVATE) == []
        # 发到别的会话：当场补进目标会话
        await tracker.on_tool_respond(
            event,
            SEND,
            _args("帮我哥问一声", GROUP),
            _result(f"Message sent to session {GROUP}"),
        )
        hist = ctx.conversation_manager.history(GROUP)
        assert hist[-1]["content"] == "[@10001]帮我哥问一声"
        assert "别的会话" in hist[-2]["content"]
        assert len(await s.ledger.find_bot_temp_messages(PRIVATE, 0)) == 1
        # 发送失败、非 aiocqhttp 平台、别的工具都不记
        await tracker.on_tool_respond(event, SEND, _args("x"), _result("error: failed"))
        await tracker.on_tool_respond(
            event,
            SEND,
            _args("x", "other:FriendMessage:1"),
            _result("Message sent to session other:FriendMessage:1"),
        )
        await tracker.on_tool_respond(
            event,
            SimpleNamespace(name="astrhe_schedule"),
            _args("x"),
            _result(f"Message sent to session {PRIVATE}"),
        )
        assert len(await s.ledger.find_bot_temp_messages(PRIVATE, 0)) == 1
        await tracker.on_agent_done(event)
        assert not tracker._tasks
    finally:
        await s.close()


def test_day_sends_to_other_sessions(tmp_path):
    arun(_day_flow(tmp_path))


async def _segmented(tmp_path):
    s = await make_storage(tmp_path)
    try:
        rec = Recorder(s, FakeLogger(), lambda: True)
        event = Event()
        event._result = SimpleNamespace(
            chain=[Plain("哥哥生日快乐！今天你最大。"), Plain("长门别乱造谣")]
        )
        rec.remember_reply(event)
        # 分段插件自己发出前几段，只把最后一段留给 AstrBot
        event._result.chain = [Plain("长门别乱造谣")]
        await rec.record_sent(event)
        temps = await s.ledger.find_bot_temp_messages(GROUP, 0)
        assert [t["text"] for t in temps] == ["哥哥生日快乐！今天你最大。长门别乱造谣"]
        # 用过即清，下一条回复不会沿用
        event._result.chain = [Plain("下一条")]
        await rec.record_sent(event)
        temps = await s.ledger.find_bot_temp_messages(GROUP, 0)
        assert temps[-1]["text"] == "下一条"
    finally:
        await s.close()


def test_segmented_reply_recorded_in_full(tmp_path):
    arun(_segmented(tmp_path))


async def _new_conversation(tmp_path):
    s = await make_storage(tmp_path)
    try:
        ctx = FakeContext()
        ctx.conversation_manager = FakeConversations([PRIVATE])
        tracker = _tracker(s, ctx)
        helper = "default:FriendMessage:30003"
        # 第一次主动私聊帮手：对方会话在 AstrBot 里还没有对话，新建一个带上原文
        await tracker.on_tool_respond(
            FakeEvent(umo=PRIVATE),
            SEND,
            _args("在吗", helper),
            _result(f"Message sent to session {helper}"),
        )
        hist = ctx.conversation_manager.history(helper)
        assert hist[-1] == {"role": "assistant", "content": "[@10001]在吗"}
        assert hist[0]["role"] == "user"
    finally:
        await s.close()


def test_send_to_session_without_conversation_creates_one(tmp_path):
    arun(_new_conversation(tmp_path))


async def _replaced(tmp_path):
    s = await make_storage(tmp_path)
    try:
        rec = Recorder(s, FakeLogger(), lambda: True)
        event = Event()
        event._result = SimpleNamespace(chain=[Plain("请求失败：APITimeoutError")])
        rec.remember_reply(event)
        # 后面的插件把整条回复换掉：按实际发出的记
        event._result.chain = [Plain("刚刚走神了")]
        await rec.record_sent(event)
        temps = await s.ledger.find_bot_temp_messages(GROUP, 0)
        assert [t["text"] for t in temps] == ["刚刚走神了"]
    finally:
        await s.close()


def test_replaced_reply_records_what_was_sent(tmp_path):
    arun(_replaced(tmp_path))
