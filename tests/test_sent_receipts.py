"""主动发送回执（4.30 路径）：发送落回执、目标下次请求补入、稳定标记确认、
崩溃恢复、清理与去重。版本无关，直接测仓库与 SentTracker 行为。
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from astrhe.sent_tracker import SentTracker, messages_to_text, sent_target
from astrhe.storage.sent_receipts import receipt_marker
from tests.conftest import FakeContext, FakeEvent, FakeLogger, arun, make_storage

TZ = ZoneInfo("Asia/Shanghai")
GROUP = "default:GroupMessage:20002"
PRIVATE = "default:FriendMessage:40004"
OTHER = "default:FriendMessage:50005"

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


class FakeConversations430:
    """4.30 形状的对话管理：按 cid 存消息列表，current 记每个会话的当前对话。"""

    def __init__(self, current=None):
        self.db = self
        self.by_cid: dict[str, list] = {}
        self.current: dict[str, str] = current or {}

    async def get_curr_conversation_id(self, umo):
        return self.current.get(umo)

    async def get_conversation_by_id(self, cid):
        if cid not in self.by_cid:
            return None
        return SimpleNamespace(content=self.by_cid[cid])


class FakePlatforms:
    def get_aiocqhttp_platforms(self):
        return [
            SimpleNamespace(
                meta=lambda: SimpleNamespace(id="default", name="aiocqhttp")
            )
        ]


def _tracker(tmp_path, *, modern=True, ctx=None, current=None):
    s = arun(make_storage(tmp_path))
    _OPEN_STORAGES.append(s)
    ctx = ctx or FakeContext()
    ctx.conversation_manager = FakeConversations430(current)
    tracker = SentTracker(
        s,
        ctx,
        FakePlatforms(),
        lambda: TZ,
        FakeLogger(),
        lambda: True,
        modern_mode_getter=lambda: modern,
    )
    return tracker, s, ctx


def _tool_result(text):
    return SimpleNamespace(content=[SimpleNamespace(text=text)])


def _send_call(event, target, text="主动消息"):
    tool = SimpleNamespace(name="send_message_to_user")
    args = {"session": target, "messages": [{"type": "plain", "text": text}]}
    return event, tool, args, _tool_result(f"Message sent to session {target}")


def _stored_marker(i):
    return {
        "role": "user",
        "content": f"（系统记录：下面是你 今晚 主动发到这个会话的消息原文，不是对方说的话。记录编号 {receipt_marker(i)}）",
    }


def _bound_call(event, ctx, cid):
    from unittest.mock import AsyncMock

    def entry(kind, **kw):
        return SimpleNamespace(payload=SimpleNamespace(type=kind, **kw))

    turn = SimpleNamespace(
        input=SimpleNamespace(event=event),
        _events=[
            entry("request.finished", request_id="req"),
            entry(
                "tool.started",
                execution_id="e",
                call_id="tool-id",
                name="send_message_to_user",
            ),
            entry("tool.finished", execution_id="e"),
        ],
    )
    event.set_extra("astrhe_main_turn", lambda: turn)
    event.set_extra("astrhe_request_cid", cid)
    ctx.conversation_manager.conversation_store = SimpleNamespace(
        read=AsyncMock(
            return_value=SimpleNamespace(
                entries=[
                    {
                        "id": "req",
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "tool-id",
                                    "function": {"name": "send_message_to_user"},
                                }
                            ]
                        },
                    }
                ]
            )
        ),
        events=AsyncMock(return_value=[]),
    )


# ---------------- 发送时 ----------------


def test_send_creates_receipt_and_marks_own_session(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        # 发到别的会话：回执落库 + 账本临时记录
        event = FakeEvent(umo=GROUP)
        await tracker.on_tool_respond(*_send_call(event, PRIVATE))
        rows = await s.sent_receipts.pending_for(PRIVATE)
        assert len(rows) == 1
        assert rows[0]["text"] == "主动消息" and rows[0]["status"] == "pending"
        assert rows[0]["exec_id"] == ""
        # 账本也记了
        assert await s.ledger.list_session_umos(0) == [PRIVATE]
        # 发到当前会话：同样建回执（取消/失败的轮次历史不保存），挂在本轮 extra 上
        await tracker.on_tool_respond(*_send_call(event, GROUP))
        rows = await s.sent_receipts.pending_for(GROUP)
        assert len(rows) == 1
        same = event.get_extra("astrhe_same_sent")
        assert same == [rows[0]["id"]]
        assert await s.ledger.list_session_umos(0) == [PRIVATE, GROUP]
        # 同一目标、重复原文：两条回执，编号独立可区分
        await tracker.on_tool_respond(*_send_call(event, PRIVATE))
        await tracker.on_tool_respond(*_send_call(event, PRIVATE, text="主动消息"))
        assert len(await s.sent_receipts.pending_for(PRIVATE)) == 3
        ids = [r["id"] for r in await s.sent_receipts.pending_for(PRIVATE)]
        assert len(set(ids)) == 3
        # 失败的发送不落回执
        fail = _send_call(event, PRIVATE)
        await tracker.on_tool_respond(
            fail[0], fail[1], fail[2], _tool_result("error: failed to send message")
        )
        assert len(await s.sent_receipts.pending_for(PRIVATE)) == 3

    arun(check())


def test_alarm_send_records_exec_identity(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        event = FakeEvent(umo=GROUP)
        event.set_extra(
            "astrhe_dispatch",
            {"alarm_id": 7, "exec_no": "exec-9", "job_id": "j1", "target_umo": GROUP},
        )
        await tracker.on_tool_respond(*_send_call(event, PRIVATE))
        rows = await s.sent_receipts.pending_for(PRIVATE)
        assert rows[0]["exec_id"] == "exec-9" and rows[0]["alarm_id"] == 7

    arun(check())


def test_legacy_mode_keeps_old_path(tmp_path):
    tracker, s, ctx = _tracker(tmp_path, modern=False)
    appended = []

    async def append_async(umo, items, name):
        appended.append((umo, [i["text"] for i in items]))

    tracker._append_history = append_async

    async def check():
        # 平时聊天发到别的会话：旧路径当场补历史（不落回执）
        event = FakeEvent(umo=GROUP)
        await tracker.on_tool_respond(*_send_call(event, PRIVATE))
        assert appended == [(PRIVATE, ["主动消息"])]
        assert await s.sent_receipts.pending_for(PRIVATE) == []

    arun(check())


# ---------------- 补入与确认 ----------------


def test_inject_appends_receipts_to_contexts(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        event = FakeEvent(umo=PRIVATE)
        await tracker.on_tool_respond(*_send_call(FakeEvent(umo=GROUP), PRIVATE))
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), PRIVATE, text="第二条")
        )
        req = SimpleNamespace(
            contexts=[{"role": "user", "content": "早"}],
            conversation=SimpleNamespace(cid="cid-1"),
        )
        await tracker.inject_into(event, req)
        # 追加在末尾，不改前面的稳定内容；顺序保持
        assert req.contexts[0] == {"role": "user", "content": "早"}
        assert req.contexts[-2]["role"] == "user"
        assert "主动发到这个会话的消息原文" in req.contexts[-2]["content"]
        assert receipt_marker(1) in req.contexts[-2]["content"]
        assert receipt_marker(2) in req.contexts[-2]["content"]
        assert req.contexts[-1] == {
            "role": "assistant",
            "content": "主动消息\n第二条",
        }
        # 事件上带着待确认的回执编号与对话
        info = event.get_extra("astrhe_receipts")
        assert info["ids"] == [1, 2] and info["cid"] == "cid-1"
        rows = await s.sent_receipts.pending_for(PRIVATE)
        assert [r["status"] for r in rows] == ["injected", "injected"]
        assert all(r["target_cid"] == "cid-1" for r in rows)

    arun(check())


def test_inject_skips_same_exec_and_already_saved(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        # 闹钟在群里唤醒、发往私聊：两条回执（当前会话不重复，正常落回执）
        wake = FakeEvent(umo=GROUP)
        wake.set_extra(
            "astrhe_dispatch",
            {"alarm_id": 7, "exec_no": "exec-9", "job_id": "j1", "target_umo": GROUP},
        )
        await tracker.on_tool_respond(*_send_call(wake, PRIVATE, text="刚发的"))
        old = FakeEvent(umo=GROUP)
        old.set_extra(
            "astrhe_dispatch",
            {"alarm_id": 6, "exec_no": "exec-8", "job_id": "j0", "target_umo": GROUP},
        )
        await tracker.on_tool_respond(*_send_call(old, PRIVATE, text="早前发的"))
        # 私聊的下一次请求（普通聊天）：待补的全部按发送顺序补
        chat = FakeEvent(umo=PRIVATE)
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-1"))
        await tracker.inject_into(chat, req)
        assert req.contexts[-1]["content"] == "刚发的\n早前发的"
        assert receipt_marker(1) in req.contexts[-2]["content"]
        # 上次注入过、历史里已查到稳定标记的：直接消费，不再重复补；
        # 查不到标记的（那轮没保存成功）：重新补
        ctx.conversation_manager.by_cid["cid-1"] = [_stored_marker(2)]
        await s.sent_receipts.mark_injected([2], "cid-1")
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), PRIVATE, text="新的")
        )
        req2 = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-1"))
        chat2 = FakeEvent(umo=PRIVATE)
        await tracker.inject_into(chat2, req2)
        assert "早前发的" not in json.dumps(req2.contexts, ensure_ascii=False)
        assert req2.contexts[-1]["content"] == "刚发的\n新的"
        assert (await s.sent_receipts.get(2))["status"] == "saved"

    arun(check())


def test_turn_settled_confirms_or_reverts(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        await tracker.on_tool_respond(*_send_call(FakeEvent(umo=GROUP), PRIVATE))
        event = FakeEvent(umo=PRIVATE)
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-1"))
        await tracker.inject_into(event, req)
        # turn 失败：退回待补
        await tracker.handle_turn_settled(
            event, SimpleNamespace(status="failed", reason="error")
        )
        assert (await s.sent_receipts.get(1))["status"] == "pending"
        # turn 成功且历史里查到稳定标记：消费
        event2 = FakeEvent(umo=PRIVATE)
        req2 = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-1"))
        await tracker.inject_into(event2, req2)
        ctx.conversation_manager.by_cid["cid-1"] = [
            _stored_marker(1),
            {"role": "assistant", "content": "回复"},
        ]
        await tracker.handle_turn_settled(
            event2, SimpleNamespace(status="completed", reason="completed")
        )
        assert (await s.sent_receipts.get(1))["status"] == "saved"
        # turn 成功但历史里没有标记（保存失败/被截断）：退回
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), PRIVATE, text="再发")
        )
        event3 = FakeEvent(umo=PRIVATE)
        req3 = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-2"))
        await tracker.inject_into(event3, req3)
        await tracker.handle_turn_settled(
            event3, SimpleNamespace(status="completed", reason="completed")
        )
        assert (await s.sent_receipts.get(2))["status"] == "pending"

    arun(check())


def test_recover_receipts_uses_stable_marker(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), PRIVATE, text="a")
        )
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), PRIVATE, text="b")
        )
        await s.sent_receipts.mark_injected([1], "cid-ok")
        await s.sent_receipts.mark_injected([2], "cid-bad")
        ctx.conversation_manager.by_cid["cid-ok"] = [_stored_marker(1)]
        await tracker.recover_receipts()
        assert (await s.sent_receipts.get(1))["status"] == "saved"
        assert (await s.sent_receipts.get(2))["status"] == "pending"

    arun(check())


def test_cleanup_removes_only_saved(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        await s.sent_receipts.add(target_umo=PRIVATE, text="旧", sent_at=100.0)
        await s.sent_receipts.add(target_umo=PRIVATE, text="新", sent_at=1e10)
        await s.sent_receipts.mark_saved([1])
        # 现实时间戳（约 1.8e9）都小于 2e9：已补的清掉，pending 的留着
        await s.sent_receipts.cleanup_saved_before(2e9)
        assert (await s.sent_receipts.get(1)) is None
        assert (await s.sent_receipts.get(2)) is not None
        # pending 的永不按时间清理
        await s.sent_receipts.add(target_umo=PRIVATE, text="未补", sent_at=100.0)
        await s.sent_receipts.cleanup_saved_before(2e9)
        assert (await s.sent_receipts.get(3)) is not None

    arun(check())


def test_receipt_respects_reset_via_origin_cid(tmp_path):
    """回执绑定发送时目标所在对话：/reset 换新对话后不补，切回旧对话照常补。"""
    tracker, s, ctx = _tracker(tmp_path, current={PRIVATE: "cid-old"})

    async def check():
        # 发送时目标正在 cid-old 对话里
        await tracker.on_tool_respond(*_send_call(FakeEvent(umo=GROUP), PRIVATE))
        rows = await s.sent_receipts.pending_for(PRIVATE)
        assert rows[0]["origin_cid"] == "cid-old"
        # /reset 之后的新对话：不补
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-new"))
        await tracker.inject_into(FakeEvent(umo=PRIVATE), req)
        assert req.contexts == []
        assert (await s.sent_receipts.get(rows[0]["id"]))["status"] == "pending"
        # 切回旧对话（历史还在）：照常补
        req2 = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-old"))
        await tracker.inject_into(FakeEvent(umo=PRIVATE), req2)
        assert "主动消息" in req2.contexts[-1]["content"]
        # 发送时目标还没有对话（origin 为空）：新对话也照常补
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=GROUP), OTHER, text="初次私聊")
        )
        req3 = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-new"))
        await tracker.inject_into(FakeEvent(umo=OTHER), req3)
        assert "初次私聊" in req3.contexts[-1]["content"]

    arun(check())


def test_pending_for_matches_isolated_umo(tmp_path):
    """会话隔离：规范群写法与「发言人_群号」写法互相认得，群回执不丢。"""
    tracker, s, ctx = _tracker(tmp_path)
    ISOLATED = "default:GroupMessage:10001_20002"

    async def check():
        # 发送侧归一化：隔离写法的目标存成规范群写法
        await tracker.on_tool_respond(
            *_send_call(FakeEvent(umo=PRIVATE), ISOLATED, text="给群发")
        )
        rows = await s.sent_receipts.pending_for(GROUP)
        assert [r["target_umo"] for r in rows] == [GROUP]
        # 注入侧：隔离写法的请求查得到规范群写法的回执
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="c1"))
        await tracker.inject_into(FakeEvent(umo=ISOLATED), req)
        assert "给群发" in req.contexts[-1]["content"]
        # 仓库直查：两个方向都命中
        assert len(await s.sent_receipts.pending_for(ISOLATED)) == 1
        assert len(await s.sent_receipts.pending_for(GROUP)) == 1

    arun(check())


def test_same_session_receipt_lifecycle(tmp_path):
    """发到当前会话的回执：轮次正常完成直接消费；取消/失败留待下次补。"""
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        # turn 正常完成：工具调用原文已随历史保存，直接消费、不注入
        event = FakeEvent(umo=PRIVATE)
        _bound_call(event, ctx, "c1")
        await tracker.on_tool_respond(*_send_call(event, PRIVATE))
        ids = event.get_extra("astrhe_same_sent")
        assert ids
        await tracker.handle_turn_settled(
            event, SimpleNamespace(status="completed", reason="completed")
        )
        assert (await s.sent_receipts.get(ids[0]))["status"] == "saved"
        # turn 取消：那轮历史没保存，留待补
        event2 = FakeEvent(umo=PRIVATE)
        await tracker.on_tool_respond(*_send_call(event2, PRIVATE))
        ids2 = event2.get_extra("astrhe_same_sent")
        await tracker.handle_turn_settled(
            event2, SimpleNamespace(status="cancelled", reason="cancelled")
        )
        assert (await s.sent_receipts.get(ids2[0]))["status"] == "pending"
        # 下次请求把它补进上下文
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="c1"))
        await tracker.inject_into(FakeEvent(umo=PRIVATE), req)
        assert "主动消息" in req.contexts[-1]["content"]
        # 观察者没跑（崩溃兜底）：pending 保持，下次注入不丢
        event3 = FakeEvent(umo=PRIVATE)
        await tracker.on_tool_respond(*_send_call(event3, PRIVATE))
        assert (await s.sent_receipts.get(event3.get_extra("astrhe_same_sent")[0]))[
            "status"
        ] == "pending"

    arun(check())


def test_isolated_group_same_session_receipt(tmp_path):
    """隔离群的当前会话发送：原始写法查原对话、判同会话；完成即消费不重复补。"""
    ISOLATED = "default:GroupMessage:10001_20002"
    tracker, s, ctx = _tracker(tmp_path, current={ISOLATED: "cid-iso"})

    async def check():
        event = FakeEvent(umo=ISOLATED)
        _bound_call(event, ctx, "cid-iso")
        await tracker.on_tool_respond(*_send_call(event, ISOLATED, text="群里的悄悄话"))
        rows = await s.sent_receipts.pending_for(ISOLATED)
        r = rows[0]
        # 入库用规范群写法（公共匹配键），原对话按原始写法查到
        assert r["target_umo"] == GROUP
        assert r["origin_cid"] == "cid-iso"
        # 同会话判断成立：挂在本轮 extra 上
        assert event.get_extra("astrhe_same_sent") == [r["id"]]
        # 规范写法的目标 + 隔离写法的事件（同一个群）也算同会话
        event2 = FakeEvent(umo=ISOLATED)
        _bound_call(event2, ctx, "cid-iso")
        await tracker.on_tool_respond(*_send_call(event2, GROUP, text="规范写法目标"))
        assert event2.get_extra("astrhe_same_sent")
        # 真实完成（宿主已把工具调用原文随历史保存）→ 直接消费
        await tracker.handle_turn_settled(
            event, SimpleNamespace(status="completed", reason="completed")
        )
        await tracker.handle_turn_settled(
            event2, SimpleNamespace(status="completed", reason="completed")
        )
        assert (await s.sent_receipts.get(r["id"]))["status"] == "saved"
        # 下一轮同对话不重复补
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-iso"))
        await tracker.inject_into(FakeEvent(umo=ISOLATED), req)
        assert req.contexts == []

    arun(check())


def test_isolated_group_same_session_receipt_reset(tmp_path):
    """隔离群当前会话发送后 /reset：崩溃窗口里没消费的回执不进新对话。"""
    ISOLATED = "default:GroupMessage:10001_20002"
    tracker, s, ctx = _tracker(tmp_path, current={ISOLATED: "cid-iso-old"})

    async def check():
        event = FakeEvent(umo=ISOLATED)
        await tracker.on_tool_respond(*_send_call(event, ISOLATED))
        r = (await s.sent_receipts.pending_for(ISOLATED))[0]
        assert r["origin_cid"] == "cid-iso-old"
        # 崩溃窗口：结算回调没跑（进程退出），回执保持待补
        # /reset 之后的新对话：不补
        req = SimpleNamespace(contexts=[], conversation=SimpleNamespace(cid="cid-new"))
        await tracker.inject_into(FakeEvent(umo=ISOLATED), req)
        assert req.contexts == []
        assert (await s.sent_receipts.get(r["id"]))["status"] == "pending"
        # 切回旧对话（历史还在）：照常补
        req2 = SimpleNamespace(
            contexts=[], conversation=SimpleNamespace(cid="cid-iso-old")
        )
        await tracker.inject_into(FakeEvent(umo=ISOLATED), req2)
        assert "主动消息" in req2.contexts[-1]["content"]

    arun(check())


def test_ledger_target_helpers_unchanged():
    assert sent_target(_tool_result(f"Message sent to session {GROUP}")) == GROUP
    assert messages_to_text([{"type": "plain", "text": "你好"}]) == "你好"


def test_separate_isolated_speakers_and_unbound_request(tmp_path):
    left = "default:GroupMessage:10001_20002"
    right = "default:GroupMessage:30003_20002"
    tracker, s, _ = _tracker(tmp_path, current={left: "left", right: "right"})

    async def check():
        event = FakeEvent(umo=left)
        await tracker.on_tool_respond(*_send_call(event, right))
        assert not event.get_extra("astrhe_same_sent")
        await tracker.on_tool_respond(*_send_call(event, GROUP))
        assert (await s.sent_receipts.get(2))["origin_cid"] == "left"
        for cid, expected in [(None, []), ("unrelated", [])]:
            req = SimpleNamespace(
                conversation=SimpleNamespace(cid=cid) if cid else None, contexts=[]
            )
            await tracker.inject_into(event, req)
            assert req.contexts == expected
        req = SimpleNamespace(conversation=SimpleNamespace(cid="right"), contexts=[])
        await tracker.inject_into(FakeEvent(umo=right), req)
        assert "astrhe-sent#1" in str(req.contexts) and "astrhe-sent#2" not in str(
            req.contexts
        )

    arun(check())


def test_receipt_marker_and_late_confirmation(tmp_path):
    tracker, s, ctx = _tracker(tmp_path)

    async def check():
        await tracker.on_tool_respond(*_send_call(FakeEvent(umo=GROUP), PRIVATE))
        r = s.sent_receipts
        ctx.conversation_manager.by_cid["cid"] = [
            _stored_marker(10),
            {"role": "assistant", "content": "astrhe-sent#1"},
        ]
        assert not await tracker._history_contains("cid", [receipt_marker(1)])
        assert await tracker._history_contains("cid", [receipt_marker(10)])
        await r.mark_saved([1])
        await r.revert_to_pending([1])
        await r.mark_injected([1], "cid")
        assert (await r.get(1))["status"] == "saved"

    arun(check())
