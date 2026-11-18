"""主动发送留存：bot 经 send_message_to_user 发出的消息。

AstrBot 不会把这条路发出的原文留在任何地方：
- AstrHe 账本原本只在 after_message_sent 记普通回复，工具发送不经过它；
- 定时任务唤醒后，AstrBot 只往唤醒会话的对话历史追加一段任务总结
  （旧版 cron 的 persist_agent_history），而且是把唤醒时读到的旧历史整份写回，
  运行中途写进去的内容会被覆盖；4.30 起连这段总结也没有了；
- 平时聊天发到别的会话时，目标会话的对话历史里什么都没有。

两种留存方式（按宿主能力分流）：

- 旧版 AstrBot：原文记进账本（无平台 id 的临时记录，夜间补拉完整覆盖后换成
  平台消息），并补进目标会话的对话历史——定时唤醒发出的等 AstrBot 写完总结
  再补，平时聊天发到别的会话的当场补；发到当前会话的工具调用本身已在对话
  历史里，不重复。
- AstrBot 4.30：发送成功立即持久保存独立回执
  （sent_receipts 表），不再抢写宿主普通历史；目标会话下一次由宿主 writer
  管理的正常请求里，把待补回执作为「系统记录」追加到请求上下文末尾；
  等 turn 真正结束且宿主历史里查到稳定标记（astrhe-sent#编号）才消费，
  失败、取消、历史保存失败、进程退出都退回待补，下次重试。
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import timezone
from typing import Any

from .platform import build_umo, is_valid_qq, parse_umo
from .storage import Storage
from .storage.sent_receipts import (
    canonical_group_umo,
    receipt_marker,
    same_session_umo,
)
from .timeutil import local_datetime_str, utc_now

#: send_message_to_user 成功时的返回前缀，后面跟着实际送达的目标会话
SEND_OK_PREFIX = "Message sent to session"
#: 本次 agent 运行里经工具发出的消息，暂存在事件上，运行结束后补进对话历史（旧版路径）
SENT_EXTRA_KEY = "astrhe_tool_sent"
#: 注入待补回执后暂存在事件上，turn 结束时据此确认或退回（4.30 路径）
RECEIPTS_EXTRA_KEY = "astrhe_receipts"
#: 本轮发到当前会话的回执编号，收尾时凭已保存的请求和工具调用编号确认
SAME_SENT_EXTRA_KEY = "astrhe_same_sent"

_MEDIA_TEXT = {
    "image": "[图片]",
    "record": "[语音]",
    "video": "[视频]",
    "file": "[文件]",
}


def messages_to_text(messages: Any) -> str:
    """把 send_message_to_user 的 messages 参数转成纯文本，写法与账本一致。"""
    parts: list[str] = []
    for msg in messages if isinstance(messages, list) else []:
        if not isinstance(msg, dict):
            continue
        kind = str(msg.get("type", "")).lower()
        if kind == "plain":
            parts.append(str(msg.get("text", "") or ""))
        elif kind == "mention_user":
            parts.append(f"[@{msg.get('mention_user_id', '')}]")
        else:
            parts.append(_MEDIA_TEXT.get(kind, f"[{kind}]"))
    return "".join(parts).strip()


def sent_target(tool_result: Any) -> str:
    """从工具结果里取出实际送达的会话；发送失败返回空串。"""
    try:
        content = getattr(tool_result, "content", None)
        if content is not None:
            text = " ".join(str(getattr(c, "text", "")) for c in content)
        else:
            text = str(tool_result or "")
    except Exception:  # noqa: BLE001
        return ""
    text = text.strip()
    if not text.startswith(SEND_OK_PREFIX):
        return ""
    target = text[len(SEND_OK_PREFIX) :].strip()
    pid, kind, sid = parse_umo(target)
    return target if pid and kind and sid else ""


def _cron_job(event: Any) -> dict | None:
    """定时唤醒事件携带的任务信息；普通事件返回 None。"""
    job = event.get_extra("cron_job")
    return job if isinstance(job, dict) else None


class SentTracker:
    """记下 bot 经工具主动发出的消息。"""

    #: 等 AstrBot 写完定时任务总结的上限（秒）与轮询间隔；超时照样补（旧版路径）
    summary_wait_seconds: float = 60.0
    summary_poll_interval: float = 1.0

    def __init__(
        self,
        storage: Storage,
        context: Any,
        platform: Any,
        tz_getter,
        logger: Any,
        enabled_getter,
        access: Any = None,
        modern_mode_getter=None,
    ):
        self.storage = storage
        self.context = context
        self.platform = platform
        self._tz_getter = tz_getter
        self.logger = logger
        self._enabled_getter = enabled_getter
        self.access = access
        self._modern_mode_getter = modern_mode_getter
        self._tasks: set[asyncio.Task] = set()

    def _enabled(self) -> bool:
        try:
            return bool(self._enabled_getter())
        except Exception:  # noqa: BLE001
            return False

    def _modern(self) -> bool:
        """按宿主版本分流；新版原生适配不可用也不回退到旧版整份改写。"""
        try:
            return bool(self._modern_mode_getter())
        except Exception:  # noqa: BLE001
            return False

    # ---------------- 钩子入口（由 main.py 转发） ----------------

    async def on_tool_respond(
        self, event: Any, tool: Any, tool_args: Any, tool_result: Any
    ) -> None:
        """工具发送成功后：记账本；4.30 落回执，旧版按旧路径补历史。"""
        try:
            if not self._enabled():
                return
            if getattr(tool, "name", "") != "send_message_to_user":
                return
            target = sent_target(tool_result)
            args = tool_args if isinstance(tool_args, dict) else {}
            text = messages_to_text(args.get("messages"))
            if not target or not text:
                return
            # 发到会话名单外的不留任何记录
            source = getattr(event, "unified_msg_origin", "")
            scope = (
                source
                if same_session_umo(source, target)
                and canonical_group_umo(target) == target
                else target
            )
            if self.access is not None and not await self.access.allows(scope):
                return
            ts = utc_now()
            if self._modern():
                async with self.storage.sent_receipts.boundary:
                    await self._add_receipt(event, target, text, ts, scope)
                await self._record_ledger(event, target, text, ts)
                return
            await self._record_ledger(event, target, text, ts)
            item = {"umo": target, "text": text, "ts": ts}
            if _cron_job(event) is not None:
                sent = event.get_extra(SENT_EXTRA_KEY) or []
                sent.append(item)
                event.set_extra(SENT_EXTRA_KEY, sent)
            elif target != getattr(event, "unified_msg_origin", ""):
                await self._append_history(target, [item], "")
        except Exception as exc:  # noqa: BLE001 - 留存失败绝不影响主流程
            self.logger.debug(f"[AstrHe] 记录主动发送失败: {exc}")

    async def on_agent_done(self, event: Any) -> None:
        """定时唤醒结束（旧版路径）：后台等 AstrBot 写完总结，再补原文。"""
        try:
            if self._modern():
                return  # 4.30 没有任务总结；回执走「目标下次请求时补入」
            job = _cron_job(event)
            sent = event.get_extra(SENT_EXTRA_KEY) if job is not None else None
            if not sent:
                return
            event.set_extra(SENT_EXTRA_KEY, [])
            task = asyncio.create_task(self._flush_cron(event, job, list(sent)))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 安排补写对话历史失败: {exc}")

    async def stop(self) -> None:
        """卸载前取消还在等待的补写任务。"""
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---------------- 回执路径（4.30） ----------------

    async def _add_receipt(
        self, event: Any, target: str, text: str, ts: float, scope: str
    ) -> None:
        """发送成功立即落回执，并记下目标会话当时的对话。

        原始 target 用于查原对话（origin_cid）与同会话判断——开启会话隔离时
        「发言人_群号」有自己独立的对话；规范群写法只作为回执的公共匹配键
        入库，隔离开关前后、事件写法不同都能查到。
        发到当前会话的也落回执：宿主在取消/中断的轮次不保存历史，工具调用
        原文可能根本没留下；收尾时检查宿主是否保存了对应工具调用。
        """
        info = event.get_extra("astrhe_dispatch")
        exec_id = str(info.get("exec_no", "")) if isinstance(info, dict) else ""
        alarm_id = int(info.get("alarm_id", 0)) if isinstance(info, dict) else None
        origin_cid = ""
        mgr = getattr(self.context, "conversation_manager", None)
        if mgr is not None:
            try:
                origin_cid = str(await mgr.get_curr_conversation_id(scope) or "")
            except Exception:  # noqa: BLE001
                origin_cid = ""
        same = same_session_umo(target, getattr(event, "unified_msg_origin", ""))
        bound = event.get_extra("astrhe_request_cid") or ""
        if same and bound and scope == event.unified_msg_origin:
            origin_cid = bound
        request_id, call_id = (
            self._tool_identity(event)
            if same and bound and bound == origin_cid
            else ("", "")
        )
        receipt_id = await self.storage.sent_receipts.add(
            target_umo=canonical_group_umo(target),
            text=text,
            sent_at=ts,
            exec_id=exec_id,
            alarm_id=alarm_id or None,
            origin_cid=origin_cid,
            scope_umo=scope,
            request_id=request_id,
            call_id=call_id,
        )
        if same:
            same = event.get_extra(SAME_SENT_EXTRA_KEY) or []
            same.append(receipt_id)
            event.set_extra(SAME_SENT_EXTRA_KEY, same)

    @staticmethod
    def _tool_identity(event):
        """宿主在发送结果钩子之前已发布本次工具的结束事件；不用消息原文猜身份。"""
        ref = event.get_extra("astrhe_main_turn")
        turn = ref() if ref else None
        if turn is None or turn.input.event is not event:
            return "", ""
        finished = started = None
        for entry in reversed(getattr(turn, "_events", ())):
            p = entry.payload
            if p.type == "tool.finished" and finished is None:
                finished = p
            elif (
                p.type == "tool.started"
                and finished
                and p.execution_id == finished.execution_id
            ):
                if p.name != "send_message_to_user":
                    break
                started = p
            elif p.type == "request.finished":
                return (p.request_id, started.call_id) if started else ("", "")
        return "", ""

    async def _saved_tool_call(self, row):
        if not row or not row["request_id"] or not row["origin_cid"]:
            return False
        store = getattr(
            self.context.conversation_manager.db, "conversation_store", None
        )
        snap = await store.read(row["origin_cid"]) if store else None
        return bool(
            snap
            and any(
                e["id"] == row["request_id"]
                and any(
                    call.get("id") == row["call_id"]
                    and call.get("function", {}).get("name") == "send_message_to_user"
                    for call in e["message"].get("tool_calls") or []
                )
                for e in snap.entries
            )
        )

    async def inject_into(self, event: Any, req: Any) -> None:
        if not self._modern() or getattr(event, "event_kind", "") == "agent_wakeup":
            return
        async with self.storage.sent_receipts.boundary:
            await self._inject_bound(event, req)

    async def _inject_bound(self, event: Any, req: Any) -> None:
        """目标会话的正常请求里，把待补回执追加到请求上下文末尾。

        由 injector 在会话名单判断之后调用。追加的一对记录保持「系统记录」
        语义与发送顺序，不改稳定 system 前缀；实际保存由宿主同一个 writer
        完成，本轮就发出去的（同一次执行的回执）不重复追加。
        """
        try:
            umo = getattr(event, "unified_msg_origin", "")
            cid = str(getattr(getattr(req, "conversation", None), "cid", "") or "")
            event.set_extra("astrhe_request_cid", cid)
            if not umo or not cid:
                return
            receipts = await self.storage.sent_receipts.pending_for(umo)
            if not receipts:
                return
            cutoff = await self.storage.sent_receipts.reset_cutoff(cid)
            # 插件未运行期间宿主也可能清空非空历史。只为确有待补的对话读重置事件。
            store = getattr(
                self.context.conversation_manager.db, "conversation_store", None
            )
            if store and hasattr(store, "events"):
                before = None
                oldest = min(r["sent_at"] for r in receipts)
                while True:
                    page = await store.events(
                        cid,
                        before_seq=before,
                        event_type="context.rebased",
                        descending=True,
                        limit=32,
                    )
                    if not page:
                        break
                    for e in page:
                        ts = e.created_at.replace(tzinfo=timezone.utc).timestamp()
                        if (e.payload or {}).get("reason") == "reset":
                            cutoff = max(cutoff, ts)
                    if cutoff or ts < oldest:
                        break
                    before = page[-1].seq
            info = event.get_extra("astrhe_dispatch")
            if isinstance(info, dict):
                # 同一次执行刚发出去的原文已在工具调用参数里，不重复补
                receipts = [
                    r for r in receipts if r["exec_id"] != str(info.get("exec_no", ""))
                ]
            # /new 换编号，/reset 通常不换：分别核对原对话和清空时间。
            receipts = [
                r
                for r in receipts
                if (not r["origin_cid"] or r["origin_cid"] == cid)
                and (not r["scope_umo"] or same_session_umo(r["scope_umo"], umo))
                and r["sent_at"] > cutoff
            ]
            # 上次注入过但没等到确认的：先查稳定标记，已在历史里的直接消费
            pending_ids: list[int] = []
            for r in receipts:
                if await self._saved_tool_call(r):
                    await self.storage.sent_receipts.mark_saved([r["id"]])
                    continue
                if r["status"] == "injected" and cid and r["target_cid"] == cid:
                    if await self._history_contains(cid, [receipt_marker(r["id"])]):
                        await self.storage.sent_receipts.mark_saved([r["id"]])
                        continue
                pending_ids.append(r["id"])
            if not pending_ids:
                return
            included = [r for r in receipts if r["id"] in set(pending_ids)]
            when = local_datetime_str(included[0]["sent_at"], self._tz_getter())
            markers = " ".join(receipt_marker(r["id"]) for r in included)
            texts = "\n".join(r["text"] for r in included)
            pair = [
                {
                    "role": "user",
                    "content": (
                        f"（系统记录：下面是你 {when} 主动发到这个会话的消息原文，"
                        f"不是对方说的话。记录编号 {markers}）"
                    ),
                },
                {"role": "assistant", "content": texts},
            ]
            contexts = getattr(req, "contexts", None)
            if contexts is None:
                req.contexts = []
            elif isinstance(contexts, str):
                req.contexts = json.loads(contexts)
            elif not isinstance(contexts, list):
                req.contexts = list(contexts)
            req.contexts.extend(pair)
            await self.storage.sent_receipts.mark_injected(pending_ids, cid)
            event.set_extra(RECEIPTS_EXTRA_KEY, {"ids": pending_ids, "cid": cid})
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 补入主动发送回执失败: {exc}")

    async def handle_turn_settled(self, event: Any, result: Any) -> None:
        """turn 结束后确认回执：宿主历史里查到稳定标记才消费，否则退回待补。

        由 wakeup_compat 的 turn 观察回调触发；注入进 req.contexts 不等于
        保存成功，模型失败、取消、历史保存失败都不能消费。
        发到当前会话的回执检查宿主实际保存的请求与工具调用，不把 completed 当作保存证明。
        """
        try:
            same_ids = event.get_extra(SAME_SENT_EXTRA_KEY) or []
            if same_ids:
                event.set_extra(SAME_SENT_EXTRA_KEY, None)
                for i in same_ids:
                    if await self._saved_tool_call(
                        await self.storage.sent_receipts.get(i)
                    ):
                        await self.storage.sent_receipts.mark_saved([i])
            info = event.get_extra(RECEIPTS_EXTRA_KEY)
            if not isinstance(info, dict) or not info.get("ids"):
                return
            event.set_extra(RECEIPTS_EXTRA_KEY, None)
            ids = [int(i) for i in info["ids"]]
            cid = str(info.get("cid") or "")
            for i in ids:
                confirmed = await self._history_contains(cid, [receipt_marker(i)])
                if confirmed:
                    await self.storage.sent_receipts.mark_saved([i])
                elif confirmed is False:
                    await self.storage.sent_receipts.revert_to_pending([i])
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 确认主动发送回执失败: {exc}")

    async def recover_receipts(self) -> None:
        """启动/重载时的崩溃恢复：injected 状态先查稳定标记再定去留。"""
        try:
            for r in await self.storage.sent_receipts.injected_receipts():
                cid = str(r["target_cid"] or "")
                confirmed = (
                    await self._history_contains(cid, [receipt_marker(r["id"])])
                    if cid
                    else None
                )
                if confirmed:
                    await self.storage.sent_receipts.mark_saved([r["id"]])
                elif confirmed is False:
                    await self.storage.sent_receipts.revert_to_pending([r["id"]])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"[AstrHe] 恢复主动发送回执失败: {exc}")

    async def _history_contains(self, cid: str, markers: list[str]) -> bool | None:
        """宿主历史里是否已包含全部稳定标记；读不到返回 None（当作未确认）。"""
        mgr = getattr(self.context, "conversation_manager", None)
        if mgr is None or not cid:
            return None
        try:
            db = getattr(mgr, "db", None)
            if db is None or not hasattr(db, "get_conversation_by_id"):
                return None
            conv = await db.get_conversation_by_id(cid)
        except Exception:  # noqa: BLE001
            return None
        if conv is None:
            return False
        content = getattr(conv, "content", None)
        if content is None:
            history = getattr(conv, "history", None)
            content = json.loads(history) if isinstance(history, str) else history
        if content is None:
            return None
        history = json.loads(content) if isinstance(content, str) else content
        found = set()
        for msg in history:
            if msg.get("role") != "user":
                continue
            body = msg.get("content")
            parts = (
                [body]
                if isinstance(body, str)
                else [p.get("text", "") for p in body or [] if isinstance(p, dict)]
            )
            for part in parts:
                match = re.fullmatch(
                    r"（系统记录：下面是你 [^\n]+ 主动发到这个会话的消息原文，不是对方说的话。记录编号 (astrhe-sent#[0-9]+(?: astrhe-sent#[0-9]+)*)）",
                    part,
                )
                if match:
                    found.update(match[1].split())
        return all(m in found for m in markers)

    # ---------------- 内部（账本与旧版补写） ----------------

    async def _record_ledger(
        self, event: Any, target: str, text: str, ts: float
    ) -> None:
        pid, kind, sid = parse_umo(target)
        ids = set()
        for inst in self.platform.get_aiocqhttp_platforms():
            try:
                ids.add(inst.meta().id)
            except Exception:  # noqa: BLE001
                continue
        if pid not in ids:
            return  # 账本只记 aiocqhttp 平台
        umo = build_umo(pid, kind, sid)
        self_id = ""
        try:
            self_id = str(event.get_self_id() or "")
        except Exception:  # noqa: BLE001
            pass
        if not is_valid_qq(self_id):
            # 定时唤醒事件的 self_id 是 AstrBot 写死的 "astrbot"，改用账本里记过的 bot QQ
            self_id = await self.storage.ledger.bot_self_id()
        await self.storage.ledger.add_message(
            umo=umo,
            platform_id=pid,
            message_type=kind,
            session_id=parse_umo(umo)[2],
            sender_id=self_id,
            sender_name="",
            sender_card="",
            text=text,
            ts=ts,
            platform_message_id=None,
            is_bot=True,
            source="live",
        )

    async def _flush_cron(self, event: Any, job: dict, sent: list[dict]) -> None:
        try:
            name = str(job.get("name") or job.get("id") or "")
            own = getattr(event, "unified_msg_origin", "")
            by_umo: dict[str, list[dict]] = {}
            for item in sent:
                by_umo.setdefault(item["umo"], []).append(item)
            # 别的会话不受写回影响，先补；唤醒会话放最后，等总结时不拖住别人
            order = sorted(by_umo, key=lambda u: u == own)
            for umo in order:
                items = by_umo[umo]
                if umo == own:
                    # 唤醒会话的历史会被 AstrBot 整份写回，等它写完再补
                    await self._wait_cron_summary(
                        umo, str(job.get("run_started_at") or ""), name
                    )
                await self._append_history(umo, items, name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 补写对话历史失败: {exc}")

    async def _wait_cron_summary(self, umo: str, started_at: str, name: str) -> None:
        """轮询目标会话的对话历史，直到出现本次任务的总结或超时。"""
        mark = started_at or name
        waited = 0.0
        while waited < self.summary_wait_seconds:
            history = await self._load_history(umo)
            if history is None:
                return
            for msg in history[-6:]:
                content = str(msg.get("content", "")) if isinstance(msg, dict) else ""
                if (
                    msg.get("role") == "assistant"
                    and content.startswith("[CronJob]")
                    and mark in content
                ):
                    return
            await asyncio.sleep(self.summary_poll_interval)
            waited += self.summary_poll_interval

    async def _load_history(self, umo: str) -> list | None:
        mgr = getattr(self.context, "conversation_manager", None)
        if mgr is None:
            return None
        cid = await mgr.get_curr_conversation_id(umo)
        if not cid:
            return None
        conv = await mgr.get_conversation(umo, cid)
        if conv is None:
            return None
        history = json.loads(conv.history or "[]")
        return history if isinstance(history, list) else None

    async def _append_history(self, umo: str, items: list[dict], name: str) -> None:
        """往目标会话当前对话追加一对记录。

        会话在 AstrBot 里还没有对话时（例如第一次主动私聊某人），
        照 AstrBot 自己的做法新建一个，对方回话时就能接上。
        """
        mgr = getattr(self.context, "conversation_manager", None)
        if mgr is None or not items:
            return
        when = local_datetime_str(items[0]["ts"], self._tz_getter())
        source = f"执行定时任务「{name}」时" if name else "在别的会话里"
        pair = [
            {
                "role": "user",
                "content": f"（系统记录：下面是你 {when} {source}主动发到这个会话的消息原文，"
                "不是对方说的话）",
            },
            {"role": "assistant", "content": "\n".join(i["text"] for i in items)},
        ]
        cid = await mgr.get_curr_conversation_id(umo)
        conv = await mgr.get_conversation(umo, cid) if cid else None
        if conv is None:
            await mgr.new_conversation(umo, parse_umo(umo)[0], content=pair)
            return
        history = json.loads(conv.history or "[]")
        if not isinstance(history, list):
            return
        history.extend(pair)
        await mgr.update_conversation(umo, cid, history=history)
