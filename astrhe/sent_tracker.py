"""主动发送留存：bot 经 send_message_to_user 发出的消息。

AstrBot 不会把这条路发出的原文留在任何地方：
- AstrHe 账本原本只在 after_message_sent 记普通回复，工具发送不经过它；
- 定时任务唤醒后，AstrBot 只往唤醒会话的对话历史追加一段任务总结
  （cron/manager.py 的 persist_agent_history），而且是把唤醒时读到的旧历史整份写回，
  运行中途写进去的内容会被覆盖；
- 平时聊天发到别的会话时，目标会话的对话历史里什么都没有。

这里把原文记进账本（无平台 id 的临时记录，夜间补拉完整覆盖后换成平台消息），
并补进目标会话的对话历史：定时唤醒发出的等 AstrBot 写完总结再补，
平时聊天发到别的会话的当场补；发到当前会话的工具调用本身已在对话历史里，不重复。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from .platform import build_umo, is_valid_qq, parse_umo
from .storage import Storage
from .timeutil import local_datetime_str, utc_now

#: send_message_to_user 成功时的返回前缀，后面跟着实际送达的目标会话
SEND_OK_PREFIX = "Message sent to session"
#: 本次 agent 运行里经工具发出的消息，暂存在事件上，运行结束后补进对话历史
SENT_EXTRA_KEY = "astrhe_tool_sent"

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

    #: 等 AstrBot 写完定时任务总结的上限（秒）与轮询间隔；超时照样补
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
    ):
        self.storage = storage
        self.context = context
        self.platform = platform
        self._tz_getter = tz_getter
        self.logger = logger
        self._enabled_getter = enabled_getter
        self.access = access
        self._tasks: set[asyncio.Task] = set()

    def _enabled(self) -> bool:
        try:
            return bool(self._enabled_getter())
        except Exception:  # noqa: BLE001
            return False

    # ---------------- 钩子入口（由 main.py 转发） ----------------

    async def on_tool_respond(
        self, event: Any, tool: Any, tool_args: Any, tool_result: Any
    ) -> None:
        """工具发送成功后：记账本；定时唤醒的先暂存，平时发到别处的当场补历史。"""
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
            # 发到会话名单外的不补账本、不补对话历史
            if self.access is not None and not await self.access.allows(target):
                return
            ts = utc_now()
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
        """定时唤醒结束：后台等 AstrBot 写完总结，再把原文补进各目标会话。"""
        try:
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

    # ---------------- 内部 ----------------

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
