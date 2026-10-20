"""补拉与合并：夜间用 NapCat 历史接口补齐账本。

- 群聊 get_group_msg_history / 私聊 get_friend_msg_history，分页锚点为
  message_seq（实为短 message_id），结果包含锚点本身，需去重。
- 会话没有消息时 NapCat 抛「消息不存在」，按「没有消息」处理。
- 补拉失败只记录到运行统计，不让整个流程失败。
- 窗口内的消息全部补拉，不设条数上限（2026-10-07 修订）。
"""

from __future__ import annotations

import re
from typing import Any

from . import constants as C
from .platform import PlatformHelper, friend_umo, group_umo
from .timeutil import utc_now
from .storage import Storage

_NO_MESSAGE_PATTERN = re.compile(
    r"消息不存在|message not found|not found", re.IGNORECASE
)

#: OneBot 消息段类型 → 占位符
_SEGMENT_PLACEHOLDER = {
    "image": "[图片]",
    "record": "[语音]",
    "video": "[视频]",
    "face": "[表情]",
    "reply": "[引用]",
    "forward": "[合并转发]",
    "file": "[文件]",
    "json": "[卡片消息]",
    "xml": "[卡片消息]",
    "poke": "[戳一戳]",
}


def segments_to_text(message: Any) -> str:
    """OneBot 消息（段列表或字符串）转纯文本。"""
    if isinstance(message, str):
        return message.strip()
    parts: list[str] = []
    for seg in message or []:
        if not isinstance(seg, dict):
            continue
        seg_type = seg.get("type", "")
        data = seg.get("data", {}) or {}
        if seg_type == "text":
            parts.append(str(data.get("text", "")))
        elif seg_type == "at":
            parts.append(f"[@{data.get('qq', '')}]")
        else:
            parts.append(_SEGMENT_PLACEHOLDER.get(seg_type, f"[{seg_type}]"))
    return "".join(parts).strip()


class Backfiller:
    """夜间补拉器。"""

    def __init__(
        self, storage: Storage, platform: PlatformHelper, logger: Any, access=None
    ):
        self.storage = storage
        self.platform = platform
        self.logger = logger
        self.access = access
        self._merged_bot_count = 0

    async def candidate_sessions(
        self, window_start: float
    ) -> tuple[list[str], list[str]]:
        """返回 (群号列表, 私聊 QQ 列表)。

        候选 = 窗口内账本有消息的会话 ∪ 窗口内触发过的 AstrHe 闹钟目标
        ∪ bot 所在的全部群（每晚都枚举，停机期间说过话的安静群也能补齐）
        ∪（冷启动或窗口有空档时）近期私聊。
        最后去掉会话名单外的会话：补拉直接找 QQ 要历史，AstrBot 的白名单管不到。
        """
        groups: set[str] = set()
        friends: set[str] = set()
        for umo in await self.storage.ledger.list_session_umos(window_start):
            pid, mtype, sid = umo.split(":", 2) if umo.count(":") >= 2 else ("", "", "")
            if mtype == "GroupMessage":
                groups.add(sid)
            elif mtype == "FriendMessage":
                friends.add(sid)

        # 已完成的闹钟也可能在窗口内主动发过消息，不能只看仍在等待的闹钟。
        alarms = {a["id"]: a for a in await self.storage.alarms.active_alarms()}
        for alarm in await self.storage.alarms.list_alarms(-1):
            if await self.storage.alarms.events(alarm["id"], window_start) or (
                alarm["run_at"] is not None and alarm["run_at"] >= window_start
            ):
                alarms[alarm["id"]] = alarm
        for alarm in alarms.values():
            _, mtype, sid = (
                alarm["target_umo"].split(":", 2)
                if alarm["target_umo"].count(":") >= 2
                else ("", "", "")
            )
            if mtype == "GroupMessage":
                groups.add(sid)
            elif mtype == "FriendMessage":
                friends.add(sid)

        # 全部群每晚都枚举：群的数量有限，安静的群一页就翻到窗口起点，代价很小；
        # 停机或停用期间插件没记到的群聊也能补齐
        try:
            group_list = await self.platform.call_action("get_group_list")
            for g in group_list or []:
                gid = str(g.get("group_id", ""))
                if gid:
                    groups.add(gid)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 取群列表失败: {exc}")

        # 近期私聊可能很多，只在冷启动（尚未成功提交）或窗口有长时间空档
        # （停用后重新启用、连续失败）时全量回溯
        last = await self.storage.runs.latest_committed_run()
        gap = utc_now() - window_start > C.BACKFILL_FULL_SCAN_GAP_SECONDS
        if last is None or gap:
            try:
                contacts = await self.platform.call_action(
                    "get_recent_contact", count=100
                )
                for c in contacts or []:
                    # chatType==1 为好友私聊（NapCat 扩展字段）
                    if c.get("chatType") == 1:
                        uid = str(c.get("user_id", "") or c.get("peerUin", ""))
                        if uid:
                            friends.add(uid)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug(f"[AstrHe] 取近期会话失败: {exc}")

        if self.access is not None:
            pid = self.platform.default_platform_id()
            groups = {g for g in groups if await self.access.allows(group_umo(pid, g))}
            friends = {
                f for f in friends if await self.access.allows(friend_umo(pid, f))
            }
        return sorted(groups), sorted(friends)

    async def backfill_all(
        self, window_start: float, window_end: float, *, self_id: str | None = None
    ) -> dict[str, Any]:
        """对全部候选会话补拉窗口内消息，返回统计信息。"""
        stats: dict[str, Any] = {
            "groups": 0,
            "friends": 0,
            "failed": [],
            "merged_bot": 0,
        }
        self._merged_bot_count = 0
        groups, friends = await self.candidate_sessions(window_start)
        platform_id = self.platform.default_platform_id()
        if self_id is None:
            self_id = await self._get_self_id()

        for gid in groups:
            try:
                n = await self._backfill_session(
                    umo=group_umo(platform_id, gid),
                    action="get_group_msg_history",
                    key="group_id",
                    target=gid,
                    window_start=window_start,
                    window_end=window_end,
                    self_id=self_id,
                )
                stats["groups"] += n
            except Exception as exc:  # noqa: BLE001 - 单会话失败不拖垮整晚
                stats["failed"].append(f"group:{gid}: {exc}")
                self.logger.debug(f"[AstrHe] 补拉群 {gid} 失败: {exc}")
        for uid in friends:
            try:
                n = await self._backfill_session(
                    umo=friend_umo(platform_id, uid),
                    action="get_friend_msg_history",
                    key="user_id",
                    target=uid,
                    window_start=window_start,
                    window_end=window_end,
                    self_id=self_id,
                )
                stats["friends"] += n
            except Exception as exc:  # noqa: BLE001
                stats["failed"].append(f"friend:{uid}: {exc}")
                self.logger.debug(f"[AstrHe] 补拉私聊 {uid} 失败: {exc}")
        stats["merged_bot"] = self._merged_bot_count
        return stats

    async def _get_self_id(self) -> str:
        try:
            info = await self.platform.call_action("get_login_info")
            return str(info.get("user_id", "") or "")
        except Exception:  # noqa: BLE001
            return ""

    async def _backfill_session(
        self,
        *,
        umo: str,
        action: str,
        key: str,
        target: str,
        window_start: float,
        window_end: float,
        self_id: str,
    ) -> int:
        """分页补拉一个会话窗口内的全部消息，返回写入/合并条数。"""
        _, message_type, session_id = umo.split(":", 2)
        anchor = ""  # 空锚点从最新开始
        seen_ids: set[str] = set()
        written = 0
        covered = False
        while True:
            params: dict[str, Any] = {key: int(target), "count": C.BACKFILL_PAGE_SIZE}
            if anchor:
                params["message_seq"] = int(anchor)
            try:
                result = await self.platform.call_action(action, **params)
            except RuntimeError as exc:
                if _NO_MESSAGE_PATTERN.search(str(exc)):
                    covered = True
                    break  # 历史已到底
                raise
            messages = (result or {}).get("messages") or []
            if not messages:
                covered = True
                break
            oldest_ts = None
            new_anchor = None
            page_new_ids = 0
            for raw in messages:
                mid = str(raw.get("message_id", "") or "")
                ts = float(raw.get("time", 0) or 0)
                if oldest_ts is None or ts < oldest_ts:
                    oldest_ts = ts
                    new_anchor = mid
                if mid in seen_ids:
                    continue  # 分页结果包含锚点本身，去重
                if mid:
                    seen_ids.add(mid)
                    page_new_ids += 1
                if not (window_start <= ts <= window_end):
                    continue
                merged = await self._merge_message(
                    umo=umo,
                    message_type=message_type,
                    session_id=session_id,
                    raw=raw,
                    self_id=self_id,
                )
                if merged:
                    written += 1
            # 整页最旧消息已早于窗口起点，停止翻页
            if oldest_ts is not None and oldest_ts < window_start:
                covered = True
                break
            if page_new_ids == 0:
                covered = True
                break
            if not new_anchor or new_anchor == anchor:
                covered = True
                break  # 没有更旧的消息了
            anchor = new_anchor
        if covered:
            removed = await self.storage.ledger.remove_bot_temps(
                umo, window_start, window_end
            )
            self._merged_bot_count += removed
        return written

    async def _merge_message(
        self,
        *,
        umo: str,
        message_type: str,
        session_id: str,
        raw: dict,
        self_id: str,
    ) -> bool:
        """合并一条补拉消息进账本，返回是否有实质写入/合并。

        规则：
        - 非 bot 消息按 (umo, platform_message_id) 去重（INSERT OR IGNORE，
          已撤回的消息不会被覆盖回未撤回）。
        - bot 消息以真实平台 id 入库，完整覆盖窗口后统一清理临时回复。
        """
        mid = str(raw.get("message_id", "") or "")
        ts = float(raw.get("time", 0) or 0)
        sender = raw.get("sender", {}) or {}
        sender_id = str(raw.get("user_id", "") or sender.get("user_id", "") or "")
        post_type = str(raw.get("post_type", "message"))
        is_bot = post_type == "message_sent" or (self_id and sender_id == self_id)
        text = segments_to_text(raw.get("message"))
        platform_id = umo.split(":", 1)[0]

        if not is_bot:
            row_id = await self.storage.ledger.add_message(
                umo=umo,
                platform_id=platform_id,
                message_type=message_type,
                session_id=session_id,
                sender_id=sender_id,
                sender_name=str(sender.get("nickname", "") or ""),
                sender_card=str(sender.get("card", "") or ""),
                text=text,
                ts=ts,
                platform_message_id=mid or None,
                is_bot=False,
                source="backfill",
            )
            return row_id is not None

        if mid:
            row_id = await self.storage.ledger.add_message(
                umo=umo,
                platform_id=platform_id,
                message_type=message_type,
                session_id=session_id,
                sender_id=sender_id or self_id,
                sender_name="",
                sender_card="",
                text=text,
                ts=ts,
                platform_message_id=mid,
                is_bot=True,
                source="backfill",
            )
            return row_id is not None
        return False
