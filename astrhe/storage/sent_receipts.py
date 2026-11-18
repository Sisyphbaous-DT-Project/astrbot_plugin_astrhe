"""主动发送回执仓库：sent_receipts 表。

4.30 起主动发送不再抢写宿主普通历史，而是先持久保存独立回执，等目标会话
下一次由宿主 writer 管理的正常请求里补进上下文，并等宿主历史确认包含稳定
标记后才消费：

- pending：已发送，尚未补进任何请求。
- injected：已追加进某次请求的上下文（target_cid 记下对话），等待确认。
- saved：宿主历史里已查到稳定标记，消费完成。

模型失败、取消、历史保存失败、进程退出都退回 pending；已补记录按现有
保留期清理，未补记录跨天、重载仍保留。
"""

from __future__ import annotations

import asyncio
from typing import Any

from ..platform import build_umo, parse_umo
from .db import Database
from ..timeutil import utc_now

#: 每条回执在历史里的稳定标记前缀；确认与崩溃恢复都靠查它
MARKER_PREFIX = "astrhe-sent#"


def receipt_marker(receipt_id: int) -> str:
    return f"{MARKER_PREFIX}{receipt_id}"


def _umo_variants(umo: str) -> list[str]:
    """一个会话 ID 的可互认写法：原始写法 + 规范群写法。

    开启会话隔离后事件的会话 ID 是「发言人_群号」，回执可能按规范群写法
    存（或相反）；两边都查，规范群写法是公共交集。
    """
    canonical = canonical_group_umo(umo)
    variants = [umo]
    if canonical != umo:
        variants.append(canonical)
    return variants


def canonical_group_umo(umo: str) -> str:
    """隔离群写法（发言人_群号）的规范群写法；其余原样返回。

    只用作回执的公共匹配键：查原对话、判断同会话必须用原始写法
    （隔离会话有自己独立的对话）。
    """
    pid, kind, sid = parse_umo(umo)
    if kind == "GroupMessage" and "_" in sid:
        return build_umo(pid, kind, sid.split("_", 1)[1])
    return umo


def same_session_umo(a: str, b: str) -> bool:
    """两个会话 ID 是否指同一个会话：原始写法或规范群写法任一相同。

    隔离事件配规范目标、规范事件配隔离目标（同一个群）都算同会话。
    """
    if not a or not b:
        return False
    if a == b:
        return True
    return canonical_group_umo(a) == b or a == canonical_group_umo(b)


class SentReceiptsRepo:
    def __init__(self, db: Database):
        self.db = db
        self.boundary = asyncio.Lock()

    async def add(
        self,
        *,
        target_umo: str,
        text: str,
        sent_at: float,
        exec_id: str = "",
        alarm_id: int | None = None,
        origin_cid: str = "",
        scope_umo: str = "",
        request_id: str = "",
        call_id: str = "",
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO sent_receipts(target_umo, text, sent_at, exec_id,"
            " alarm_id, origin_cid, scope_umo, request_id, call_id, status) VALUES(?,?,?,?,?,?,?,?,?, 'pending')",
            (
                target_umo,
                text,
                sent_at,
                exec_id,
                alarm_id,
                origin_cid,
                scope_umo,
                request_id,
                call_id,
            ),
        )
        return int(cur.lastrowid)

    async def pending_for(self, umo: str) -> list[dict[str, Any]]:
        """该会话尚未保存的回执（含待确认），按发送顺序。

        会话隔离下按原始写法和规范群写法都查得到。
        """
        variants = _umo_variants(umo)
        placeholders = ",".join("?" * len(variants))
        return await self.db.fetchall(
            f"SELECT * FROM sent_receipts WHERE target_umo IN ({placeholders})"
            " AND status IN ('pending','injected') ORDER BY id",
            tuple(variants),
        )

    async def get(self, receipt_id: int) -> dict[str, Any] | None:
        return await self.db.fetchone(
            "SELECT * FROM sent_receipts WHERE id=?", (receipt_id,)
        )

    async def discard_before_reset(self, umo: str, cid: str, cutoff: float) -> None:
        await self.db.execute(
            "INSERT INTO receipt_resets(cid,cutoff) VALUES(?,?) ON CONFLICT(cid) DO UPDATE SET cutoff=MAX(cutoff,excluded.cutoff)",
            (cid, cutoff),
        )
        await self.db.execute(
            "DELETE FROM sent_receipts WHERE status!='saved' AND sent_at<=? AND (origin_cid=? OR target_cid=? OR (origin_cid='' AND scope_umo=?))",
            (cutoff, cid, cid, umo),
        )

    async def reset_cutoff(self, cid: str) -> float:
        row = await self.db.fetchone(
            "SELECT cutoff FROM receipt_resets WHERE cid=?", (cid,)
        )
        return row["cutoff"] if row else 0

    async def mark_injected(self, ids: list[int], target_cid: str) -> None:
        if not ids:
            return
        await self.db.execute(
            f"UPDATE sent_receipts SET status='injected', target_cid=?,origin_cid=CASE WHEN origin_cid='' THEN ? ELSE origin_cid END"
            f" WHERE status!='saved' AND id IN ({','.join('?' * len(ids))})",
            (target_cid, target_cid, *ids),
        )

    async def mark_saved(self, ids: list[int]) -> None:
        if not ids:
            return
        await self.db.execute(
            f"UPDATE sent_receipts SET status='saved', saved_at=?"
            f" WHERE status!='saved' AND id IN ({','.join('?' * len(ids))})",
            (utc_now(), *ids),
        )

    async def revert_to_pending(self, ids: list[int]) -> None:
        if not ids:
            return
        await self.db.execute(
            f"UPDATE sent_receipts SET status='pending'"
            f" WHERE status='injected' AND id IN ({','.join('?' * len(ids))})",
            tuple(ids),
        )

    async def injected_receipts(self) -> list[dict[str, Any]]:
        return await self.db.fetchall(
            "SELECT * FROM sent_receipts WHERE status='injected' ORDER BY id"
        )

    async def cleanup_saved_before(self, before_ts: float) -> int:
        """清掉早已补进历史的回执；未补的（pending/injected）绝不按时间清理。"""
        cur = await self.db.execute(
            "DELETE FROM sent_receipts WHERE status='saved' AND saved_at<?",
            (before_ts,),
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def count(self, status: str) -> int:
        row = await self.db.fetchone(
            "SELECT COUNT(*) AS n FROM sent_receipts WHERE status=?", (status,)
        )
        return int(row["n"]) if row else 0
