"""每晚开跑前统一清理过期记录；日记和关系本保留，回收站满期才删除。

两种截止点：
- 过程记录（运行与汇报、草稿、审稿、小动作计数、修订、回收站）按 30 天截止；
- 聊天账本、已结束闹钟与执行流水按「30 天」和「本次复盘起点」中更早的一个截止，
  夜间长期失败时，还没复盘过的消息和闹钟结果不会被删。
"""

from .. import constants as C
from ..timeutil import utc_now
from .alarms import LIVE_ALARM_SQL

#: 生效中闹钟最近一次触发及其之后的流水要保留，否则年度、月度重复闹钟
#: 会显示成「未见执行记录」。
_KEEP_LATEST_RUN_SQL = (
    "EXISTS (SELECT 1 FROM alarms WHERE alarms.id=alarm_events.alarm_id"
    f" AND {LIVE_ALARM_SQL} AND alarm_events.ts>=(SELECT MAX(e.ts) FROM alarm_events e"
    " WHERE e.alarm_id=alarms.id AND e.kind IN ('fired','manual_run')))"
)


async def cleanup_history(
    storage,
    cutoff,
    keep_run_id,
    *,
    protected_cutoff=None,
    cron_manager=None,
    logger=None,
):
    protected = cutoff if protected_cutoff is None else min(cutoff, protected_cutoff)
    finished = await storage.db.fetchall(
        f"SELECT * FROM alarms WHERE NOT {LIVE_ALARM_SQL} "
        "AND COALESCE(finished_at, run_at, created_at)<?",
        (protected,),
    )
    if cron_manager is not None and finished:
        try:
            jobs = {j.job_id for j in await cron_manager.list_jobs("active_agent")}
            for alarm in finished:
                # 已结束闹钟不会再用到平台任务：停用的、停机错过的残留都一并删掉。
                if alarm["cron_job_id"] in jobs:
                    try:
                        await cron_manager.delete_job(alarm["cron_job_id"])
                    except Exception as exc:
                        if logger:
                            logger.warning(f"[AstrHe] 清理残留闹钟任务失败：{exc}")
        except Exception as exc:
            if logger:
                logger.warning(f"[AstrHe] 清理时查询平台任务失败：{exc}")
    db = storage.db
    counts = {}
    async with db.write_lock:
        try:
            await db.conn.execute("BEGIN")
            # 最近一次提交用于确定复盘窗口，即使很久没运行也不能删除。
            latest = await storage.runs.latest_committed_run()
            keep = latest["id"] if latest else -1
            old_runs = await db.fetchall(
                "SELECT id FROM runs WHERE ended_at<? AND id NOT IN (?,?)",
                (cutoff, keep_run_id, keep),
            )
            for row in old_runs:
                for table in ("reports", "drafts", "reviews"):
                    await db.conn.execute(
                        f"DELETE FROM {table} WHERE run_id=?", (row["id"],)
                    )
                await db.conn.execute("DELETE FROM runs WHERE id=?", (row["id"],))
            counts["runs"] = len(old_runs)
            # 再按当前状态判定，避免外部清理期间闹钟又发生变化。
            deleted = 0
            for alarm in finished:
                cur = await db.conn.execute(
                    f"DELETE FROM alarms WHERE id=? AND NOT {LIVE_ALARM_SQL} "
                    "AND COALESCE(finished_at, run_at, created_at)<?",
                    (alarm["id"], protected),
                )
                if cur.rowcount:
                    deleted += 1
                    await db.conn.execute(
                        "DELETE FROM alarm_events WHERE alarm_id=?", (alarm["id"],)
                    )
            counts["alarms"] = deleted
            cur = await db.conn.execute(
                f"DELETE FROM alarm_events WHERE ts<? AND NOT {_KEEP_LATEST_RUN_SQL}",
                (protected,),
            )
            counts["alarm_events"] = cur.rowcount
            for table in ("action_log", "memory_revisions"):
                cur = await db.conn.execute(
                    f"DELETE FROM {table} WHERE ts<?", (cutoff,)
                )
                counts[table] = cur.rowcount
            await db.conn.execute(
                "DELETE FROM plan_sections WHERE plan_id IN "
                "(SELECT id FROM plans WHERE deleted_at<?)",
                (cutoff,),
            )
            for table in ("plans", "todos", "promises"):
                cur = await db.conn.execute(
                    f"DELETE FROM {table} WHERE deleted_at<?", (cutoff,)
                )
                counts[table] = cur.rowcount
            await db.conn.commit()
        except BaseException:
            await db.conn.rollback()
            raise
    return counts


async def cleanup_before_night(storage, context, keep_run_id, window_start, logger):
    """每次非演练运行开头调用，不论当晚成败；清理失败只记日志。"""
    cutoff = utc_now() - C.LEDGER_RETENTION_SECONDS
    try:
        await storage.ledger.cleanup(min(cutoff, window_start))
    except Exception as exc:
        logger.warning(f"[AstrHe] 聊天账本清理失败：{exc}")
    try:
        # 已补进宿主历史的发送回执按保留期清理；未补的绝不按时间删除
        await storage.sent_receipts.cleanup_saved_before(min(cutoff, window_start))
    except Exception as exc:
        logger.warning(f"[AstrHe] 发送回执清理失败：{exc}")
    try:
        await cleanup_history(
            storage,
            cutoff,
            keep_run_id,
            protected_cutoff=window_start,
            cron_manager=getattr(context, "cron_manager", None),
            logger=logger,
        )
    except Exception as exc:
        logger.warning(f"[AstrHe] 过程记录清理失败：{exc}")
