"""闹钟跟踪与对账。

- 生命周期与发送结果分离：fired/done 状态，send_ok/send_failed 事件。
- 手动提前执行记 manual_run 事件；一次性闹钟开始执行即停用平台任务，避免面板立即执行后到点再跑一次。
- 完整回流窗口内的执行、发送失败、缺失和中断；触发一小时内不判中断。
- 停机错过的一次性闹钟、平台任务已消失的重复闹钟，判为失踪并删掉残留任务。
- 唤醒时同时核对闹钟编号与平台任务编号；插件数据重置后遗留的旧任务不认，对账时删掉。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from . import constants as C
from .timeutil import local_datetime_str, utc_now

#: 判断「立即执行」的容差（秒）：比原定时间早这么多才标记为立即执行；只影响对账标签
MANUAL_RUN_TOLERANCE = 300

_SEND_OK_PREFIX = "Message sent to session"


class AlarmTracker:
    def __init__(self, storage: Any, context: Any, tz_getter, logger: Any):
        self.storage = storage
        self.context = context
        self._tz_getter = tz_getter
        self.logger = logger

    # ---------------- 钩子入口（由 main.py 转发） ----------------

    async def _alarm_for_event(self, event: Any) -> dict | None:
        """找出唤醒事件对应的 AstrHe 闹钟。

        插件数据被删除重建后，旧平台任务里的闹钟编号会撞上新闹钟，
        所以还要核对平台任务编号，对不上就不认，免得把新闹钟误标执行、误停任务。
        """
        payload = event.get_extra("cron_payload")
        if not isinstance(payload, dict) or not payload.get("astrhe_alarm_id"):
            return None
        alarm = await self.storage.alarms.get_by_alarm_id_str(
            str(payload["astrhe_alarm_id"])
        )
        if alarm is None:
            return None
        cron_job = event.get_extra("cron_job")
        job_id = str(cron_job.get("id") or "") if isinstance(cron_job, dict) else ""
        if job_id and job_id != alarm["cron_job_id"]:
            return None
        return alarm

    async def on_agent_begin(self, event: Any) -> None:
        """闹钟唤醒开始。"""
        alarm = await self._alarm_for_event(event)
        if alarm is None:
            return
        # 立即执行识别
        cron_job = event.get_extra("cron_job")
        if not isinstance(cron_job, dict):
            cron_job = {}
        run_started_at = str(cron_job.get("run_started_at", "") or "")
        run_at = alarm["run_at"]
        manual = False
        if run_at and run_started_at:
            try:
                started_ts = datetime.fromisoformat(run_started_at).timestamp()
                manual = started_ts < float(run_at) - MANUAL_RUN_TOLERANCE
            except (ValueError, TypeError):
                pass
        await self.storage.alarms.add_event(
            alarm["id"], "manual_run" if manual else "fired"
        )
        await self.storage.alarms.set_status(alarm["id"], "fired")
        # 一次性闹钟一开始执行就停用平台任务：到点执行的 AstrBot 跑完会自行删除；
        # 面板「立即执行」不删除任务，停用后原定时间不会再跑一次（含临近原定时间的点击）
        await self._disable_run_once(alarm)

    async def on_llm_tool_respond(
        self, event: Any, tool: Any, tool_args: Any, tool_result: Any
    ) -> None:
        """记录 send_message_to_user 的独立发送结果，不覆盖生命周期。"""
        if getattr(tool, "name", "") != "send_message_to_user":
            return
        alarm = await self._alarm_for_event(event)
        if alarm is None:
            return
        text = ""
        try:
            content = getattr(tool_result, "content", None) or []
            text = " ".join(str(getattr(c, "text", "")) for c in content)
        except Exception:  # noqa: BLE001
            text = str(tool_result or "")
        if _SEND_OK_PREFIX in text:
            await self.storage.alarms.add_event(
                alarm["id"], "send_ok", json.dumps(tool_args, ensure_ascii=False)
            )
        elif "error" in text.lower() or "failed" in text.lower():
            await self.storage.alarms.add_event(
                alarm["id"],
                "send_failed",
                text + "；参数：" + json.dumps(tool_args, ensure_ascii=False),
            )

    async def on_agent_done(self, event: Any) -> None:
        """正常结束 → done。"""
        alarm = await self._alarm_for_event(event)
        if alarm is None:
            return
        if alarm["status"] == "fired":
            await self.storage.alarms.add_event(alarm["id"], "done")
            await self.storage.alarms.set_status(alarm["id"], "done")

    async def finalize_stale_started(self) -> None:
        """触发超过一小时仍未正常结束才判中断。"""
        for alarm in await self.storage.alarms.active_alarms():
            if alarm["status"] != "fired":
                continue
            events = await self.storage.alarms.events(alarm["id"])
            fired = [e for e in events if e["kind"] in ("fired", "manual_run")]
            if fired and utc_now() - fired[-1]["ts"] > 3600:
                await self.storage.alarms.set_status(
                    alarm["id"], "failed", "执行被中断（超过一小时没有正常结束）"
                )

    async def _disable_run_once(self, alarm: dict) -> None:
        """面板立即执行后，停用 run_once 任务防止原定时间再跑一次。"""
        if not alarm["cron_job_id"] or alarm["cron_expression"]:
            return
        try:
            await self.context.cron_manager.update_job(
                alarm["cron_job_id"], enabled=False
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 停用 run_once 任务失败: {exc}")

    # ---------------- 夜间对账 ----------------

    async def _list_jobs(self) -> tuple[dict, bool]:
        """返回 (平台任务表, 是否读取成功)；读取失败时不能据此判断任务缺失。"""
        try:
            jobs = await self.context.cron_manager.list_jobs("active_agent")
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 读取平台任务失败: {exc}")
            return {}, False
        return {j.job_id: j for j in jobs}, True

    async def finish_lost_alarms(self, jobs: dict, jobs_ok: bool) -> set[int]:
        """把再也不会触发的生效中闹钟判为失踪，并删掉平台上的残留任务。

        - 一次性闹钟过了原定时间一小时仍无执行记录：AstrBot 停机错过 5 分钟
          宽限后只丢弃这次触发，任务行留在表里却永远不会再跑。
        - 重复闹钟的平台任务已经不存在（例如在面板里被删除）。
        """
        now = utc_now()
        lost: set[int] = set()
        for alarm in await self.storage.alarms.live_alarms():
            note = ""
            if not alarm["cron_expression"]:
                if (
                    alarm["status"] == "scheduled"
                    and alarm["run_at"] is not None
                    and alarm["run_at"] < now - C.ALARM_LOST_GRACE_SECONDS
                    and not any(
                        e["kind"] in ("fired", "manual_run")
                        for e in await self.storage.alarms.events(alarm["id"])
                    )
                ):
                    note = "错过：原定时间已过仍没有执行（可能因停机），不会再触发"
            elif jobs_ok and alarm["cron_job_id"] not in jobs:
                note = "重复任务在平台上已不存在（可能被面板删除），不会再触发"
            if not note:
                continue
            # 状态、原因和结束时间都记在闹钟本身；不另记事件，免得次晚对账重复出现
            await self.storage.alarms.set_status(alarm["id"], "missing", note)
            lost.add(alarm["id"])
            if alarm["cron_job_id"] in jobs:
                try:
                    await self.context.cron_manager.delete_job(alarm["cron_job_id"])
                except Exception as exc:  # noqa: BLE001
                    self.logger.debug(f"[AstrHe] 删除残留任务失败: {exc}")
        return lost

    async def drop_orphan_jobs(self, jobs: dict, jobs_ok: bool) -> list[str]:
        """删掉带 AstrHe 闹钟编号、却对不上任何现有闹钟的平台任务。

        插件数据被删除重建后，旧闹钟的平台任务还留在 AstrBot 里，到点会照旧执行，
        和新计划重复；它们是 AstrHe 自己建的，对账时一并清掉。
        """
        if not jobs_ok:
            return []
        dropped = []
        for job_id, job in list(jobs.items()):
            alarm_id = (job.payload or {}).get("astrhe_alarm_id")
            if not alarm_id:
                continue
            alarm = await self.storage.alarms.get_by_alarm_id_str(str(alarm_id))
            if alarm is not None and alarm["cron_job_id"] == job_id:
                continue
            try:
                await self.context.cron_manager.delete_job(job_id)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug(f"[AstrHe] 删除遗留平台任务失败: {exc}")
                continue
            jobs.pop(job_id, None)
            dropped.append(f"「{job.name}」（{job_id}）")
        if dropped:
            self.logger.info(
                f"[AstrHe] 删除遗留平台任务 {len(dropped)} 个：{'、'.join(dropped)}"
            )
        return dropped

    async def cancel_unlisted(
        self, access: Any, *, admins=(), dry: bool = False
    ) -> str:
        """目标已不在会话名单里的生效闹钟：管理员白天亲口定的保留，其余取消。

        正在执行的跳过，下一晚再看；演练只报告不取消。
        """
        admin_ids = {str(a) for a in admins}
        cancelled, kept, failed = [], [], []
        for alarm in await self.storage.alarms.live_alarms():
            if alarm["status"] == "fired" or await access.allows(alarm["target_umo"]):
                continue
            label = f"#{alarm['id']} → {alarm['target_umo']}"
            if alarm["origin"] == "day" and str(alarm["creator_id"]) in admin_ids:
                kept.append(label)
                continue
            if dry:
                cancelled.append(f"{label}（演练，未实际取消）")
                continue
            if alarm["cron_job_id"]:
                try:
                    await self.context.cron_manager.delete_job(alarm["cron_job_id"])
                except Exception as exc:  # noqa: BLE001
                    failed.append(f"{label}（{exc}）")
                    continue
            note = "目标会话已不在会话名单里，自动取消"
            await self.storage.alarms.set_status(alarm["id"], "cancelled", note)
            await self.storage.alarms.add_event(alarm["id"], "cancelled", note)
            cancelled.append(label)
        lines = []
        if cancelled:
            lines.append(
                "目标已不在会话名单里、已自动取消的闹钟：" + "、".join(cancelled)
            )
        if kept:
            lines.append(
                "目标不在会话名单里、由管理员定下而保留的闹钟（不要再往这些会话安排事）："
                + "、".join(kept)
            )
        if failed:
            lines.append(
                "目标不在会话名单里、但取消失败而保留原状态的闹钟（下次对账再试）："
                + "、".join(failed)
            )
        return "\n".join(lines)

    async def nightly_reconcile(self, window_start=0, window_end=None) -> str:
        """完整回流窗口内触发或应触发的闹钟，发送结果独立于生命周期。"""
        await self.finalize_stale_started()
        now = utc_now() if window_end is None else window_end
        tz = self._tz_getter()
        cron_jobs, jobs_ok = await self._list_jobs()
        dropped = await self.drop_orphan_jobs(cron_jobs, jobs_ok)
        # 不限于本窗口：停机错过的闹钟可能早于窗口起点，也要一次结清。
        lost = await self.finish_lost_alarms(cron_jobs, jobs_ok)
        lines = []
        for alarm in await self.storage.alarms.list_alarms(-1):
            events = await self.storage.alarms.events(alarm["id"], window_start, now)
            due = alarm["run_at"] is not None and window_start <= alarm["run_at"] <= now
            # 外部创建失败也要回流，不能等到永远不会发生的 fired 事件。
            failed = (
                alarm["status"] == "failed"
                and window_start <= alarm["created_at"] <= now
            )
            if not events and not due and not failed and alarm["id"] not in lost:
                continue
            all_events = await self.storage.alarms.events(alarm["id"])
            fired = any(e["kind"] in ("fired", "manual_run") for e in all_events)
            status = alarm["status"]
            if due and status == "scheduled" and not fired and jobs_ok:
                job = cron_jobs.get(alarm["cron_job_id"])
                if job is None or not job.enabled:
                    status = "missing"
                    await self.storage.alarms.set_status(
                        alarm["id"], status, "到点没执行，任务缺失或停用"
                    )
            details = []
            if any(e["kind"] == "manual_run" for e in events):
                details.append("面板立即执行过")
            sends = [e for e in events if e["kind"] in ("send_ok", "send_failed")]
            for e in sends:
                details.append(
                    ("消息已发出：" if e["kind"] == "send_ok" else "发送失败：")
                    + e["detail"]
                )
            labels = {
                "scheduled": "到点未见执行记录",
                "fired": "正在执行",
                "done": "已执行完成",
                "failed": "执行中断或创建失败",
                "missing": "到点没执行（失踪）",
                "cancelled": "已取消",
            }
            details.append(labels.get(status, status))
            if alarm["result_note"]:
                details.append(alarm["result_note"])
            when = (
                local_datetime_str(alarm["run_at"], tz)
                if alarm["run_at"]
                else alarm["cron_expression"] or "时间未建立"
            )
            lines.append(
                f"#{alarm['id']} → {alarm['target_umo']}（原定 {when}）："
                + "；".join(details)
            )
        text = (
            "闹钟对账：\n" + "\n".join(lines)
            if lines
            else "对账完成：窗口内没有触发或应触发的闹钟"
        )
        if dropped:
            text += (
                "\n已删除遗留的平台任务（带 AstrHe 闹钟编号，但对不上现有的 AstrHe 闹钟，"
                "多半是插件数据被重置过）：" + "、".join(dropped)
            )
        return text
