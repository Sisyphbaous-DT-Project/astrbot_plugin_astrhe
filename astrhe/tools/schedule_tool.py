"""astrhe_schedule：增强定时工具，替代自带 future_task。

- 任务仍写入 AstrBot 定时任务系统（cron_manager.add_active_job），面板可见可改。
- 卡片（来龙去脉）完整写进 note/description：闹钟唤醒路径不经过 on_llm_request，
  被叫醒时只能看到 note，所以卡片必须完整到「只看卡片就能办成事」。
- 权限：管理员可跨会话；非管理员只能操作当前会话的 AstrHe 闹钟；
  外部任务（非 AstrHe 创建）只能看和取消。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .. import constants as C
from ..platform import build_umo, is_valid_qq, event_umo
from .alarm_card import render_card_note, alarm_description
from .validation import validate_alarm, validate_alarm_time
from ..timeutil import (
    local_datetime_str,
    utc_now,
)
from .secrecy import filter_secret_text, is_secret_visible_here, normalize_qq_ids

TOOL_NAME = "astrhe_schedule"

class ScheduleToolImpl:
    """astrhe_schedule 的实现。"""

    def __init__(
        self,
        storage: Any,
        context: Any,
        qq_client: Any,
        rate_limiter: Any,
        tz_getter,
        secrecy_resolver: Any,
        admins_getter,
        friend_request_manager=None,
        access=None,
    ):
        self.storage = storage
        self.context = context
        self.qq = qq_client
        self.rate_limiter = rate_limiter
        self._tz_getter = tz_getter
        self.secrecy_resolver = secrecy_resolver
        self._admins_getter = admins_getter
        self.friend_requests = friend_request_manager
        self.access = access

    # ---------------- 入口 ----------------

    async def run(self, event: Any, action: str = "", **kwargs: Any) -> str:
        if action == "create":
            return await self._create(event, **kwargs)
        if action == "list":
            return await self._list(event)
        if action == "edit":
            return await self._edit(event, **kwargs)
        if action == "cancel":
            return await self._cancel(event, **kwargs)
        return "action 只能是 create / list / edit / cancel"

    # ---------------- 权限与发起人 ----------------

    def _caller(self, event: Any) -> tuple[str, bool]:
        """返回 (发起人 sender_id, 是否管理员)。

        闹钟唤醒路径里发起人沿用 cron_payload.sender_id，权限看 event.role。
        """
        cron_payload = event.get_extra("cron_payload")
        if isinstance(cron_payload, dict) and cron_payload.get("sender_id"):
            sender_id = str(cron_payload["sender_id"])
        else:
            try:
                sender_id = str(event.get_sender_id())
            except Exception:  # noqa: BLE001
                sender_id = ""
        is_admin = getattr(event, "role", "member") == "admin"
        return sender_id, is_admin

    # ---------------- create ----------------

    async def _create(self, event: Any, **kwargs: Any) -> str:
        sender_id, is_admin = self._caller(event)
        target_type = str(kwargs.get("target_type", "current"))
        target_id = str(kwargs.get("target_id", "") or "")
        platform_id = ""
        current_umo = ""
        try:
            platform_id = event.get_platform_id()
            current_umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass

        # 目标会话
        if target_type == "current":
            target_umo = current_umo
        elif target_type == "group":
            if not is_admin:
                return "只有管理员才能给别的会话定闹钟；给自己所在的会话定用 target_type=current"
            if not is_valid_qq(target_id):
                return "target_id 需要是群号（5-12 位数字）"
            target_umo = build_umo(
                platform_id or self.qq.platform.default_platform_id(),
                "GroupMessage",
                target_id,
            )
        elif target_type == "private":
            if not is_admin:
                return "只有管理员才能给别的会话定闹钟；给自己所在的会话定用 target_type=current"
            if not is_valid_qq(target_id):
                return "target_id 需要是 QQ 号（5-12 位数字）"
            target_umo = build_umo(
                platform_id or self.qq.platform.default_platform_id(),
                "FriendMessage",
                target_id,
            )
        else:
            return "target_type 只能是 current / group / private"
        if not target_umo or target_umo.startswith(":"):
            return "确定不了目标会话，请检查 target_type 和 target_id"

        run_at_str = str(kwargs.get("run_at", "") or "")
        cron_expression = str(kwargs.get("cron_expression", "") or "")
        tz = self._tz_getter()
        expect_ids = kwargs.get("expect_friend_ids") or []
        if expect_ids and not is_admin:
            return "只有管理员能标记预期联系人"
        if not isinstance(expect_ids, list) or any(
            not is_valid_qq(str(q)) for q in expect_ids
        ):
            return "expect_friend_ids 必须是 QQ 号数组"
        if len(set(expect_ids)) > C.EXPECTED_FRIEND_MAX_PER_NIGHT:
            return "一次最多标记 3 名预期联系人"

        # 卡片
        card = {
            "background": str(kwargs.get("background", "") or ""),
            "purpose": str(kwargs.get("purpose", "") or ""),
            "how": str(kwargs.get("how", "") or ""),
            "involved": kwargs.get("involved") or [],
            "mentions": kwargs.get("mentions") or [],
            "secret_from": kwargs.get("secret_from") or [],
            "taboo": str(kwargs.get("taboo", "") or ""),
            "agreed": bool(kwargs.get("agreed", False)),
            "agreed_reason": str(kwargs.get("agreed_reason", "") or ""),
        }
        for key in ("background", "purpose", "how"):
            if not card[key]:
                return f"卡片缺少「{key}」：闹钟要讲清楚来龙去脉，被叫醒时只看卡片就要能办成事"
        if isinstance(card["involved"], str):
            card["involved"] = [card["involved"]]
        try:
            # QQ 号统一成字符串，避免数字与字符串比较失配导致保密失效
            for key in ("mentions", "secret_from"):
                card[key] = normalize_qq_ids(card[key])
        except ValueError as exc:
            return f"闹钟未创建：{exc}"

        try:
            checked, friend_warning = await validate_alarm(
                {
                    "target_umo": target_umo,
                    "run_at": run_at_str,
                    "cron_expression": cron_expression,
                    "card": card,
                },
                self.qq,
                tz,
                allow_repeat=is_admin,
                # 管理员亲口要求给名单外的会话定闹钟时照办，只提醒一句
                access=self.access,
                allow_unlisted=is_admin,
            )
        except Exception as exc:
            return f"闹钟未创建：{exc}"
        run_at_ts = checked["next_run_at"] if run_at_str else None

        # 频率上限
        deny = await self.rate_limiter.check_alarm_creation(
            origin="day", target_umo=target_umo, creator_id=sender_id, now_ts=utc_now()
        )
        if deny:
            return deny

        note_text = render_card_note(card, target_umo)
        alarm_id = await self.storage.alarms.add_alarm(
            target_umo=target_umo,
            creator_id=sender_id,
            origin="day",
            run_at=run_at_ts,
            cron_expression=cron_expression,
            card=card,
            note_text=note_text,
        )
        try:
            job = await self.context.cron_manager.add_active_job(
                name=f"astrhe闹钟#{alarm_id}",
                cron_expression=cron_expression or None,
                payload={
                    "session": target_umo,
                    "sender_id": sender_id,
                    "note": note_text,
                    "origin": "tool",
                    "astrhe_alarm_id": str(alarm_id),
                },
                description=alarm_description(card, alarm_id),
                timezone=getattr(tz, "key", None) or str(tz),
                run_once=run_at_ts is not None,
                run_at=datetime.fromtimestamp(run_at_ts, tz) if run_at_ts else None,
            )
        except Exception as exc:  # noqa: BLE001
            await self.storage.alarms.set_status(alarm_id, "failed", str(exc))
            return f"创建定时任务失败: {exc}"
        await self.storage.alarms.bind_cron_job(alarm_id, job.job_id)
        when = (
            local_datetime_str(run_at_ts, tz)
            if run_at_ts
            else f"重复规则 {cron_expression}"
        )
        followup = []
        for qq in set(map(str, expect_ids)):
            await self.storage.persons.set_expected_friend(
                qq, utc_now() + C.EXPECTED_FRIEND_TTL_SECONDS, card["purpose"]
            )
        if expect_ids and self.friend_requests is not None:
            followup = await self.friend_requests.nightly_followup()
        return (
            f"闹钟 #{alarm_id} 定好了：{when} → {target_umo}"
            + (f"；注意：{friend_warning}" if friend_warning else "")
            + ("\n" + "\n".join(followup) if followup else "")
        )

    # ---------------- list ----------------

    async def _list(self, event: Any) -> str:
        sender_id, is_admin = self._caller(event)
        current_umo = ""
        try:
            current_umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass
        viewer_id = sender_id if not self._is_cron_event(event) else ""
        present = (
            await self.secrecy_resolver.present_ids(current_umo)
            if current_umo
            else set()
        )
        tz = self._tz_getter()

        lines: list[str] = []
        alarms = await self.storage.alarms.list_alarms(100)
        for a in alarms:
            if not is_admin and a["target_umo"] != current_umo:
                continue
            card = json.loads(a["card_json"] or "{}")
            secret_from = card.get("secret_from") or []
            if not is_secret_visible_here(
                secret_from,
                viewer_id=viewer_id,
                session_umo=current_umo,
                present_ids=present,
            ):
                continue
            when = (
                local_datetime_str(a["run_at"], tz)
                if a["run_at"]
                else a["cron_expression"]
            )
            purpose = filter_secret_text(
                str(card.get("purpose", "")),
                viewer_id=viewer_id,
                session_umo=current_umo,
                present_ids=present,
            )
            lines.append(
                f"#{a['id']} [{a['status']}] {when} → {a['target_umo']}：{purpose}"
            )

        if is_admin:
            cron = getattr(self.context, "cron_manager", None)
            if cron is not None:
                try:
                    jobs = await cron.list_jobs("active_agent")
                except Exception:  # noqa: BLE001
                    jobs = []
                astrhe_ids = {a["cron_job_id"] for a in alarms if a["cron_job_id"]}
                external = [j for j in jobs if j.job_id not in astrhe_ids]
                if external:
                    lines.append("外部任务（不是 AstrHe 建的，只能查看和取消）：")
                    for j in external:
                        payload = j.payload or {}
                        lines.append(
                            f"  {j.job_id}「{j.name}」→ {payload.get('session', '')}"
                            f" enabled={j.enabled}"
                        )
        return "\n".join(lines) if lines else "现在没有可见的闹钟"

    def _is_cron_event(self, event: Any) -> bool:
        return event.get_extra("cron_payload") is not None

    async def _hidden_from_caller(self, event: Any, alarm: dict, sender_id: str) -> bool:
        """与闹钟列表同一套保密判断：对当前调用者保密的闹钟当作不存在。"""
        current_umo = ""
        try:
            current_umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass
        present = (
            await self.secrecy_resolver.present_ids(current_umo)
            if current_umo
            else set()
        )
        card = json.loads(alarm["card_json"] or "{}")
        return not is_secret_visible_here(
            card.get("secret_from") or [],
            viewer_id=sender_id if not self._is_cron_event(event) else "",
            session_umo=current_umo,
            present_ids=present,
        )

    # ---------------- edit / cancel ----------------

    async def _edit(self, event: Any, **kwargs: Any) -> str:
        sender_id, is_admin = self._caller(event)
        alarm_id = kwargs.get("alarm_id")
        try:
            alarm_id = int(alarm_id)
        except (TypeError, ValueError):
            return "edit 需要 alarm_id（闹钟编号）"
        alarm = await self.storage.alarms.get_alarm(alarm_id)
        # 保密闹钟对非管理员与不存在的编号同样回复，连「这个编号存在」也不透露；
        # 管理员报得出编号就能处理（列表里仍然看不到，惊喜不会被剧透）
        if alarm is None or (
            not is_admin and await self._hidden_from_caller(event, alarm, sender_id)
        ):
            return f"找不到闹钟 #{alarm_id}"
        deny = self._check_manage(event, alarm, is_admin)
        if deny:
            return deny

        updates: dict[str, Any] = {}
        card = json.loads(alarm["card_json"] or "{}")
        for key in ("background", "purpose", "how", "taboo", "agreed_reason"):
            if kwargs.get(key):
                card[key] = str(kwargs[key])
        if "agreed" in kwargs:
            card["agreed"] = bool(kwargs["agreed"])
        for key in ("involved", "mentions", "secret_from"):
            if kwargs.get(key):
                value = kwargs[key]
                card[key] = [value] if isinstance(value, str) else value
        try:
            for key in ("mentions", "secret_from"):
                card[key] = normalize_qq_ids(card.get(key))
        except ValueError as exc:
            return f"闹钟未修改：{exc}"
        note_text = render_card_note(card, alarm["target_umo"])
        run_at_str = str(kwargs.get("run_at", "") or "")
        tz = self._tz_getter()
        if run_at_str:
            try:
                run_at_ts = validate_alarm_time(run_at_str, "", card, tz)
            except ValueError as exc:
                return str(exc)
            if run_at_ts <= utc_now() + C.ALARM_MIN_LEAD_SECONDS:
                return "执行时间必须晚于现在 5 分钟以后"
            updates["run_at"] = run_at_ts
            if alarm["cron_job_id"]:
                try:
                    job = await self.context.cron_manager.add_active_job(
                        name=f"astrhe闹钟#{alarm_id}",
                        cron_expression=None,
                        payload={
                            "session": alarm["target_umo"],
                            "sender_id": alarm["creator_id"],
                            "note": note_text,
                            "origin": "tool",
                            "astrhe_alarm_id": str(alarm_id),
                        },
                        description=alarm_description(card, alarm_id),
                        timezone=getattr(tz, "key", None) or str(tz),
                        run_once=True,
                        run_at=datetime.fromtimestamp(run_at_ts, tz),
                    )
                except Exception as exc:  # noqa: BLE001
                    return f"修改定时任务失败，旧闹钟已保留: {exc}"
                try:
                    await self.context.cron_manager.delete_job(alarm["cron_job_id"])
                except Exception as exc:
                    # 删除旧任务失败时撤销新任务，避免出现两个提醒。
                    try:
                        await self.context.cron_manager.delete_job(job.job_id)
                    except Exception as rollback_exc:
                        return (
                            f"修改失败：删除旧任务失败（{exc}），"
                            f"新任务 {job.job_id} 也未能撤销（{rollback_exc}），请管理员检查"
                        )
                    return f"修改失败，旧闹钟已保留：{exc}"
                await self.storage.alarms.bind_cron_job(alarm_id, job.job_id)
                updates["cron_expression"] = ""
        if not run_at_str and alarm["cron_expression"]:
            try:
                validate_alarm_time(
                    "", alarm["cron_expression"], card, tz, allow_repeat=is_admin
                )
            except ValueError as exc:
                return str(exc)
        if alarm["cron_job_id"] and not run_at_str:
            # 只改卡片：先同步平台任务里的 note，成功后再改本地，避免唤醒时仍用旧卡片
            try:
                job_list = await self.context.cron_manager.list_jobs("active_agent")
                job = next(
                    (j for j in job_list if j.job_id == alarm["cron_job_id"]), None
                )
                if job is None:
                    return "修改失败：平台上找不到这个闹钟的定时任务，卡片未改"
                payload = dict(job.payload or {})
                payload["note"] = note_text
                updated = await self.context.cron_manager.update_job(
                    alarm["cron_job_id"],
                    payload=payload,
                    description=alarm_description(card, alarm_id),
                )
                if updated is None:
                    return "修改失败：平台没有更新这个定时任务，卡片未改"
            except Exception as exc:  # noqa: BLE001
                return f"修改失败，卡片未改：{exc}"
        # 更新卡片与 note
        await self.storage.db.execute(
            "UPDATE alarms SET card_json=?, note_text=?, run_at=COALESCE(?, run_at),"
            " cron_expression=COALESCE(?, cron_expression)"
            " WHERE id=?",
            (
                json.dumps(card, ensure_ascii=False),
                note_text,
                updates.get("run_at"),
                updates.get("cron_expression"),
                alarm_id,
            ),
        )
        return f"闹钟 #{alarm_id} 已更新"

    async def _cancel(self, event: Any, **kwargs: Any) -> str:
        sender_id, is_admin = self._caller(event)
        alarm_id = kwargs.get("alarm_id")
        job_id = str(kwargs.get("job_id", "") or "")
        if job_id:
            # 取消外部任务：只有管理员可以
            if not is_admin:
                return "外部任务只有管理员能取消"
            try:
                await self.context.cron_manager.delete_job(job_id)
            except Exception as exc:  # noqa: BLE001
                return f"取消失败: {exc}"
            return f"外部任务 {job_id} 已取消"
        try:
            alarm_id = int(alarm_id)
        except (TypeError, ValueError):
            return "cancel 需要 alarm_id（AstrHe 闹钟编号）或 job_id（外部任务）"
        alarm = await self.storage.alarms.get_alarm(alarm_id)
        # 保密闹钟对非管理员与不存在的编号同样回复，连「这个编号存在」也不透露；
        # 管理员报得出编号就能处理（列表里仍然看不到，惊喜不会被剧透）
        if alarm is None or (
            not is_admin and await self._hidden_from_caller(event, alarm, sender_id)
        ):
            return f"找不到闹钟 #{alarm_id}"
        deny = self._check_manage(event, alarm, is_admin)
        if deny:
            return deny
        if alarm["cron_job_id"]:
            try:
                await self.context.cron_manager.delete_job(alarm["cron_job_id"])
            except Exception as exc:  # noqa: BLE001
                # 平台任务没删掉就不能标记已取消，否则重启后它会恢复
                return f"取消失败，闹钟保持原状态：{exc}"
        await self.storage.alarms.set_status(alarm_id, "cancelled")
        return f"闹钟 #{alarm_id} 已取消"

    def _check_manage(self, event: Any, alarm: dict, is_admin: bool) -> str:
        """非管理员只能操作当前会话的 AstrHe 闹钟；返回空串表示允许。

        管理员亲自安排的闹钟只有管理员能改或取消，群友的话不算（与夜间撤销
        必须附管理员依据的规则一致）。
        """
        if is_admin:
            return ""
        current_umo = ""
        try:
            current_umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass
        if alarm["target_umo"] != current_umo:
            return "只能操作当前会话的闹钟"
        admins = {str(x) for x in (self._admins_getter() or []) if str(x).isdigit()}
        if str(alarm["creator_id"]) in admins:
            return "这是管理员亲自安排的闹钟，只有管理员能修改或取消"
        return ""
