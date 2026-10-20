"""草稿、白天定时与提交共用的输入校验。"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from .. import constants as C
from ..platform import parse_umo, resolve_session_ref
from ..storage.alarms import is_live_alarm
from ..timeutil import is_quiet_time, parse_run_at, utc_now

try:  # 与 AstrBot 调度一致的星期换算（标准 crontab 0/7=周日）
    from astrbot.core.cron.manager import _normalize_crontab_day_of_week
except ImportError:  # pragma: no cover - 没有该函数的 AstrBot 版本按原样解析
    _normalize_crontab_day_of_week = None

ANNIVERSARY_EXAMPLE = '[{"name":"生日","date":"10-11"}]'


def cron_trigger(cron_expression: str, tz: Any) -> CronTrigger:
    """按 AstrBot 的方式解析重复规则；APScheduler 的数字星期是周一=0，需先换算。"""
    fields = str(cron_expression or "").split()
    if len(fields) != 5:
        raise ValueError("重复规则需要 5 段：分 时 日 月 星期")
    if _normalize_crontab_day_of_week is not None:
        fields[4] = _normalize_crontab_day_of_week(fields[4])
    return CronTrigger.from_crontab(" ".join(fields), timezone=tz)


def fires_on_date(cron_expression: str, date_str: str, tz: Any) -> bool:
    """重复规则在某个本地日期是否会触发。"""
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    fire = cron_trigger(cron_expression, tz).get_next_fire_time(None, day)
    return fire is not None and fire.strftime("%Y-%m-%d") == date_str


def validate_anniversaries(value: Any) -> list:
    """限制持久化格式，同时允许空列表清空纪念日。"""
    if not isinstance(value, list):
        raise ValueError(f"anniversaries 必须是数组，例如 {ANNIVERSARY_EXAMPLE}")
    for item in value:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or not item["name"].strip()
        ):
            raise ValueError(f"纪念日需要 name 和 date，例如 {ANNIVERSARY_EXAMPLE}")
        text = item.get("date")
        if not isinstance(text, str) or not re.fullmatch(
            r"\d{2}-\d{2}|\d{4}-\d{2}-\d{2}", text
        ):
            raise ValueError(
                f"纪念日日期须为 MM-DD 或 YYYY-MM-DD，例如 {ANNIVERSARY_EXAMPLE}"
            )
        try:
            datetime.strptime("2000-" + text if len(text) == 5 else text, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"纪念日日期不合法，例如 {ANNIVERSARY_EXAMPLE}") from exc
    return value


def validate_alarm_time(
    run_at: str,
    cron_expression: str,
    card: dict,
    tz: Any,
    *,
    allow_repeat: bool = True,
) -> float:
    """返回下一次触发的时间戳；重复规则检查跨完整一周的深夜时段。"""
    now = utc_now()
    agreed = card.get("agreed") and str(card.get("agreed_reason", "")).strip()
    if run_at and cron_expression:
        raise ValueError("run_at 和 cron_expression 只能选一个")
    if run_at:
        ts = parse_run_at(run_at, tz)
        if ts <= now + C.ALARM_MIN_LEAD_SECONDS:
            raise ValueError("执行时间必须晚于现在 5 分钟以后")
        if is_quiet_time(ts, tz) and not agreed:
            raise ValueError(
                "深夜免打扰（00:30-08:00）：必须 agreed=true 并写 agreed_reason"
            )
        return ts
    if not cron_expression:
        raise ValueError("需要 run_at 或 cron_expression")
    if not allow_repeat:
        raise ValueError("只有管理员或夜间主 agent 能建重复闹钟")
    try:
        trigger = cron_trigger(cron_expression, tz)
    except ValueError as exc:
        raise ValueError(f"重复规则不合法：{exc}") from exc
    cursor = datetime.fromtimestamp(now, tz)
    previous = None
    first = None
    # 一周逐分钟规则最多 10080 次；这里只校验重复频率，不限制 agent 工作量。
    for _ in range(10082):
        nxt = trigger.get_next_fire_time(previous, cursor)
        if nxt is None:
            break
        ts = nxt.timestamp()
        if first is None:
            first = ts
            if ts <= now + C.ALARM_MIN_LEAD_SECONDS:
                raise ValueError("首次执行时间必须晚于现在 5 分钟以后")
        if previous is not None and ts - previous.timestamp() < 3600:
            raise ValueError("重复闹钟相邻触发间隔不能短于 1 小时")
        if is_quiet_time(ts, tz) and not agreed:
            raise ValueError("重复规则包含深夜免打扰时段，必须填写约定理由")
        if first is not None and ts - first >= 8 * 86400:
            break
        previous = nxt
        cursor = nxt
    if first is None:
        raise ValueError("重复规则没有未来触发时间")
    return first


def _join(first: str, second: str) -> str:
    return f"{first}；{second}" if first else second


async def validate_alarm(
    payload: dict,
    qq: Any,
    tz: Any,
    *,
    allow_repeat: bool = True,
    access: Any = None,
    allow_unlisted: bool = False,
) -> tuple[dict, str]:
    """验证会话、平台、卡片和时间。私聊非好友只给提醒，不阻止创建。

    access 给出时检查会话名单：名单外的目标拒绝；allow_unlisted（管理员亲口要求）
    时照办，只在提醒里说明。
    """
    pid = qq.platform.default_platform_id()
    if not pid:
        raise ValueError("当前没有可用的 aiocqhttp 平台")
    result = dict(payload)
    result["target_umo"] = resolve_session_ref(payload.get("target_umo", ""), pid)
    _, kind, sid = parse_umo(result["target_umo"])
    warning = ""
    if access is not None and not await access.allows(result["target_umo"]):
        if not allow_unlisted:
            raise ValueError(
                f"会话 {result['target_umo']} 不在 AstrHe 的会话名单里"
                "（插件白名单、黑名单或 AstrBot 访问控制），不能给它定闹钟"
            )
        warning = "这个会话不在 AstrHe 的会话名单里，按管理员的要求照办"
    if kind == "GroupMessage":
        groups = await qq.group_ids()
        if groups is None:
            warning = _join(
                warning, "平台暂时不可用，无法确认是否在目标群；已保留闹钟安排"
            )
        elif sid not in groups:
            raise ValueError(f"我不在群 {sid} 里")
    else:
        friends = await qq.friend_ids()
        if friends is None:
            warning = _join(
                warning, "平台暂时不可用，无法确认好友关系；消息可能被 QQ 拦截"
            )
        elif sid not in friends:
            warning = _join(warning, "对方不是好友，消息可能被 QQ 拦截")
    card = result.get("card") or {}
    for key in ("background", "purpose", "how"):
        if not str(card.get(key, "")).strip():
            raise ValueError(f"闹钟卡片缺少「{key}」")
    result["next_run_at"] = validate_alarm_time(
        result.get("run_at", ""),
        result.get("cron_expression", ""),
        card,
        tz,
        allow_repeat=allow_repeat,
    )
    return result, warning


async def validate_night_alarm_frequency(
    storage, payload: dict, tz, run_id: int, active=None, *, excluded_alarm_ids=None
) -> None:
    """按本次运行和目标执行日计数，提交时再次检查长运行期间的变化。"""
    from ..timeutil import local_date_str

    active = active or []
    excluded_alarm_ids = excluded_alarm_ids or set()
    # 重复闹钟看「还会不会继续响」：某次执行中断被标为 failed 时，平台任务仍在，
    # 照常计数；一次性闹钟取消或失败就不再计数
    alarms = [
        a
        for a in await storage.alarms.list_alarms(-1)
        if a["id"] not in excluded_alarm_ids
        and (
            is_live_alarm(a)
            if a["cron_expression"]
            else a["status"] not in ("cancelled", "failed")
        )
    ]
    if (
        sum(a["created_run_id"] == run_id for a in alarms) + len(active)
        >= C.RATE_ALARM_NIGHT_PER_RUN
    ):
        raise ValueError("本晚主动闹钟已达到频率上限")
    date = local_date_str(payload["next_run_at"], tz)
    count = 0
    for alarm in alarms:
        if alarm["target_umo"] != payload["target_umo"]:
            continue
        if alarm["cron_expression"]:
            # 重复闹钟按「目标日期当天会不会触发」计数，而不是只看下一次
            if is_live_alarm(alarm) and fires_on_date(
                alarm["cron_expression"], date, tz
            ):
                count += 1
        elif (
            alarm["run_at"] is not None and local_date_str(alarm["run_at"], tz) == date
        ):
            count += 1
    for draft in active:
        if draft["target_umo"] != payload["target_umo"]:
            continue
        if draft.get("cron_expression"):
            count += fires_on_date(draft["cron_expression"], date, tz)
        else:
            count += local_date_str(draft["next_run_at"], tz) == date
    if count >= C.RATE_ALARM_SAME_TARGET_PER_DAY:
        raise ValueError("同一目标每天的主动闹钟达到频率上限")


async def validate_alarm_cancel(
    storage, alarm_id: int, *, admins=(), evidence_message_id: str = ""
) -> dict:
    """草稿与执行共用：重复任务可撤销，管理员安排必须有有效依据。"""
    alarm = await storage.alarms.get_alarm(alarm_id)
    if alarm is None:
        raise ValueError(f"找不到闹钟 #{alarm_id}")
    events = await storage.alarms.events(alarm_id)
    if alarm["cron_expression"]:
        executed = alarm["status"] == "cancelled"
    else:
        executed = alarm["status"] != "scheduled" or any(
            e["kind"] in ("fired", "manual_run", "send_ok", "send_failed", "done")
            for e in events
        )
    if executed:
        raise ValueError(f"闹钟 #{alarm_id} 已执行或不再等待执行，不能取消")
    if not alarm["cron_job_id"]:
        raise ValueError(f"闹钟 #{alarm_id} 没有关联的定时任务")
    admin_ids = {str(x) for x in admins if str(x).isdigit()}
    if alarm["creator_id"] in admin_ids:
        mid = str(evidence_message_id).strip().lstrip("#")
        if not mid:
            raise ValueError(
                "管理员安排的闹钟需要管理员本人取消或改期消息的 evidence_message_id"
            )
        message = await storage.ledger.get_message_by_platform_id(mid)
        if message is None:
            raise ValueError("账本里找不到取消依据消息")
        if message["recalled"]:
            raise ValueError("取消依据消息已经撤回")
        if message["sender_id"] not in admin_ids:
            raise ValueError("取消依据必须是管理员本人发出的消息，群友的话不算")
        if message["ts"] <= alarm["created_at"]:
            raise ValueError("取消依据消息必须晚于闹钟创建时间")
    return alarm
