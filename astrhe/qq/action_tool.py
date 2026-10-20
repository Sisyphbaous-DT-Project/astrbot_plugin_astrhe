"""astrhe_qq_act：QQ「活人感」动作工具（白天给 bot 自主使用，受频率上限约束）。

action: react（贴表情）/ poke（戳一戳）/ like（名片点赞）/
        signature（改个性签名）/ status（改在线状态）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .. import constants as C
from ..platform import event_umo, parse_umo
from ..timeutil import utc_now
from .emoji_table import resolve_emoji, supported_meanings
from .lookup_tool import UNLISTED_GROUP, group_allowed

try:
    from astrbot.api import FunctionTool
except Exception:  # pragma: no cover
    FunctionTool = None  # type: ignore[assignment]

TOOL_NAME = "astrhe_qq_act"

#: set_online_status 的常用预设：名称 -> (status, ext_status)
_ONLINE_PRESETS = {
    "在线": (10, 0),
    "离开": (30, 0),
    "忙碌": (50, 0),
    "请勿打扰": (70, 0),
    "Q我吧": (60, 0),
}

_ACTIONS = (
    "react（给一条消息贴表情回应，message_id 不传默认当前消息，参数 emoji；emoji 可以填含义如「赞」「爱心」或编号）\n"
    "poke（戳一戳某人，参数 user_id，群里加 group_id）\n"
    "like（给某人名片点赞，参数 user_id、times（1-10））\n"
    "signature（改我的个性签名，参数 text；所有人都看得到，"
    f"{C.RATE_SIGNATURE_INTERVAL_DAYS} 天内最多换一次，"
    "白天和夜间复盘共用，没有值得挂出来的心情或大事就别换）\n"
    "status（改我的在线状态，参数 preset（在线/离开/忙碌/请勿打扰/Q我吧）"
    "或 wording（自定义状态文字，此时 face_id 必填））"
)


def _tool_base_class():
    return FunctionTool if FunctionTool is not None else object


@dataclass
class QQActionTool(_tool_base_class()):  # type: ignore[misc]
    """QQ 动作工具，带频率上限。"""

    qq_client: Any = None
    rate_limiter: Any = None
    access: Any = None
    name: str = TOOL_NAME
    description: str = (
        "QQ 小动作工具（用来表达情绪和存在感，要有分寸，别频繁用）。可用动作：\n"
        + _ACTIONS
        + "\n支持的表情含义："
        + supported_meanings()
    )
    parameters: dict[str, Any] = field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "action": {"type": "string", "description": "动作名，见工具描述"},
                "message_id": {"type": "string", "description": "react 的目标消息 id"},
                "emoji": {"type": "string", "description": "react 的表情含义或编号"},
                "user_id": {"type": "string", "description": "目标 QQ 号"},
                "group_id": {"type": "string", "description": "群号（在群里戳人时用）"},
                "times": {"type": "integer", "description": "like 的点赞次数，1-10"},
                "text": {"type": "string", "description": "signature 的签名内容"},
                "preset": {"type": "string", "description": "status 的预设名"},
                "wording": {"type": "string", "description": "status 的自定义状态文字"},
                "face_id": {
                    "type": "integer",
                    "description": "status 自定义状态的图标编号",
                },
            },
            "required": ["action"],
        }
    )

    async def run(self, event: Any, action: str = "", **kwargs: Any) -> str:
        umo = ""
        try:
            umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass
        if action == "react" and not kwargs.get("message_id"):
            if event.get_extra("cron_payload") is None:
                kwargs["message_id"] = str(
                    getattr(getattr(event, "message_obj", None), "message_id", "") or ""
                )
        if action == "poke" and not kwargs.get("group_id"):
            _, kind, sid = parse_umo(umo)
            if kind == "GroupMessage":
                kwargs["group_id"] = sid
        try:
            if action == "poke" and not await group_allowed(
                self.access, event, kwargs.get("group_id", "")
            ):
                return UNLISTED_GROUP
            return await self._dispatch(action, umo=umo, **kwargs)
        except Exception as exc:  # noqa: BLE001
            return f"操作失败: {exc}"

    async def _dispatch(self, action: str, *, umo: str, **kwargs: Any) -> str:
        now = utc_now()
        if action == "react":
            message_id = str(kwargs.get("message_id", ""))
            emoji = resolve_emoji(str(kwargs.get("emoji", "")))
            if not message_id:
                return "react 需要 message_id 参数"
            if emoji is None:
                return f"不认识这个表情。支持的含义：{supported_meanings()}，也可以直接填编号"
            deny = await self.rate_limiter.check("react", now, umo=umo)
            if deny:
                return deny
            ok, result = await self.qq_client.call_friendly(
                "set_msg_emoji_like", message_id=int(message_id), emoji_id=emoji
            )
            if not ok:
                return str(result)
            await self.rate_limiter.record("react", umo=umo)
            return "已贴上表情"
        if action == "poke":
            user_id = str(kwargs.get("user_id", ""))
            if not user_id:
                return "poke 需要 user_id 参数"
            deny = await self.rate_limiter.check("poke", now, target=user_id)
            if deny:
                return deny
            params: dict[str, Any] = {"user_id": int(user_id)}
            group_id = str(kwargs.get("group_id", ""))
            if group_id:
                params["group_id"] = int(group_id)
            try:
                await self.qq_client.call("send_poke", **params)
            except RuntimeError:
                return "当前环境不支持戳一戳"
            await self.rate_limiter.record("poke", target=user_id, umo=umo)
            return f"戳了 {user_id} 一下"
        if action == "like":
            user_id = str(kwargs.get("user_id", ""))
            times = int(kwargs.get("times", 1) or 1)
            if not user_id:
                return "like 需要 user_id 参数"
            times = max(1, min(times, 10))
            deny = await self.rate_limiter.check("like", now, target=user_id)
            if deny:
                return deny
            ok, result = await self.qq_client.call_friendly(
                "send_like", user_id=int(user_id), times=times
            )
            if not ok:
                return f"点赞失败: {result}"
            await self.rate_limiter.record("like", target=user_id, umo=umo)
            return f"已给 {user_id} 点了 {times} 个赞"
        if action == "signature":
            text = str(kwargs.get("text", "")).strip()
            if not text:
                return "signature 需要 text 参数"
            deny = await self.rate_limiter.check("signature", now)
            if deny:
                return deny
            await self.qq_client.call("set_self_longnick", longNick=text)
            await self.rate_limiter.record("signature", target=text)
            return f"签名已改成：{text}"
        if action == "status":
            preset = str(kwargs.get("preset", "")).strip()
            wording = str(kwargs.get("wording", "")).strip()
            deny = await self.rate_limiter.check("status", now)
            if deny:
                return deny
            if wording:
                face_id = int(kwargs.get("face_id", 0) or 0)
                if not face_id:
                    return "自定义状态文字需要 face_id（图标编号）"
                await self.qq_client.call(
                    "set_diy_online_status", face_id=face_id, wording=wording
                )
                await self.rate_limiter.record("status")
                return f"在线状态已改成：{wording}"
            if preset in _ONLINE_PRESETS:
                status, ext = _ONLINE_PRESETS[preset]
                await self.qq_client.call(
                    "set_online_status", status=status, ext_status=ext, battery_status=0
                )
                await self.rate_limiter.record("status")
                return f"在线状态已改成：{preset}"
            return "status 需要 preset（在线/离开/忙碌/请勿打扰/Q我吧）或 wording + face_id"
        return f"不认识的动作: {action}。可用动作见工具描述。"
