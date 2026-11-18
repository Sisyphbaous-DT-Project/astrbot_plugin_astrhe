"""会话名单：决定 AstrHe 能不能碰某个会话（记录、补拉、复盘、定闹钟）。

补拉和旧版闹钟不经过消息流水线；新版闹钟排队期间也可能改配置。各入口统一在这里核对。

判断顺序：
1. AstrBot 会话停用：整体停用（4.30 的 id_blacklist / 旧版会话管理）、或单独
   对这个会话停用了本插件的（disabled_plugins / plugin_set），不碰；
   管理员私聊与 webchat 不豁免停用。
2. 插件黑名单优先于插件白名单。白天管理员明确安排的例外仍须符合宿主访问规则。
3. 面板网页对话豁免白名单；管理员私聊的宿主豁免取目标配置。
4. AstrBot 白名单：开了且名单非空时，会话必须在里面——这是最外层限制，
   插件自己的白名单只能再收窄，不能越过它。
5. 插件白名单：填写了的话以它为准再收窄一次；留空不限制。

名单条目写法与 AstrBot 白名单相同：完整会话 ID（/sid）或纯群号。
读取 AstrBot 设置失败时只按插件自己的名单判断，并在日志里警告；
这种「读取未知」绝不当作明确名单外去自动取消旧闹钟。
"""

from __future__ import annotations

from typing import Any

from .platform import build_umo, normalize_session_id, parse_umo

try:
    from astrbot.core.star.session_llm_manager import SessionServiceManager
    from astrbot.core.star.session_plugin_manager import SessionPluginManager
except Exception:  # AstrBot 4.30 起两个 Session 管理器已删除，改读有效配置
    SessionServiceManager = None  # type: ignore[assignment]
    SessionPluginManager = None  # type: ignore[assignment]

#: AstrBot 面板网页对话的平台 id；AstrBot 的白名单对它豁免
WEBCHAT_PLATFORM_ID = "webchat"

#: 插件在 AstrBot 里的名字（metadata.yaml 的 name），会话管理按它判断是否停用
PLUGIN_NAME = "astrbot_plugin_astrhe"

#: 拒绝原因（reject_reason 的返回值）；空串表示允许
REASON_PLUGIN_BLACKLIST = "plugin_blacklist"
REASON_HOST_WHITELIST = "host_whitelist"
REASON_PLUGIN_WHITELIST = "plugin_whitelist"
REASON_HOST_DISABLED = "host_disabled"


def entry_keys(entries: Any) -> set[str]:
    """名单条目规范化：完整会话 ID 统一成插件内部的写法，其余（群号）原样去空白。"""
    keys: set[str] = set()
    for item in entries or []:
        text = str(item).strip()
        if not text:
            continue
        pid, kind, sid = parse_umo(text)
        keys.add(build_umo(pid, kind, sid) if pid and kind and sid else text)
    return keys


def listed(umo: str, keys: set[str]) -> bool:
    """会话 ID 完全一致，或是群聊且群号在名单里（与 AstrBot 白名单一致）。"""
    _, kind, sid = parse_umo(umo)
    return umo in keys or (kind == "GroupMessage" and sid in keys)


class SessionAccess:
    """会话名单判定。所有入口共用一个实例。"""

    def __init__(
        self,
        context: Any,
        config: Any,
        admins_getter,
        logger: Any,
        platform_id_getter=None,
        plugin_name: str = PLUGIN_NAME,
    ):
        self.context = context
        self.config = config if config is not None else {}
        self._admins_getter = admins_getter
        self.logger = logger
        self._platform_id_getter = platform_id_getter
        self.plugin_name = plugin_name
        self._warned: set[str] = set()

    def platform_id(self) -> str:
        """没有当前会话时（夜间、只给了群号）拼会话 ID 用的平台 id。"""
        try:
            return (
                str(self._platform_id_getter() or "")
                if self._platform_id_getter
                else ""
            )
        except Exception:  # noqa: BLE001
            return ""

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.logger.warning(message)

    def _keys(self, name: str) -> set[str]:
        try:
            return entry_keys(self.config.get(name, []))
        except Exception:  # noqa: BLE001
            return set()

    def target_admins(self, umo: str) -> set[str] | None:
        """按目标使用的配置判断权限；读失败不是空名单，不能据此删除已有安排。"""
        try:
            cfg = self.context.get_config(umo=umo)
            return {str(x) for x in cfg.get("admins_id", []) or []}
        except Exception as exc:  # noqa: BLE001
            self._warn_once("admins", f"[AstrHe] 无法读取目标会话的管理员配置: {exc}")
            return None

    def _is_admin_private(self, umo: str) -> bool:
        _, kind, sid = parse_umo(umo)
        return kind == "FriendMessage" and sid in (self.target_admins(umo) or ())

    def _astrbot_whitelist_allows(self, umo: str, *, admin: bool = False) -> bool:
        try:
            settings = self.context.get_config(umo=umo)["platform_settings"]
            if not settings.get("enable_id_white_list", False):
                return True
            keys = {
                str(x).strip()
                for x in settings.get("id_whitelist", [])
                if str(x).strip()
            }
            _, kind, sid = parse_umo(umo)
            if admin and settings.get(
                "wl_ignore_admin_on_group"
                if kind == "GroupMessage"
                else "wl_ignore_admin_on_friend",
                True,
            ):
                return True
        except Exception as exc:  # noqa: BLE001
            self._warn_once(
                "whitelist",
                f"[AstrHe] 读取 AstrBot 白名单失败，只按插件自己的会话名单判断: {exc}",
            )
            return True
        return (
            not keys
            or umo in keys
            or (kind == "GroupMessage" and normalize_session_id(kind, sid) in keys)
        )

    def _list_reject_reason(self, umo: str) -> str:
        """只看名单（黑名单、管理员私聊、宿主白名单、插件白名单）；空串表示允许。"""
        pid, kind, sid = parse_umo(umo)
        if not (pid and kind and sid):
            return ""
        canonical = build_umo(pid, kind, sid)
        if listed(canonical, self._keys("session_blacklist")):
            return REASON_PLUGIN_BLACKLIST
        if pid == WEBCHAT_PLATFORM_ID:
            return ""  # 与 AstrBot 一致：名单级豁免（停用不豁免，见 _session_enabled）
        private_admin = self._is_admin_private(umo)
        if not self._astrbot_whitelist_allows(umo, admin=private_admin):
            return REASON_HOST_WHITELIST
        if private_admin:
            return ""
        whitelist = self._keys("session_whitelist")
        if whitelist and not listed(canonical, whitelist):
            return REASON_PLUGIN_WHITELIST
        return ""

    def list_allows(self, umo: str) -> bool:
        """只看名单（黑名单、管理员私聊、宿主/插件白名单），不查会话停用；
        同步，可逐条消息调用。到达的消息已经过 AstrBot 的停用过滤。"""
        return not self._list_reject_reason(umo)

    async def _session_enabled(self, umo: str) -> bool:
        """AstrBot 会话停用：整体停用，或单独对这个会话停用了本插件。

        4.29 走两个 Session 管理器；4.30 起改读该会话的有效配置
        （platform_settings.id_blacklist / disabled_plugins / plugin_set），
        与宿主 waking_check、session_status_check 同一套规则。
        """
        if SessionServiceManager is not None and SessionPluginManager is not None:
            try:
                if not await SessionServiceManager.is_session_enabled(umo):
                    return False
                return await SessionPluginManager.is_plugin_enabled_for_session(
                    umo, self.plugin_name
                )
            except Exception as exc:  # noqa: BLE001
                self._warn_once(
                    "session",
                    f"[AstrHe] 读取 AstrBot 会话管理失败，只按插件自己的会话名单判断: {exc}",
                )
                return True
        try:
            config = self.context.get_config(umo=umo)
        except Exception as exc:  # noqa: BLE001
            self._warn_once(
                "session",
                f"[AstrHe] 读取 AstrBot 会话配置失败，只按插件自己的会话名单判断: {exc}",
            )
            return True
        try:
            settings = config.get("platform_settings", {}) or {}
            blacklist = {
                str(item).strip() for item in settings.get("id_blacklist", []) or []
            }
            if blacklist:
                _, kind, sid = parse_umo(umo)
                if umo in blacklist or (
                    kind == "GroupMessage"
                    and normalize_session_id(kind, sid) in blacklist
                ):
                    return False  # 整体停用，管理员与 webchat 都不豁免
            disabled = {str(x) for x in config.get("disabled_plugins", []) or []}
            if self.plugin_name in disabled:
                return False
            enabled = [str(x) for x in config.get("plugin_set", ["*"]) or []]
            # 与宿主一致：plugin_set 空列表等于全部禁用；"*" 或显式包含才启用
            if (
                enabled != ["*"]
                and self.plugin_name not in enabled
                and not (disabled and "*" in enabled)
            ):
                return False
        except Exception as exc:  # noqa: BLE001
            self._warn_once(
                "session",
                f"[AstrHe] 解析 AstrBot 会话配置失败，只按插件自己的会话名单判断: {exc}",
            )
            return True
        return True

    async def _reject_reason(self, umo: str) -> str:
        """完整判断（名单 + 会话停用）的拒绝原因；空串表示允许。"""
        pid, kind, sid = parse_umo(umo)
        if not (pid and kind and sid):
            return ""
        # 新宿主按原始 UMO 匹配；旧版管理器也可能留有按整群设置的停用项。
        keys = (
            (umo, build_umo(pid, kind, sid))
            if SessionServiceManager is not None
            else (umo,)
        )
        for key in dict.fromkeys(keys):
            if not await self._session_enabled(key):
                return REASON_HOST_DISABLED
        return self._list_reject_reason(umo)

    async def allows_alarm(self, umo: str, *, day_admin: bool = False) -> bool:
        """管理员亲自安排的闹钟只豁免插件名单，宿主仍按该事件的权限把关。"""
        if not day_admin:
            return await self.allows(umo)
        if await self.reject_reason(umo) == REASON_HOST_DISABLED:
            return False
        return self._astrbot_whitelist_allows(umo, admin=True)

    async def allows(self, umo: str) -> bool:
        """完整判断：名单 + AstrBot 会话停用。

        有原始「发言人_群号」的入口应直接传入，只有插件名单按群归一。
        """
        return not await self._reject_reason(umo)

    async def reject_reason(self, umo: str) -> str:
        """完整判断的拒绝原因；空串表示允许。

        区分「名单外」（host_whitelist / plugin_whitelist）与「宿主停用」
        （host_disabled）：只有插件名单或宿主明确豁免的名单可以照办，停用不能越过。
        """
        return await self._reject_reason(umo)

    def restricted(self) -> bool:
        """有没有设置任何名单：插件黑白名单，或默认配置里生效的 AstrBot
        白名单 / 会话黑名单（4.30 的 id_blacklist）。"""
        if self._keys("session_blacklist") or self._keys("session_whitelist"):
            return True
        try:
            settings = self.context.get_config()["platform_settings"]
            if settings.get("enable_id_white_list", False) and entry_keys(
                settings.get("id_whitelist", [])
            ):
                return True
            return bool(entry_keys(settings.get("id_blacklist", [])))
        except Exception:  # noqa: BLE001
            return False

    async def filter_umos(self, umos: list[str]) -> list[str]:
        return [umo for umo in umos if await self.allows(umo)]
