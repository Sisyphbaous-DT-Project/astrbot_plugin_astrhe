"""会话名单：决定 AstrHe 能不能碰某个会话（记录、补拉、复盘、定闹钟）。

AstrHe 的补拉直接找 QQ 要历史，闹钟到点由 AstrBot 定时任务直接唤醒，都不经过
AstrBot 的消息流水线，AstrBot 的白名单管不到，所以插件自己在各个入口问一次这里。

判断顺序：
1. 插件黑名单：写进去的一律不碰（AstrBot 没有黑名单，由插件自建）。
2. AstrBot 会话管理：整体停用的会话、或单独对该会话停用了本插件的，不碰。
3. 管理员私聊、AstrBot 面板的网页对话（与 AstrBot 一样豁免白名单）：放行。
4. 插件白名单：填写了就以它为准，不再看 AstrBot 的白名单。
5. 插件白名单留空：按该会话实际使用的配置文件照 AstrBot 的规则判断；
   AstrBot 没开白名单或名单为空时不限制。

名单条目写法与 AstrBot 白名单相同：完整会话 ID（/sid）或纯群号。
读取 AstrBot 设置失败时只按插件自己的名单判断，并在日志里警告。
"""

from __future__ import annotations

from typing import Any

from .platform import build_umo, parse_umo

try:
    from astrbot.core.star.session_llm_manager import SessionServiceManager
    from astrbot.core.star.session_plugin_manager import SessionPluginManager
except Exception:  # pragma: no cover - AstrBot 内部模块改名时只按名单判断
    SessionServiceManager = None  # type: ignore[assignment]
    SessionPluginManager = None  # type: ignore[assignment]

#: AstrBot 面板网页对话的平台 id；AstrBot 的白名单对它豁免
WEBCHAT_PLATFORM_ID = "webchat"

#: 插件在 AstrBot 里的名字（metadata.yaml 的 name），会话管理按它判断是否停用
PLUGIN_NAME = "astrbot_plugin_astrhe"


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

    def _is_admin_private(self, umo: str) -> bool:
        _, kind, sid = parse_umo(umo)
        if kind != "FriendMessage":
            return False
        try:
            admins = {str(x) for x in self._admins_getter() or []}
        except Exception:  # noqa: BLE001
            admins = set()
        return sid in admins

    def _astrbot_whitelist_allows(self, umo: str) -> bool:
        try:
            settings = self.context.get_config(umo=umo)["platform_settings"]
            if not settings.get("enable_id_white_list", False):
                return True
            keys = entry_keys(settings.get("id_whitelist", []))
        except Exception as exc:  # noqa: BLE001
            self._warn_once(
                "whitelist",
                f"[AstrHe] 读取 AstrBot 白名单失败，只按插件自己的会话名单判断: {exc}",
            )
            return True
        return not keys or listed(umo, keys)

    def list_allows(self, umo: str) -> bool:
        """只看名单（黑名单、管理员私聊、白名单），不查会话管理；同步，可逐条消息调用。"""
        pid, kind, sid = parse_umo(umo)
        if not (pid and kind and sid):
            return True
        umo = build_umo(pid, kind, sid)
        if listed(umo, self._keys("session_blacklist")):
            return False
        if self._is_admin_private(umo) or pid == WEBCHAT_PLATFORM_ID:
            return True
        whitelist = self._keys("session_whitelist")
        if whitelist:
            return listed(umo, whitelist)
        return self._astrbot_whitelist_allows(umo)

    async def _session_enabled(self, umo: str) -> bool:
        """AstrBot 会话管理：整体停用，或单独对这个会话停用了本插件。"""
        try:
            if SessionServiceManager is None or SessionPluginManager is None:
                raise RuntimeError("找不到 AstrBot 的会话管理模块")
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

    async def allows(self, umo: str) -> bool:
        """完整判断：名单 + AstrBot 会话管理。

        开启会话隔离时 AstrBot 按「发言人_群号」记会话停用状态，所以有原始会话 ID 的
        入口直接把它传进来：原始写法和群号写法任何一个被停用都算停用。
        """
        if not self.list_allows(umo):
            return False
        pid, kind, sid = parse_umo(umo)
        if not (pid and kind and sid):
            return True
        for key in dict.fromkeys((build_umo(pid, kind, sid), umo)):
            if not await self._session_enabled(key):
                return False
        return True

    def restricted(self) -> bool:
        """有没有设置任何名单：插件黑白名单，或默认配置里生效的 AstrBot 白名单。"""
        if self._keys("session_blacklist") or self._keys("session_whitelist"):
            return True
        try:
            settings = self.context.get_config()["platform_settings"]
            return bool(
                settings.get("enable_id_white_list", False)
                and entry_keys(settings.get("id_whitelist", []))
            )
        except Exception:  # noqa: BLE001
            return False

    async def filter_umos(self, umos: list[str]) -> list[str]:
        return [umo for umo in umos if await self.allows(umo)]
