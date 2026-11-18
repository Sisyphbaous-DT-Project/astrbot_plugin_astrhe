"""外部工具代理与黑名单过滤（夜间 agent 用）。

- 黑名单：直接发消息类工具、future_task、transfer_to_* 转交工具、
  astrhe_ 前缀的白天工具（只读的 astrhe_qq_lookup / astrhe_read_plan 除外）。
- 代理：把 None / MessageEventResult 结果转成文字，并调用 event.clear_result()，
  防止执行器触发真实发送（astr_agent_tool_exec.py:716-731）。
- 外部工具超时沿用 AstrBot 配置的工具超时（4.30 键 agent.config.misc.tool_call_timeout，
  旧版 agent_runner.config.misc.tool_call_timeout，缺省 120 秒）；超时只返回文字说明，不终止当晚运行。
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any

from .. import constants as C

try:
    from astrbot.api import FunctionTool
except Exception:  # pragma: no cover
    FunctionTool = None  # type: ignore[assignment]

try:
    from astrbot.core.message.message_event_result import (
        CommandResult,
        MessageEventResult,
    )
except Exception:  # pragma: no cover
    MessageEventResult = None  # type: ignore[assignment]
    CommandResult = None  # type: ignore[assignment]


def is_blacklisted(name: str) -> bool:
    """夜间 agent 工具黑名单判断。"""
    if not name:
        return True
    if name in C.NIGHT_TOOL_BLACKLIST_EXACT:
        return True
    if name.startswith("send_"):
        return True
    if name.startswith("transfer_to_"):
        return True
    if name.startswith("astrhe_") and name not in C.NIGHT_ASTRHE_TOOL_WHITELIST:
        return True
    return False


def get_external_tool_timeout(astrbot_config: Any) -> int:
    """取 AstrBot 配置的外部工具超时（秒），取不到用默认 120。

    4.30 起配置键从 agent_runner.config.misc.tool_call_timeout
    改为 agent.config.misc.tool_call_timeout，两版都试。
    """
    for key in ("agent", "agent_runner"):
        try:
            cfg = astrbot_config.get(key, {}) or {}
            misc = cfg.get("config", {}).get("misc", {}) or {}
            value = misc.get("tool_call_timeout")
            if isinstance(value, int) and value > 0:
                return value
        except Exception:  # noqa: BLE001
            pass
    return C.EXTERNAL_TOOL_TIMEOUT_DEFAULT


def _tool_base_class():
    return FunctionTool if FunctionTool is not None else object


@dataclass
class ProxiedTool(_tool_base_class()):  # type: ignore[misc]
    """外部工具的夜间代理。重写 call（拿 run_context），内部转发给原工具。"""

    inner: Any = None
    timeout: int = C.EXTERNAL_TOOL_TIMEOUT_DEFAULT
    name: str = ""
    description: str = ""
    parameters: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.inner is not None:
            self.name = self.inner.name
            self.description = self.inner.description
            self.parameters = self.inner.parameters

    async def call(self, context: Any, **kwargs: Any) -> str:
        event = context.context.event
        try:
            result = await asyncio.wait_for(
                self._invoke_inner(context, event, **kwargs),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError:
            self._clear_event_result(event)
            return f"工具 {self.name} 执行超时（{self.timeout} 秒），请换个方式继续"
        except Exception as exc:  # noqa: BLE001
            self._clear_event_result(event)
            return f"工具 {self.name} 执行失败: {exc}"
        self._clear_event_result(event)
        if result is None:
            return "工具已执行，无文字结果"
        if isinstance(result, str):
            return result if result.strip() else "工具已执行，无文字结果"
        # MessageEventResult / CallToolResult 等一律转成文字
        return f"工具已执行: {result}"

    async def _invoke_inner(self, context: Any, event: Any, **kwargs: Any) -> Any:
        inner = self.inner
        if inner is None:
            return "代理的内部工具缺失"
        if inner.handler:
            return await self._maybe_consume_gen(inner.handler(event, **kwargs))
        # 内部工具重写了 call
        for ty in type(inner).mro():
            if "call" in ty.__dict__ and ty.__dict__["call"] is not getattr(
                _tool_base_class(), "call", None
            ):
                return await self._maybe_consume_gen(inner.call(context, **kwargs))
        if hasattr(inner, "run"):
            return await self._maybe_consume_gen(inner.run(event, **kwargs))
        return "内部工具没有可调用的入口"

    async def _maybe_consume_gen(self, value: Any) -> Any:
        """内部工具若是异步生成器（yield MessageEventResult），消费并转成文字。

        不调用 event.set_result：这些「直接发给用户」的结果在夜间一律丢弃。
        """
        if not inspect.isasyncgen(value):
            if inspect.iscoroutine(value):
                return await value
            return value
        texts: list[str] = []
        async for item in value:
            if item is None:
                continue
            if MessageEventResult is not None and isinstance(
                item, MessageEventResult | CommandResult
            ):
                texts.append("（一条原本要直接发送的消息，夜间已拦截）")
            else:
                texts.append(str(item))
        return "\n".join(texts) if texts else None

    @staticmethod
    def _clear_event_result(event: Any) -> None:
        try:
            event.clear_result()
        except Exception:  # noqa: BLE001
            pass


def build_night_toolset(
    all_tools: list[Any],
    internal_tools: list[Any],
    astrbot_config: Any,
) -> tuple[list[Any], list[str]]:
    """组装夜间工具集：AstrHe 内部工具原样 + 外部工具套代理，按黑名单过滤。

    返回 (工具列表, 被排除的工具名列表)。
    """
    timeout = get_external_tool_timeout(astrbot_config)
    internal_names = {t.name for t in internal_tools}
    result = list(internal_tools)
    excluded: list[str] = []
    for tool in all_tools or []:
        name = getattr(tool, "name", "")
        if not getattr(tool, "active", True) or not name or name in internal_names:
            continue
        if is_blacklisted(name):
            excluded.append(name)
            continue
        result.append(ProxiedTool(inner=tool, timeout=timeout))
    return result, excluded
