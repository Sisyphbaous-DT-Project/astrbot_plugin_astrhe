"""会话人格：找出白天用的人格与夜间复盘人格不同的会话。

AstrBot 允许不同会话用不同人格（会话管理规则 > 对话自带人格 > 配置文件默认人格），
夜间复盘只用插件设置里选的人格。人格不同的会话：汇报员换成该会话的人格，
规划与审稿拿到人格对照，白天提醒也不注入这些会话。
查询失败一律按「与设置人格相同」处理，保持原有行为。
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field, replace
from typing import Any

from astrbot.api import logger

#: 夜间汇报读的都是 aiocqhttp 账本；platform_name 只影响 webchat 的特殊默认人格
_PLATFORM_NAME = "aiocqhttp"
_LOOKUP = object()

#: resolve_selected_persona 是否还接受 conversation_persona_id（AstrBot 4.29.x）。
#: 4.30 起会话人格写进有效配置，接口只收 umo/platform_name 并返回三元组；
#: 旧版返回四元组。两版的「webchat 特殊默认人格」标志都在返回值末位。
_WANTS_CONVERSATION_PARAM: dict[int, bool] = {}


@dataclass(frozen=True)
class PersonaInfo:
    """一个会话实际生效的人格。none=True 表示白天在这里说话时不带任何人格提示词。"""

    name: str
    prompt: str
    none: bool = False
    #: 只用于比较：设置人格留空（或不存在）时，AstrBot 白天给「走默认」的会话解析出的人格。
    #: 它可能与插件读到的默认人格不同（例如 beta.1 库里没有名为 default 的人格时，
    #: 白天不带人格），比较时以它为准，免得把所有会话都判成人格不同。
    match: PersonaInfo | None = None

    def _key(self) -> tuple[bool, str]:
        base = self.match or self
        return base.none, base.prompt.strip()

    def same_as(self, other: PersonaInfo) -> bool:
        return self._key() == other._key()

    @property
    def label(self) -> str:
        if self.none:
            return "没有设置人格"
        return f"人格「{self.name}」" if self.name else "未命名人格"


@dataclass
class SessionPersonas:
    """夜间人格与各会话人格的对照；differing 只放人格不同的会话。"""

    selected: PersonaInfo
    differing: dict[str, PersonaInfo] = field(default_factory=dict)

    def for_umo(self, umo: str) -> PersonaInfo:
        return self.differing.get(umo, self.selected)

    def differs(self, umo: str) -> bool:
        return umo in self.differing


def _fields(persona: Any) -> tuple[str, str]:
    if isinstance(persona, dict):
        return str(persona.get("name", "") or ""), str(persona.get("prompt", "") or "")
    return (
        str(getattr(persona, "name", "") or ""),
        str(getattr(persona, "prompt", "") or ""),
    )


async def selected_persona(
    context: Any, persona_id: str = "", umo: str = "", *, warn: bool = True
) -> PersonaInfo:
    """插件设置里选的人格；为空或不存在时回退 AstrBot 默认人格。

    白天每次请求都会读，传 warn=False 免得刷日志。
    """
    manager = context.persona_manager
    persona = None
    fallback = False
    if persona_id:
        try:
            persona = manager.get_persona_v3_by_id(persona_id)
            if inspect.isawaitable(persona):
                persona = await persona
        except Exception as exc:
            logger.warning(f"[AstrHe] 读取选定人格失败：{exc}")
    if persona is None:
        if warn:
            logger.warning("[AstrHe] 未选择人格或人格不存在，使用 AstrBot 默认人格")
        persona = await manager.get_default_persona_v3(umo or None)
        fallback = True
    name, prompt = _fields(persona)
    info = PersonaInfo(name=name, prompt=prompt)
    if fallback:
        daytime = await session_persona(context, umo, conversation=None)
        if daytime is not None:
            info = replace(info, match=daytime)
    return info


async def _current_conversation(context: Any, umo: str) -> Any:
    manager = context.conversation_manager
    cid = await manager.get_curr_conversation_id(umo)
    if not cid:
        return None
    return await manager.get_conversation(umo, cid)


def _wants_conversation_param(fn: Any) -> bool:
    """探测 resolve_selected_persona 是否接受 conversation_persona_id。"""
    key = id(getattr(fn, "__func__", fn))
    cached = _WANTS_CONVERSATION_PARAM.get(key)
    if cached is None:
        try:
            cached = "conversation_persona_id" in inspect.signature(fn).parameters
        except (TypeError, ValueError):
            cached = False
        _WANTS_CONVERSATION_PARAM[key] = cached
    return cached


def _webchat_default_prompt() -> str:
    """面板网页对话的特殊默认人格提示词；取不到时用固定占位（比较用）。"""
    try:
        from astrbot.core.astr_agent_runtime.main_agent.persona import (
            CHATUI_SPECIAL_DEFAULT_PERSONA_PROMPT,
        )

        return CHATUI_SPECIAL_DEFAULT_PERSONA_PROMPT
    except Exception:  # noqa: BLE001 - 4.29 里常量在别处，占位文本仅供比较
        return "__astrhe_webchat_default__"


async def session_persona(
    context: Any,
    umo: str,
    *,
    conversation: Any = _LOOKUP,
    platform_name: str = _PLATFORM_NAME,
) -> PersonaInfo | None:
    """按 AstrBot 自己的规则解析会话人格；查不到返回 None。

    conversation 不传时自己读当前对话（夜间）；白天直接传请求里的对话。
    4.30 从有效配置解析，旧 conversation 人格不再覆盖配置。
    """
    try:
        if conversation is _LOOKUP:
            conversation = await _current_conversation(context, umo)
        manager = context.persona_manager
        kwargs: dict[str, Any] = {"umo": umo, "platform_name": platform_name}
        if _wants_conversation_param(manager.resolve_selected_persona):
            kwargs["conversation_persona_id"] = getattr(
                conversation, "persona_id", None
            )
        result = await manager.resolve_selected_persona(**kwargs)
        persona_id = result[0] if result else ""
        persona = result[1] if len(result) > 1 else None
        use_webchat_default = bool(result[-1]) if result else False
    except Exception as exc:  # noqa: BLE001 - 查不到就按设置人格处理
        logger.debug(f"[AstrHe] 读取会话人格失败（{umo}）：{exc}")
        return None
    if persona:
        name, prompt = _fields(persona)
        return PersonaInfo(name=name or str(persona_id or ""), prompt=prompt)
    if use_webchat_default:
        # 面板网页对话的特殊默认人格：白天带一段专用提示词，不算「没有人格」
        return PersonaInfo(name="_chatui_default_", prompt=_webchat_default_prompt())
    # 明确不用人格，或所选人格已被删除：白天在这里说话时不带人格提示词
    return PersonaInfo(name=str(persona_id or ""), prompt="", none=True)


async def resolve_session_personas(
    context: Any, selected: PersonaInfo, umos: list[str]
) -> SessionPersonas:
    result = SessionPersonas(selected)
    for umo in umos:
        info = await session_persona(context, umo)
        if info is not None and not info.same_as(selected):
            result.differing[umo] = info
    return result
