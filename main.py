"""AstrHe 插件入口：薄入口，只装配 runtime 与注册钩子、指令。

业务逻辑全部在 astrhe/ 包里。
"""

from __future__ import annotations

import sys

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.star.filter.permission import PermissionType

try:
    from .astrhe.runtime import AstrHeRuntime
except ImportError:  # 作为顶层模块被直接导入时（如测试）
    from astrhe.runtime import AstrHeRuntime


class AstrHe(Star):
    """AstrHe 插件入口类。"""

    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.runtime = AstrHeRuntime(context=context, config=config, logger=logger)

    async def initialize(self) -> None:
        await self.runtime.initialize()

    async def terminate(self) -> None:
        await self.runtime.terminate()

    # ---------------- 钩子 ----------------

    @filter.platform_adapter_type(
        filter.PlatformAdapterType.ALL, priority=sys.maxsize - 3
    )
    async def record_all_events(self, event: AstrMessageEvent) -> None:
        """记录消息 / notice / request 进聊天账本（不产出结果，不 stop 事件）。"""
        await self.runtime.handle_event(event)

    @filter.after_message_sent(priority=sys.maxsize - 3)
    async def record_bot_sent(self, event: AstrMessageEvent) -> None:
        """记录 bot 发出的普通回复（临时记录，夜间补拉合并）。"""
        await self.runtime.recorder.record_sent(event)

    @filter.on_decorating_result(priority=sys.maxsize - 3)
    async def remember_full_reply(self, event: AstrMessageEvent) -> None:
        """在分段等装饰插件拆分之前记下整条回复，发送后按整条入账。"""
        self.runtime.recorder.remember_reply(event)

    @filter.on_llm_request(priority=sys.maxsize - 3)
    async def inject_plan(self, event: AstrMessageEvent, req) -> None:
        """注入该会话的待办与规划段落，并隐藏自带 future_task。"""
        await self.runtime.injector.on_llm_request(event, req)

    @filter.on_agent_begin()
    async def track_agent_begin(self, event: AstrMessageEvent, run_context) -> None:
        await self.runtime.alarm_tracker.on_agent_begin(event)

    @filter.on_llm_tool_respond()
    async def track_tool_respond(
        self, event: AstrMessageEvent, tool, tool_args, tool_result
    ) -> None:
        # sent_tracker 自己吞掉异常，放前面，不受闹钟记录出错影响
        await self.runtime.sent_tracker.on_tool_respond(
            event, tool, tool_args, tool_result
        )
        await self.runtime.alarm_tracker.on_llm_tool_respond(
            event, tool, tool_args, tool_result
        )

    @filter.on_agent_done()
    async def track_agent_done(self, event: AstrMessageEvent, run_context, response) -> None:
        await self.runtime.alarm_tracker.on_agent_done(event, run_context)
        await self.runtime.sent_tracker.on_agent_done(event)

    # ---------------- 指令（仅管理员） ----------------

    @filter.command_group("astrhe")
    def astrhe_group(self):
        pass

    @filter.permission_type(PermissionType.ADMIN)
    @astrhe_group.command("run")
    async def cmd_run(self, event: AstrMessageEvent) -> None:
        """立即运行一次夜间复盘并提交（后台执行）。"""
        yield event.plain_result(await self.runtime.cmd_run("manual"))

    @filter.permission_type(PermissionType.ADMIN)
    @astrhe_group.command("dryrun")
    async def cmd_dryrun(self, event: AstrMessageEvent) -> None:
        """演练一次夜间复盘，不提交（后台执行）。"""
        yield event.plain_result(await self.runtime.cmd_run("dry"))

    @filter.permission_type(PermissionType.ADMIN)
    @astrhe_group.command("status")
    async def cmd_status(self, event: AstrMessageEvent) -> None:
        """查看最近一次运行的摘要。"""
        yield event.plain_result(await self.runtime.cmd_status())

    @filter.permission_type(PermissionType.ADMIN)
    @astrhe_group.command("stop")
    async def cmd_stop(self, event: AstrMessageEvent) -> None:
        """停止正在进行的夜间运行。"""
        yield event.plain_result(await self.runtime.cmd_stop())
