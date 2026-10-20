"""AstrHe 运行时：装配配置、数据库、调度器、工具注册与生命周期。"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from . import constants as C
from .access import SessionAccess
from .alarm_tracker import AlarmTracker
from .backfill import Backfiller
from .injector import Injector
from .night.pipeline import NightPipeline
from .platform import PlatformHelper
from .qq.client import QQClient
from .qq.friend_requests import FriendRequestManager
from .qq.rate_limit import RateLimiter
from .recorder import Recorder
from .sent_tracker import SentTracker
from .scheduler import NightScheduler
from .storage import Storage
from .timeutil import get_config_timezone, local_datetime_str, utc_now
from .tools.base import AstrHeFunctionTool
from .tools.day_tools import DayTools
from .tools.draft_tools import DraftTools
from .tools.reader_tools import ReaderTools
from .tools.memory_readers import PLAN_READ_PARAMETERS
from .tools.schedule_tool import ScheduleToolImpl
from .tools.secrecy import PresentResolver
from .webapi import WebApi

try:
    from astrbot.core.star.star_tools import StarTools
except Exception:  # pragma: no cover
    StarTools = None  # type: ignore[assignment]

PLUGIN_DIR_NAME = "astrbot_plugin_astrhe"


class AstrHeRuntime:
    def __init__(self, context: Any, config: Any, logger: Any):
        self.context = context
        self.config = config if config is not None else {}
        self.logger = logger

        data_dir = (
            StarTools.get_data_dir(PLUGIN_DIR_NAME)
            if StarTools is not None
            else __import__("pathlib").Path("data")
        )
        self.storage = Storage(data_dir)
        self.platform = PlatformHelper(context, logger)
        # 会话名单：所有记录、补拉、复盘、定闹钟的入口共用
        self.access = SessionAccess(
            context,
            self.config,
            self.admins,
            logger,
            platform_id_getter=self.platform.default_platform_id,
        )
        self.qq = QQClient(self.platform)
        self.rate_limiter = RateLimiter(self.storage, self.tz)
        self.secrecy = PresentResolver(self.storage, self.qq)
        self.recorder = Recorder(self.storage, logger, self.enabled, self.access)
        self.backfiller = Backfiller(self.storage, self.platform, logger, self.access)
        self.friend_requests = FriendRequestManager(self.storage, self.qq, logger)
        self.day_tools = DayTools(self.storage, self.tz, self.secrecy)
        self.schedule_impl = ScheduleToolImpl(
            self.storage,
            context,
            self.qq,
            self.rate_limiter,
            self.tz,
            self.secrecy,
            self.admins,
            self.friend_requests,
            access=self.access,
        )
        self.reader_tools = ReaderTools(
            self.storage,
            context,
            self.tz,
            self._default_window,
            self.secrecy,
            access=self.access,
        )
        self.injector = Injector(
            self.storage,
            self.tz,
            self.secrecy,
            self.enabled,
            logger,
            context=context,
            persona_id_getter=lambda: self.config.get("persona_id", ""),
            access=self.access,
        )
        self.alarm_tracker = AlarmTracker(self.storage, context, self.tz, logger)
        self.sent_tracker = SentTracker(
            self.storage,
            context,
            self.platform,
            self.tz,
            logger,
            self.enabled,
            access=self.access,
        )
        self.scheduler = NightScheduler(self.storage, self.tz, self.run_night, logger)
        self.webapi = WebApi(
            self.storage,
            context,
            self.tz,
            self.status_dict,
            self.admins,
            frontend_getter=self.frontend_choices,
        )

        self._external_tasks: set[asyncio.Task] = set()
        self._tools_registered = False
        self._current_pipeline: NightPipeline | None = None

    # ---------------- 配置 ----------------

    def enabled(self) -> bool:
        try:
            return bool(self.config.get("enabled", False))
        except Exception:  # noqa: BLE001
            return False

    def frontend_choices(self) -> tuple[str, str]:
        """档案终端横屏、竖屏各用哪套前端（非法值由接口回退默认）。"""
        return (
            self.config.get("frontend_landscape", C.FRONTEND_LANDSCAPE_DEFAULT),
            self.config.get("frontend_portrait", C.FRONTEND_PORTRAIT_DEFAULT),
        )

    def tz(self):
        try:
            cfg = self.context.get_config()
        except Exception:  # noqa: BLE001
            cfg = None
        return get_config_timezone(cfg)

    def admins(self) -> list[str]:
        try:
            ids = self.context.get_config().get("admins_id", []) or []
        except Exception:  # noqa: BLE001
            ids = []
        return [str(x) for x in ids if str(x).isdigit()]

    @staticmethod
    def _default_window() -> tuple[float, float]:
        now = utc_now()
        return now - C.COLD_START_SECONDS, now

    # ---------------- 生命周期 ----------------

    async def initialize(self) -> None:
        await self.storage.open()
        if not self.enabled():
            self.logger.info("[AstrHe] 插件未启用（enabled=false），只初始化存储")
            return
        self._register_tools()
        self.webapi.register()
        self.scheduler.start()
        await self.cancel_unlisted_alarms()
        self.logger.info("[AstrHe] 已启用：零点调度、记录器、白天注入与工具已就绪")

    async def cancel_unlisted_alarms(self) -> None:
        """启动时（包括改设置后的重载）就取消名单外的闹钟，不必等到零点对账。

        AstrBot 启动时插件先于定时任务调度加载，这里删掉的任务不会再被排上。
        """
        try:
            text = await self.alarm_tracker.cancel_unlisted(
                self.access, admins=self.admins()
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"[AstrHe] 清理会话名单外的闹钟失败: {exc}")
            return
        if text:
            self.logger.info(f"[AstrHe] {text}")

    async def terminate(self) -> None:
        await self.scheduler.stop()
        await self.sent_tracker.stop()
        # 进行中的运行由 scheduler.stop() 取消，pipeline 内部按
        # 「提交前判失败、提交后外部动作照常收尾」的规则收尾
        while self._external_tasks:
            await asyncio.gather(*self._external_tasks, return_exceptions=True)
        await self.storage.close()

    # ---------------- 工具注册 ----------------

    def _register_tools(self) -> None:
        if self._tools_registered:
            return
        add_llm_tools = getattr(self.context, "add_llm_tools", None)
        if not callable(add_llm_tools):
            self.logger.warning("[AstrHe] 找不到工具注册入口，跳过注册")
            return
        from .qq.action_tool import QQActionTool
        from .qq.lookup_tool import QQLookupTool

        todo_meta = DayTools.todo_update_tool()
        schedule_params = {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "create / list / edit / cancel",
                },
                "target_type": {
                    "type": "string",
                    "description": "current / group / private",
                },
                "target_id": {"type": "string", "description": "群号或 QQ 号"},
                "run_at": {
                    "type": "string",
                    "description": "执行时间，如 2026-10-11 00:00",
                },
                "cron_expression": {
                    "type": "string",
                    "description": "重复规则（cron 表达式）",
                },
                "background": {"type": "string", "description": "背景（必填）"},
                "purpose": {"type": "string", "description": "目的（必填）"},
                "how": {"type": "string", "description": "怎么做（必填）"},
                "involved": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "牵涉的人",
                },
                "mentions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "要 @ 的 QQ 号",
                },
                "secret_from": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "对谁保密",
                },
                "taboo": {"type": "string", "description": "禁忌"},
                "agreed": {"type": "boolean", "description": "是否事先约定好的时刻"},
                "agreed_reason": {"type": "string", "description": "约定理由"},
                "alarm_id": {
                    "type": "integer",
                    "description": "edit / cancel 时的闹钟编号",
                },
                "expect_friend_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "管理员标记预期联系人，立即检查已有申请",
                },
                "job_id": {
                    "type": "string",
                    "description": "cancel 外部任务时的 job_id",
                },
            },
            "required": ["action"],
        }
        try:
            add_llm_tools(
                AstrHeFunctionTool(
                    name=todo_meta["name"],
                    description=todo_meta["description"],
                    parameters=todo_meta["parameters"],
                    fn=self.day_tools.todo_update,
                ),
                AstrHeFunctionTool(
                    name="astrhe_read_plan",
                    description=(
                        "读规划书全文。参数：date（可选，YYYY-MM-DD，默认最近一份）。"
                        "list=true 列日期，每页 60 份，offset 翻页。对别人保密的内容会被遮挡。"
                    ),
                    parameters=PLAN_READ_PARAMETERS,
                    fn=self.reader_tools._read_plan,
                ),
                AstrHeFunctionTool(
                    name="astrhe_schedule",
                    description=(
                        "定时任务工具（替代 future_task，可指定目标会话）。"
                        "action=create 时必须写清 background / purpose / how，"
                        "被叫醒时只看卡片就要能办成事。"
                    ),
                    parameters=schedule_params,
                    fn=self.schedule_impl.run,
                ),
                QQLookupTool(qq_client=self.qq, access=self.access),
                QQActionTool(
                    qq_client=self.qq,
                    rate_limiter=self.rate_limiter,
                    access=self.access,
                ),
            )
            self._tools_registered = True
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"[AstrHe] 注册工具失败: {exc}")

    # ---------------- 夜间运行 ----------------
    # ---------------- 事件入口 ----------------

    async def handle_event(self, event: Any) -> None:
        """记录事件；好友申请按预期联系人规则自动同意。"""
        await self.recorder.record_event(event)
        if not self.enabled():
            return
        try:
            raw = event.message_obj.raw_message
            if (
                isinstance(raw, dict)
                and raw.get("post_type") == "request"
                and raw.get("request_type") == "friend"
            ):
                ok = await self.friend_requests.handle_incoming(
                    str(raw.get("user_id", "")),
                    str(raw.get("flag", "")),
                    str(raw.get("comment", "")),
                )
                if ok:
                    self.logger.info("[AstrHe] 已自动同意预期联系人的好友申请")
        except Exception:  # noqa: BLE001
            pass

    # ---------------- 夜间运行 ----------------

    def _internal_tools_provider(self, kind: str, **kwargs: Any) -> list:
        if kind == "reader":
            window = kwargs.get("window") or self._default_window()
            reader = ReaderTools(
                self.storage,
                self.context,
                self.tz,
                lambda: window,
                self.secrecy,
                run_id=kwargs.get("run_id"),
                access=self.access,
            )
            return reader.build()
        if kind == "draft":
            return DraftTools(
                self.storage,
                kwargs["run_id"],
                kwargs["request_review_cb"],
                kwargs["commit_cb"],
                qq_client=self.qq,
                tz_getter=self.tz,
                commit_state=kwargs.get("commit_state"),
                admins=kwargs.get("admins", self.admins()),
                access=self.access,
            ).build()
        return []

    async def run_night(self, mode: str = "auto") -> dict[str, Any]:
        """调度器与指令共用的入口。"""
        if self._external_tasks:
            self.logger.info("[AstrHe] 上次提交的外部动作仍在收尾，跳过本次触发")
            return {"status": "skipped", "reason": "上次提交仍在收尾"}
        pipeline = NightPipeline(
            storage=self.storage,
            context=self.context,
            config=self.config,
            platform_helper=self.platform,
            qq_client=self.qq,
            rate_limiter=self.rate_limiter,
            secrecy_resolver=self.secrecy,
            admins_getter=self.admins,
            tz_getter=self.tz,
            backfiller=self.backfiller,
            friend_request_manager=self.friend_requests,
            internal_tools_provider=self._internal_tools_provider,
            logger=self.logger,
            session_access=self.access,
        )
        pipeline.alarm_tracker = self.alarm_tracker
        pipeline.external_tasks = self._external_tasks
        self._current_pipeline = pipeline
        try:
            return await pipeline.run(mode)
        finally:
            if not self._external_tasks:
                self._current_pipeline = None
            else:

                def clear_finished_pipeline(_):
                    if self._current_pipeline is pipeline and not self._external_tasks:
                        self._current_pipeline = None

                for task in tuple(self._external_tasks):
                    task.add_done_callback(clear_finished_pipeline)

    # ---------------- 指令与状态 ----------------

    async def cmd_run(self, mode: str) -> str:
        """run / dryrun 指令：开后台任务，立刻回复。"""
        if not self.enabled():
            return "AstrHe 还没启用（插件设置里 enabled 是关的）"
        if self._external_tasks:
            return "上一次已提交，外部动作仍在收尾，等它完成后再运行"
        result = await self.scheduler.trigger(mode)
        if result is None:
            return "上一次夜间运行还在进行，等它跑完吧"
        return "已开始" + ("（演练模式，不提交）" if mode == "dry" else "")

    async def cmd_stop(self) -> str:
        if self._external_tasks and not self.scheduler.is_running:
            return "计划已经提交，外部动作正在收尾，停止不会撤销它们"
        return await self.scheduler.stop_running()

    async def cmd_status(self) -> str:
        latest = await self.storage.runs.latest_run()
        lines = []
        if (
            self.scheduler.is_running or self._external_tasks
        ) and self._current_pipeline is not None:
            pipeline = self._current_pipeline
            elapsed = int(time.time() - pipeline.stage_since)
            lines.append(
                f"进行中：当前阶段 {pipeline.stage}，本阶段已用 {elapsed} 秒，"
                f"总计 {int(time.time() - pipeline.started_at)} 秒"
            )
        if latest is None:
            lines.append("还没有运行记录")
        else:
            tz = self.tz()
            started = local_datetime_str(latest["started_at"], tz)
            lines.append(
                f"最近一次：{latest['plan_date']} [{latest['mode']}] {latest['status']}"
                f"（开始于 {started}）"
            )
            if latest["error"]:
                lines.append(f"失败原因：{latest['error']}")
        plan = await self.storage.plans.get_latest_plan()
        if plan:
            lines.append(f"当前生效的规划书：{plan['plan_date']}")
        return "\n".join(lines)

    def status_dict(self) -> dict[str, Any]:
        if (
            self.scheduler.is_running or self._external_tasks
        ) and self._current_pipeline is not None:
            pipeline = self._current_pipeline
            return {
                "running": True,
                "stage": pipeline.stage,
                "elapsed_seconds": int(time.time() - pipeline.started_at),
                "stage_elapsed_seconds": int(time.time() - pipeline.stage_since),
            }
        return {"running": False}
