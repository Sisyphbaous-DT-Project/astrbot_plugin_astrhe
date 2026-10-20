"""夜间流程编排：清理 → 补拉 → 对账 → 汇报 → 主 agent → 审稿 → 提交或失败回退。

自动停止条件：连续三次模型请求失败（NightAgentError）；不设任何时长、步数、数据量上限。
人工停止：stop() / 任务被取消；提交前取消判失败，提交后外部动作照常收尾。
运行重叠：由调度器保证不并发；pipeline 开头再自查一次。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .. import constants as C
from ..backfill import Backfiller
from ..storage.retention import cleanup_before_night
from ..timeutil import local_date_str, utc_now
from .agent_runner import NightAgentError, retry_stats
from .draft import CommitError, DraftCommitter
from .draft_preview import describe_dry_actions
from .personas import resolve_session_personas, selected_persona
from .planner import Planner
from .reporter import Reporter
from .reviewer import Reviewer


class NightPipeline:
    """夜间复盘流程。一个实例对应一次运行。"""

    def __init__(
        self,
        storage: Any,
        context: Any,
        config: Any,
        platform_helper: Any,
        qq_client: Any,
        rate_limiter: Any,
        secrecy_resolver: Any,
        admins_getter,
        tz_getter,
        backfiller: Backfiller | None,
        friend_request_manager: Any,
        internal_tools_provider,
        logger: Any,
        session_access: Any = None,
    ):
        self.storage = storage
        self.context = context
        self.config = config
        self.platform = platform_helper
        self.qq = qq_client
        self.rate_limiter = rate_limiter
        self.secrecy = secrecy_resolver
        self._admins_getter = admins_getter
        self._tz_getter = tz_getter
        self.backfiller = backfiller
        self.friend_requests = friend_request_manager
        self._internal_tools_provider = internal_tools_provider
        self.logger = logger
        self.access = session_access

        self.stage = "init"
        self.stage_since = time.time()
        self._fatal: str = ""
        self._commit_state: dict[str, Any] = {"committed": False}
        self.external_tasks: set[asyncio.Task] = set()
        self._stats = {}
        self._run_task = None
        self._run_id = None
        self._committer = None
        self._external_task = None
        self._stage_started = time.monotonic()
        self.started_at = time.time()

    def _set_stage(self, stage: str) -> None:
        if self._stats:
            stages = self._stats.setdefault("stages", {})
            stages[self.stage] = round(
                stages.get(self.stage, 0) + time.monotonic() - self._stage_started, 3
            )
        self.stage = stage
        self.stage_since = time.time()
        self._stage_started = time.monotonic()

    # ------------------------------------------------------------------

    async def resolve_provider_id(self) -> str:
        """配置的 provider_id；留空用当前默认聊天模型并写日志。"""
        configured = ""
        try:
            configured = str(self.config.get("provider_id", "") or "")
        except Exception:  # noqa: BLE001
            configured = ""
        if configured:
            return configured
        try:
            prov = await self.context.get_using_provider_async()
            pid = str(prov.meta().id) if prov else ""
            if pid:
                self.logger.info(f"[AstrHe] 未配置模型，使用当前默认聊天模型: {pid}")
                return pid
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"[AstrHe] 取默认模型失败: {exc}")
        raise NightAgentError("没有可用的聊天模型：请在插件设置里选择模型")

    def _admins(self) -> list[str]:
        try:
            ids = self._admins_getter() or []
        except Exception:  # noqa: BLE001
            ids = []
        # 过滤掉 "astrbot" 这类非数字占位
        return [str(x) for x in ids if str(x).isdigit()]

    # ------------------------------------------------------------------

    async def run(self, mode: str = "auto") -> dict[str, Any]:
        """跑一次夜间流程。mode: auto / manual / dry。"""
        tz = self._tz_getter()
        now = utc_now()

        # 运行重叠自查（调度器之外的双保险）
        running = await self.storage.runs.running_runs()
        if running:
            self.logger.info("[AstrHe] 已有夜间运行在进行，跳过本次触发")
            return {"status": "skipped", "reason": "已有运行在进行"}

        last_committed = await self.storage.runs.latest_committed_run()
        if last_committed and last_committed["window_end"]:
            window_start = float(last_committed["window_end"])
        else:
            window_start = now - C.COLD_START_SECONDS
        plan_date = local_date_str(now, tz)
        run_id = await self.storage.runs.start_run(
            plan_date=plan_date, mode=mode, window_start=window_start, window_end=now
        )

        stats: dict[str, Any] = {"stages": {}}
        dry = mode == "dry"
        self._stats = stats
        self._run_id = run_id
        self._run_task = asyncio.current_task()
        token = retry_stats.set(stats)
        try:
            result = await self._run_stages(
                run_id, plan_date, window_start, now, tz, dry, stats
            )
            return result
        except NightAgentError as exc:
            if self._commit_state.get("committed"):
                status = "dry_done" if dry else "committed"
                await self.storage.runs.finish_run(
                    run_id, status, error=f"提交后模型失败：{exc}", stats=stats
                )
                return {"status": status, "stats": stats, "error": str(exc)}
            await self.storage.runs.finish_run(
                run_id,
                "failed",
                error=f"模型请求失败（{self.stage}）: {exc}",
                stats=stats,
            )
            return {"status": "failed", "error": str(exc), "stage": self.stage}
        except asyncio.CancelledError:
            # 提交事务返回的临界点若被取消，以持久状态为准补启动收尾。
            stored = await self.storage.runs.get_run(run_id)
            if stored and stored["status"] == "committed":
                self._commit_state["committed"] = True
                if self._committer is not None:
                    self._ensure_external(self._committer)
            # 人工停止 / 插件终止：已提交的不改状态，未提交的判失败
            if self._commit_state.get("committed"):
                # 已提交：正式数据已落库，标记 committed；shield 保护的外部动作
                # 仍在后台收尾
                await self.storage.runs.finish_run(
                    run_id,
                    "dry_done" if dry else "committed",
                    error="提交后被停止，外部动作后台收尾",
                    stats=stats,
                )
            else:
                await self.storage.runs.finish_run(
                    run_id,
                    "failed",
                    error=self._fatal or f"运行被取消（{self.stage}）",
                    stats=stats,
                )
            if self._fatal:
                return {"status": "failed", "error": self._fatal}
            raise
        except Exception as exc:  # noqa: BLE001
            if self._commit_state.get("committed"):
                status = "dry_done" if dry else "committed"
                await self.storage.runs.finish_run(
                    run_id, status, error=str(exc), stats=stats
                )
                return {"status": status, "stats": stats, "error": str(exc)}
            await self.storage.runs.finish_run(
                run_id, "failed", error=f"{self.stage}: {exc}", stats=stats
            )
            self.logger.warning(f"[AstrHe] 夜间运行失败（{self.stage}）: {exc}")
            return {"status": "failed", "error": str(exc), "stage": self.stage}

        finally:
            stages = stats.setdefault("stages", {})
            if self.stage != "finished" and not self.external_tasks:
                stages[self.stage] = round(
                    stages.get(self.stage, 0) + time.monotonic() - self._stage_started,
                    3,
                )
            stats["elapsed_seconds"] = round(time.time() - self.started_at, 3)
            await self.storage.runs.update_run_stats(run_id, stats)
            retry_stats.reset(token)

    async def _run_stages(
        self,
        run_id: int,
        plan_date: str,
        window_start: float,
        window_end: float,
        tz: Any,
        dry: bool,
        stats: dict[str, Any],
    ) -> dict[str, Any]:

        # ⓪ 清理过期记录：不论当晚成败都做，演练不动数据
        if not dry:
            self._set_stage("cleanup")
            await cleanup_before_night(
                self.storage, self.context, run_id, window_start, self.logger
            )

        # ① 补拉
        self._set_stage("backfill")
        self_id = await self.qq.self_id()
        backfill_stats: dict[str, Any] = {}
        if self.backfiller is not None:
            try:
                backfill_stats = await self.backfiller.backfill_all(
                    window_start, window_end, self_id=self_id
                )
            except Exception as exc:  # noqa: BLE001 - 补拉失败不让整晚失败
                backfill_stats = {"failed": [str(exc)]}
        stats["backfill"] = backfill_stats

        # ② 闹钟对账
        self._set_stage("reconcile")
        reconcile_text = ""
        tracker = getattr(self, "alarm_tracker", None)
        if tracker is not None:
            try:
                reconcile_text = await tracker.nightly_reconcile(
                    window_start, window_end
                )
            except Exception as exc:  # noqa: BLE001
                reconcile_text = f"对账失败: {exc}"
            if self.access is not None:
                try:
                    unlisted = await tracker.cancel_unlisted(
                        self.access, admins=self._admins(), dry=dry
                    )
                except Exception as exc:  # noqa: BLE001
                    unlisted = f"检查会话名单外的闹钟失败: {exc}"
                if unlisted:
                    reconcile_text += "\n" + unlisted

        # ③ 汇报子代理
        self._set_stage("report")
        provider_id = await self.resolve_provider_id()
        selected = await selected_persona(
            self.context, str(self.config.get("persona_id", "") or "")
        )
        persona_prompt = selected.prompt
        umos = await self.storage.ledger.list_session_umos(window_start)
        # 会话名单外的旧记录留着但不再复盘
        if self.access is not None:
            umos = await self.access.filter_umos(umos)
        # 白天人格和夜间人格不同的会话：汇报换人格，规划与审稿拿到对照
        personas = await resolve_session_personas(self.context, selected, umos)
        if personas.differing:
            stats["persona_sessions"] = {
                umo: info.label for umo, info in personas.differing.items()
            }
        reader_tools = self._internal_tools_provider(
            "reader", window=(window_start, window_end), run_id=run_id
        )
        from ..timeutil import build_time_info

        time_info = build_time_info(window_start, window_end, plan_date, tz)
        reporter = Reporter(
            self.storage,
            self.context,
            provider_id,
            persona_prompt,
            tz,
            reader_tools,
            time_info=time_info,
            admins=self._admins(),
            self_id=self_id,
            personas=personas,
        )
        reports = await self._run_reporters(
            reporter, run_id, umos, window_start, window_end
        )
        stats["sessions"] = len(umos)
        stats["reports"] = len(reports)
        stats["reports_failed"] = [
            r["umo"] for r in reports if not r.get("parse_ok", True)
        ]

        # ④ 主 agent
        self._set_stage("plan")
        admins = self._admins()
        planner = Planner(
            self.storage,
            self.context,
            provider_id,
            persona_prompt,
            admins,
            tz,
            time_info=time_info,
            window=(window_start, window_end),
            self_id=self_id,
            personas=personas,
        )
        reviewer = Reviewer(
            self.storage,
            self.context,
            provider_id,
            persona_prompt,
            admins,
            reader_tools,
            time_info=time_info,
            self_id=self_id,
            personas=personas,
        )
        committer = DraftCommitter(
            self.storage,
            self.context,
            run_id,
            plan_date,
            tz,
            self.rate_limiter,
            self.qq,
            admins=admins,
            access=self.access,
        )
        last_run = await self.storage.runs.previous_run(run_id)
        last_failed = bool(
            last_run and last_run["id"] != run_id and last_run["status"] == "failed"
        )
        materials = await planner.build_materials(reports, reconcile_text, last_failed)
        draft_tools = self._internal_tools_provider(
            "draft",
            run_id=run_id,
            admins=admins,
            commit_state=self._commit_state,
            request_review_cb=lambda focus: self._on_review(reviewer, run_id, focus),
            commit_cb=lambda summary: self._on_commit(committer, summary, dry),
        )
        planner_tools = reader_tools + draft_tools
        # 外部工具（代理 + 黑名单）
        planner_tools = planner_tools + self._external_proxied_tools(
            {t.name for t in planner_tools}
        )
        final_text = await planner.run_planner(
            materials=materials, tools=planner_tools, commit_state=self._commit_state
        )

        if self._fatal:
            raise NightAgentError(self._fatal)
        if not self._commit_state.get("committed"):
            await self.storage.runs.finish_run(
                run_id,
                "failed",
                error="主 agent 没有调用 astrhe_commit，视为失败",
                stats=stats,
            )
            return {"status": "failed", "error": "未提交"}

        # 提交工具已经执行并返回外部动作结果，此处只收尾运行记录。
        self._set_stage("finished")
        final_status = "dry_done" if dry else "committed"
        await self.storage.runs.finish_run(run_id, final_status, stats=stats)
        self.logger.info(
            f"[AstrHe] 夜间运行完成：{plan_date} dry={dry} 会话 {stats.get('sessions', 0)} 个"
        )
        return {
            "status": final_status,
            "plan_date": plan_date,
            "stats": stats,
            "final_text": final_text,
        }

    async def _run_reporters(
        self,
        reporter: Reporter,
        run_id: int,
        umos: list[str],
        window_start: float,
        window_end: float,
    ) -> list[dict[str, Any]]:
        """并发 3 跑汇报；任何一个模型失败，取消其余并终止整晚。"""
        tasks = [
            asyncio.ensure_future(
                reporter.report_session(run_id, umo, window_start, window_end)
            )
            for umo in umos
        ]
        results: list[dict[str, Any]] = []
        try:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
        except BaseException:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return results

    # ------------------------------------------------------------------

    async def _on_review(self, reviewer: Reviewer, run_id: int, focus: str) -> str:
        """主 agent 的 astrhe_request_review 回调。

        审稿的模型失败按修订要求终止整晚：记录 fatal，给主 agent 返回说明，
        并拒绝后续提交。
        """
        self._set_stage("review")
        try:
            return await reviewer.review(run_id, focus)
        except NightAgentError as exc:
            self._fatal = f"审稿模型请求失败: {exc}"
            if self._run_task is not None:
                self._run_task.cancel()
            return f"审稿失败：{exc}。本次运行终止。"

        finally:
            self._set_stage("plan")

    async def _on_commit(
        self, committer: DraftCommitter, summary: str, dry: bool
    ) -> str:
        """主 agent 的 astrhe_commit 回调。"""
        if self._commit_state.get("committed"):
            return "今晚已经提交，不能重复提交"
        if self._fatal:
            return f"提交被拒绝：{self._fatal}"
        try:
            await committer.validate()
        except CommitError as exc:
            return f"提交被拒绝：{exc}"
        if dry:
            drafts = await self.storage.runs.get_drafts(committer.run_id)
            self._commit_state["committed"] = True
            return describe_dry_actions(drafts)
        self._committer = committer
        self._set_stage("commit")
        try:
            written = await committer.commit_db()
        except Exception as exc:
            self._set_stage("plan")
            return f"提交时数据库出错: {exc}"
        self._commit_state["committed"] = True
        # 创建任务后立即登记，取消主运行也不会丢掉外部收尾。
        task = self._ensure_external(committer)
        results = await asyncio.shield(task)
        return f"已提交：{summary}（写入：{written}）\n" + "\n".join(results)

    def _ensure_external(self, committer):
        if self._external_task is None:
            self._external_task = asyncio.create_task(self._finish_external(committer))
            self.external_tasks.add(self._external_task)
            self._external_task.add_done_callback(self.external_tasks.discard)
        return self._external_task

    async def _finish_external(self, committer) -> list[str]:
        self._set_stage("external")
        results = []
        try:
            results += await committer.run_external_actions()
        except Exception as exc:
            results.append(f"外部动作异常：{exc}")
        if self.friend_requests is not None:
            try:
                results += await self.friend_requests.nightly_followup()
            except Exception as exc:
                results.append(f"好友申请收尾异常：{exc}")
        self._stats["external_results"] = results
        self._set_stage(
            "plan"
            if self._run_task is not None and not self._run_task.done()
            else "finished"
        )
        self._stats["elapsed_seconds"] = round(time.time() - self.started_at, 3)
        await self.storage.runs.update_run_stats(committer.run_id, self._stats)
        return results

    def _external_proxied_tools(self, exclude_names: set[str]) -> list:
        """外部工具代理列表：从全局工具表取，按黑名单过滤。"""
        from .tool_proxy import ProxiedTool, get_external_tool_timeout, is_blacklisted

        timeout = get_external_tool_timeout(self._astrbot_config())
        result = []
        func_list = self.context.get_llm_tool_manager().get_full_tool_set().tools
        for tool in func_list:
            name = getattr(tool, "name", "")
            if (
                not getattr(tool, "active", True)
                or not name
                or name in exclude_names
                or is_blacklisted(name)
            ):
                continue
            result.append(ProxiedTool(inner=tool, timeout=timeout))
        return result

    def _astrbot_config(self) -> Any:
        try:
            return self.context.get_config()
        except Exception:  # noqa: BLE001
            return {}
