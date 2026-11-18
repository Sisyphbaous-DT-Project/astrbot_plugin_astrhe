"""仅在新版宿主上接管 AstrHe 的闹钟；所有外部任务仍由宿主派发。"""

from __future__ import annotations

import asyncio
import json
import uuid
import weakref
from contextlib import contextmanager
from datetime import datetime
from importlib.util import find_spec

from .platform import normalize_session_id, parse_umo
from .timeutil import utc_now

DISPATCH_EXTRA_KEY = "astrhe_dispatch"
WAKE_PLACEHOLDER = "（AstrHe 定时任务唤醒，任务卡片仅供本轮使用。）"


@contextmanager
def _latest_config():
    from astrbot.core.config.runtime import current_message_config

    token = current_message_config.set(None)
    try:
        yield
    finally:
        current_message_config.reset(token)


class WakeupCompat:
    def __init__(
        self,
        *,
        context,
        storage,
        access,
        alarm_tracker,
        logger,
        enabled_getter=lambda: True,
    ):
        self.context = context
        self.storage = storage
        self.access = access
        self.alarm_tracker = alarm_tracker
        self.logger = logger
        self.enabled = enabled_getter
        self._closed = False
        self._installed = False
        self._guarded = False
        self._execs: dict[str, dict] = {}
        self._dispatch_lock = asyncio.Lock()
        self._restore_stack = []
        self._sessions = weakref.WeakKeyDictionary()
        self._observer_tasks = set()
        self._watcher_tasks = set()
        self.on_turn_settled = []
        alarm_tracker.dispatch_active = lambda job: any(
            i["job_id"] == job for i in self._execs.values()
        )

    @property
    def installed(self):
        return self._installed

    @staticmethod
    def modern():
        return find_spec("astrbot.core.platform.agent_wakeup_event") is not None

    def _native_runtime(self):
        return getattr(getattr(self.context, "agent", None), "_runtimes", {}).get(
            "astrbot"
        )

    def detect(self):
        if not self.modern():
            return False
        from astrbot.core.platform.agent_wakeup_event import AgentWakeupEvent
        from astrbot.core.pipeline.process_stage.stage import ProcessStage

        runtime = self._native_runtime()
        manager = getattr(self.context, "cron_manager", None)
        return isinstance(getattr(manager, "_running_tasks", None), dict) and all(
            callable(fn)
            for fn in (
                AgentWakeupEvent,
                ProcessStage.process,
                getattr(runtime, "open_session", None),
                getattr(runtime, "active_turn", None),
                getattr(manager, "_run_active_agent_job", None),
                getattr(manager, "delete_job", None),
                getattr(manager, "update_job", None),
                getattr(getattr(manager, "db", None), "get_cron_job", None),
                getattr(getattr(manager, "db", None), "update_cron_job", None),
            )
        )

    def _wrap(self, obj, name, wrapper):
        original_slot = vars(obj).get(name)
        setattr(obj, name, wrapper)
        self._restore_stack.append((obj, name, original_slot, wrapper))

    def install(self):
        if self._installed or not self.detect():
            return self._installed
        guarded = self._guarded
        if guarded:
            self._restore_all()
        self._closed = False
        try:
            self._install_dispatch_wrapper()
            self._install_process_stage_wrapper()
            self._install_runtime_wrapper()
            self._install_reset_wrapper()
        except Exception:
            self._restore_all()
            self._closed = True
            if guarded:
                self.install_guard()
            raise
        self._installed = True
        return True

    async def create_alarm_job(self, **kwargs):
        """只有自家创建入口走这里。旧工具晚于卸载返回，也不能重新写入带目标的任务。"""
        if not self.modern():
            return await self.context.cron_manager.add_active_job(**kwargs)
        async with self._dispatch_lock:
            if self._closed or not self.enabled() or not self._installed:
                raise ValueError(
                    "AstrHe 闹钟已暂停：需要启用插件并使用受支持的原生执行环境"
                )
            self._target_config(kwargs["payload"]["session"])
            return await self.context.cron_manager.add_active_job(**kwargs)

    def install_guard(self):
        """新版宿主缺少原生执行能力时，只暂停自家闹钟，不停用日间工具。"""
        if self._guarded or self._installed:
            return
        manager = self.context.cron_manager
        original = getattr(manager, "_run_active_agent_job", None)
        self._closed = False
        try:
            if original is not None:

                async def reject(job, start_time):
                    if self._guarded and (job.payload or {}).get("astrhe_alarm_id"):
                        raise ValueError("AstrHe 闹钟已暂停：不支持当前执行环境")
                    return await original(job, start_time=start_time)

                self._wrap(manager, "_run_active_agent_job", reject)
            self._install_reset_wrapper()
        except Exception:
            self._restore_all()
            self._closed = True
            raise
        self._guarded = True

    def _install_dispatch_wrapper(self):
        manager = self.context.cron_manager
        original = manager._run_active_agent_job

        async def dispatch(job, start_time):
            if not (job.payload or {}).get("astrhe_alarm_id") or (
                self._closed and not self._installed
            ):
                return await original(job, start_time)
            async with self._dispatch_lock:
                if not self._closed and self.enabled():
                    with _latest_config():
                        await self._dispatch(job, start_time)

        self._wrap(manager, "_run_active_agent_job", dispatch)
        self._install_panel_wrappers(manager)

    async def _marked_job(self, job_id):
        job = await self.context.cron_manager.db.get_cron_job(job_id)
        return job if job and (job.payload or {}).get("astrhe_alarm_id") else None

    def _install_panel_wrappers(self, manager):
        original_update = manager.update_job

        async def update(job_id, **changes):
            if self._closed or not {
                "run_at",
                "run_once",
                "cron_expression",
                "timezone",
            }.intersection(changes):
                return await original_update(job_id, **changes)
            if not await self._marked_job(job_id):
                return await original_update(job_id, **changes)
            # 面板改期保留 job_id。排队中的旧执行不能据同一个编号认领新安排。
            async with self._dispatch_lock:
                before = await self._marked_job(job_id)
                if before is None:
                    return await original_update(job_id, **changes)
                job = await original_update(job_id, **changes)
                if job and self._schedule_key(before) != self._schedule_key(job):
                    run_at = job.payload.get("run_at") if job.run_once else None
                    at = datetime.fromisoformat(run_at).timestamp() if run_at else None
                    expression = job.cron_expression or "" if not job.run_once else ""
                    await self.storage.alarms.move_platform_schedule(
                        job.payload["astrhe_alarm_id"], job_id, at, expression
                    )
                    tasks = self._cancel_preloaded({job_id})
                else:
                    tasks = set()
            await asyncio.gather(*tasks, return_exceptions=True)
            return job

        self._wrap(manager, "update_job", update)

        original_delete = manager.delete_job

        async def delete(job_id):
            if self._closed or asyncio.current_task() in manager._running_tasks.get(
                job_id, ()
            ):
                # 真正到点的一次性任务由自己的宿主调度协程删行；不是用户撤销。
                return await original_delete(job_id)
            job = await self._marked_job(job_id)
            if job is None:
                return await original_delete(job_id)
            async with self._dispatch_lock:
                result = await original_delete(job_id)
                await self.storage.alarms.cancel_platform_schedule(
                    job.payload["astrhe_alarm_id"], job_id
                )
                return result

        self._wrap(manager, "delete_job", delete)

    @staticmethod
    def _schedule_key(job):
        return (
            job.run_once,
            job.payload.get("run_at") if job.run_once else job.cron_expression,
            job.timezone,
        )

    def _install_process_stage_wrapper(self):
        from astrbot.core.pipeline.process_stage.stage import ProcessStage

        original = ProcessStage.process

        def process(stage, event):
            if not self._closed and self.is_our_event(event):
                return stage.agent_sub_stage.process(event)
            return original(stage, event)

        self._wrap(ProcessStage, "process", process)

    def _install_runtime_wrapper(self):
        runtime = self._native_runtime()
        original_active = runtime.active_turn
        original_open = runtime.open_session

        def active(umo):
            turn = original_active(umo)
            if self._installed and turn and self.is_our_event(turn.input.event):
                return None
            return turn

        async def open_session(umo, **kwargs):
            session = await original_open(umo, **kwargs)
            if not self._closed and getattr(session, "is_main", True):
                self._wrap_session_run_turn(session)
            return session

        self._wrap(runtime, "active_turn", active)
        self._wrap(runtime, "open_session", open_session)

    def _wrap_session_run_turn(self, session):
        if session in self._sessions:
            return
        original = session.run_turn

        async def run_turn(input, *, admit=None):
            if self._closed:
                return await original(input, admit=admit)
            event = input.event
            info = self._dispatch_info(event)
            if not self.is_our_event(event):
                turn = await original(input, admit=admit)
                event.set_extra("astrhe_main_turn", weakref.ref(turn))
                if self.on_turn_settled and not self._closed:
                    self._track(self._notify(event, turn), self._watcher_tasks)
                return turn
            turn = await original(input, admit=self._make_admit(info, admit, event))
            event.set_extra("astrhe_main_turn", weakref.ref(turn))
            live = self._execs.get(info["exec_no"])
            if live:
                live["turn"] = turn
            self._track(self._observe_turn(info, turn), self._observer_tasks)
            return turn

        # 只由会话强持其包装；恢复表不延长宿主历史会话的寿命。
        run_turn.previous_slot = vars(session).get("run_turn")
        session.run_turn = run_turn
        self._sessions[session] = weakref.ref(run_turn)

    def _install_reset_wrapper(self):
        mgr = getattr(self.context, "conversation_manager", None)
        original = getattr(mgr, "update_conversation", None)
        if original is None:
            return

        async def update(
            unified_msg_origin, conversation_id=None, history=None, *args, **kwargs
        ):
            if self._closed or history != []:
                return await original(
                    unified_msg_origin, conversation_id, history, *args, **kwargs
                )
            cid = conversation_id or await mgr.get_curr_conversation_id(
                unified_msg_origin
            )
            repo = self.storage.sent_receipts
            async with repo.boundary:
                cutoff = utc_now()
                result = await original(
                    unified_msg_origin, cid, history, *args, **kwargs
                )
                if cid:
                    await repo.discard_before_reset(unified_msg_origin, cid, cutoff)
                return result

        self._wrap(mgr, "update_conversation", update)

    @staticmethod
    def _dispatch_info(event):
        info = event.get_extra(DISPATCH_EXTRA_KEY) if event is not None else None
        return info if isinstance(info, dict) else None

    def is_our_event(self, event):
        info = self._dispatch_info(event)
        live = self._execs.get(info.get("exec_no")) if info else None
        return bool(live and live["event"]() is event)

    def _target_config(self, umo):
        try:
            cfg = self.context.get_config(umo=umo)
            name = cfg.get("agent", {}).get("runtime", "astrbot")
        except Exception as exc:
            raise ValueError("无法确认目标会话的 agent runtime，未派发") from exc
        if name != "astrbot":
            raise ValueError(
                f"目标会话 runtime 为 {name!r}，AstrHe 闹钟只支持原生 astrbot"
            )
        return cfg

    @staticmethod
    def _role(payload, cfg):
        admins = {str(x) for x in cfg.get("admins_id", [])}
        return (
            "admin"
            if payload.get("allow_admin_role") is not False
            and (
                payload.get("origin") == "api"
                or str(payload.get("sender_id", "")) in admins
            )
            else "member"
        )

    async def _allowed(self, alarm, role):
        if self.access is None:
            return True
        if hasattr(self.access, "allows_alarm"):
            return await self.access.allows_alarm(
                alarm["target_umo"],
                day_admin=alarm["origin"] == "day" and role == "admin",
            )
        return await self.access.allows(alarm["target_umo"])

    async def _dispatch(self, job, start_time):
        from astrbot.core.agent.events import AgentSessionRef
        from astrbot.core.cron.manager import PROACTIVE_AGENT_CRON_WOKE_USER_PROMPT
        from astrbot.core.platform.agent_wakeup_event import AgentWakeupEvent
        from astrbot.core.platform.message_session import MessageSession

        payload = dict(job.payload)
        repo = self.storage.alarms
        alarm = await repo.get_by_alarm_id_str(str(payload["astrhe_alarm_id"]))
        if (
            not alarm
            or alarm["cron_job_id"] != job.job_id
            or alarm["status"] in ("cancelled", "missing")
        ):
            return
        umo = str(payload.get("session") or "").strip()
        if umo != alarm["target_umo"]:
            raise ValueError("平台目标与 AstrHe 卡片绑定不同，请在 AstrHe 重新安排")
        info = {
            "alarm_id": alarm["id"],
            "exec_no": uuid.uuid4().hex,
            "job_id": job.job_id,
            "target_umo": umo,
            "run_once": bool(job.run_once),
            "dispatched_at": utc_now(),
            "revision": alarm["schedule_revision"],
        }
        try:
            cfg = self._target_config(umo)
        except ValueError as exc:
            await self._note_skipped(info, str(exc), terminal=job.run_once)
            raise
        role = self._role(payload, cfg)
        if not await self._allowed(alarm, role):
            await self._note_skipped(info, "派发前目标会话被停用或不在名单里")
            return
        session = MessageSession.from_str(umo)
        platform = self.context.get_platform_inst(session.platform_id)
        if platform is None:
            reason = f"未找到目标平台 {session.platform_id}"
            await self._note_skipped(info, reason)
            raise ValueError(reason)
        native = await self._native_runtime().open_session(umo)
        event = AgentWakeupEvent(
            context=self.context,
            session=session,
            platform_meta=platform.meta(),
            prompt=WAKE_PLACEHOLDER,
            source="cron",
            source_id=job.job_id,
            role=role,
            agent_session=AgentSessionRef("astrbot", native.id),
        )
        _, kind, sid = parse_umo(umo)
        if kind == "GroupMessage":
            event.message_obj.group_id = normalize_session_id(kind, sid)
        snapshot = {
            "id": job.job_id,
            "type": job.job_type,
            "run_once": job.run_once,
            "run_started_at": start_time.isoformat(),
            "run_at": payload.get("run_at"),
            "session": umo,
        }
        event.set_extra("cron_job", snapshot.copy())
        event.set_extra(
            "cron_payload",
            {
                k: payload[k]
                for k in ("astrhe_alarm_id", "sender_id", "origin", "allow_admin_role")
                if k in payload
            },
        )
        event.set_extra(DISPATCH_EXTRA_KEY, info)
        snapshot.update(
            name=job.name,
            description=job.description,
            note=payload.get("note") or job.description or job.name,
        )
        card = PROACTIVE_AGENT_CRON_WOKE_USER_PROMPT.format(
            cron_job=json.dumps(snapshot, ensure_ascii=False)
        )
        if not await repo.claim_dispatch(info):
            return
        exec_no = info["exec_no"]
        loop = asyncio.get_running_loop()

        def ended(*_, reason="未进入执行"):
            live = self._execs.get(exec_no)
            if (
                live
                and "turn" not in live
                and not self._closed
                and not loop.is_closed()
            ):
                self._execs.pop(exec_no)
                self._track(self._settle(info, "failed", reason), self._observer_tasks)

        self._execs[exec_no] = {
            **info,
            "card": card,
            "event": weakref.ref(event, ended),
        }
        # 外层阶段可提前停止，甚至在 EventBus 找不到平台时直接丢弃事件。
        clean = weakref.WeakMethod(event.cleanup_temporary_local_files)

        def cleanup():
            try:
                clean()()
            finally:
                ended()

        event.cleanup_temporary_local_files = cleanup
        # 总线撤下平台时不会进入流水线清理，最后一个事件还会一直留在其局部变量里。
        record = weakref.WeakMethod(event.trace.record)

        def record_end(action, **fields):
            try:
                return record()(action, **fields)
            finally:
                if action == "pipeline_end" and fields.get("status") == "cancelled":
                    ended(reason="宿主取消了排队事件")

        event.trace.record = record_end
        try:
            if job.run_once:
                await self.context.cron_manager.update_job(job.job_id, enabled=False)
            await self.context.get_event_queue().put(event)
        except BaseException:
            ended()
            raise

    def _make_admit(self, info, original_admit, event=None):
        async def admit():
            with _latest_config():
                if (
                    self._closed
                    or not self.enabled()
                    or not await self._recheck(info, event)
                ):
                    return False
                if original_admit is not None and not await original_admit():
                    return False
                if event is not None:
                    if self._closed or not self.is_our_event(event):
                        return False
                    event.set_extra("astrhe_card", self._execs[info["exec_no"]]["card"])
                return True

        return admit

    async def _recheck(self, info, event=None):
        alarm = await self.storage.alarms.get_alarm(info["alarm_id"])
        if (
            not alarm
            or alarm["cron_job_id"] != info["job_id"]
            or alarm["target_umo"] != info["target_umo"]
            or alarm["schedule_revision"] != info.get("revision", 0)
        ):
            await self._note_skipped(info, "闹钟记录或绑定已变化", terminal=False)
            return False
        if alarm["status"] in ("cancelled", "missing") or (
            not alarm["cron_expression"]
            and alarm["status"] not in ("scheduled", "fired")
        ):
            await self._note_skipped(info, "安排已结束")
            return False
        with _latest_config():
            try:
                cfg = self._target_config(alarm["target_umo"])
            except ValueError as exc:
                await self._note_skipped(info, str(exc))
                return False
            role = (
                self._role(event.get_extra("cron_payload") or {}, cfg)
                if event
                else "member"
            )
            if event is not None:
                event.role = role
            if not await self._allowed(alarm, role):
                await self._note_skipped(info, "目标会话已停用或不在名单里")
                return False
        return True

    async def _note_skipped(self, info, reason, *, terminal=True):
        await self.storage.alarms.add_event(
            info["alarm_id"],
            "skipped",
            json.dumps({"exec": info["exec_no"], "note": reason}, ensure_ascii=False),
        )
        if terminal:
            await self.storage.db.execute(
                "UPDATE alarms SET status='failed',result_note=?,finished_at=? WHERE id=? AND cron_job_id=? AND schedule_revision=? AND cron_expression='' AND status IN ('scheduled','fired')",
                (
                    reason,
                    utc_now(),
                    info["alarm_id"],
                    info["job_id"],
                    info.get("revision", 0),
                ),
            )

    def _track(self, coro, tasks):
        task = asyncio.create_task(coro)
        tasks.add(task)

        def finished(t):
            tasks.discard(t)
            if not t.cancelled() and (exc := t.exception()):
                self.logger.warning(f"[AstrHe] 执行收尾失败：{exc}")

        task.add_done_callback(finished)

    async def _settle(self, info, status, reason, error=""):
        await self.alarm_tracker.settle_turn(
            info["alarm_id"], info["job_id"], info["exec_no"], status, reason, error
        )

    async def _notify(self, event, turn, result=None):
        if result is None:
            result = await turn.result()
        for callback in self.on_turn_settled:
            await callback(event, result)

    async def _observe_turn(self, info, turn):
        try:
            result = await turn.result()
            status = getattr(result.status, "value", result.status)
            await self._settle(
                info,
                status,
                result.reason,
                getattr(result.error, "message", "") if result.error else "",
            )
            await self._notify(turn.input.event, turn, result)
        finally:
            self._execs.pop(info["exec_no"], None)

    async def uninstall(self):
        self._closed = True
        async with self._dispatch_lock:
            for info in list(self._execs.values()):
                event = info["event"]()
                if event is not None:
                    event.stop_event()
                    event.set_extra("astrhe_card", None)
                turn = info.get("turn")
                if turn is None:
                    self._execs.pop(info["exec_no"], None)
                    await self._settle(info, "cancelled", "插件卸载")
                elif not turn.done:
                    turn.request_stop()
        if self._installed or self._guarded:
            await self.park_jobs()
        for task in list(self._watcher_tasks):
            task.cancel()
        await asyncio.gather(
            *self._watcher_tasks, *self._observer_tasks, return_exceptions=True
        )
        self._restore_all()

    async def park_jobs(self, *, restore=False):
        """卸载后宿主不认识 AstrHe；撤下派发目标，连忽略 enabled 的面板立即执行也不会裸跑。"""
        async with self._dispatch_lock:
            owned = await self._set_parked(restore)
            tasks = set() if restore else self._cancel_preloaded(owned)
        await asyncio.gather(*tasks, return_exceptions=True)

    def _cancel_preloaded(self, owned):
        # 宿主先登记任务、读取旧快照，再进入派发包装。只停放数据库拦不住已读出的目标。
        running = getattr(self.context.cron_manager, "_running_tasks", {})
        current = asyncio.current_task()
        tasks = {
            t
            for job_id in owned
            for t in running.get(job_id, ())
            if t is not current and not t.done()
        }
        for task in tasks:
            task.cancel()
        return tasks

    async def _set_parked(self, restore):
        manager = self.context.cron_manager
        owned = set()
        for job in await manager.list_jobs("active_agent"):
            payload = dict(job.payload or {})
            if not payload.get("astrhe_alarm_id"):
                continue
            owned.add(job.job_id)
            parked = payload.get("astrhe_parked")
            if restore:
                if not parked:
                    continue
                alarm = await self.storage.alarms.get_by_alarm_id_str(
                    str(payload["astrhe_alarm_id"])
                )
                if (
                    not alarm
                    or alarm["cron_job_id"] != job.job_id
                    or alarm["status"] in ("cancelled", "missing")
                ):
                    continue
                payload["session"] = parked["session"]
                del payload["astrhe_parked"]
                # 冷启动时平台尚未就绪；这里只还原持久记录，由宿主 start(ctx) 正常排程。
                update = (
                    manager.db.update_cron_job
                    if getattr(manager, "ctx", None) is None
                    else manager.update_job
                )
                await update(job.job_id, payload=payload, enabled=parked["enabled"])
            elif not parked and payload.get("session"):
                payload["astrhe_parked"] = {
                    "session": payload.pop("session"),
                    "enabled": bool(job.enabled),
                }
                await manager.update_job(job.job_id, payload=payload, enabled=False)
        return owned

    @staticmethod
    def _restore(obj, name, slot, wrapper):
        if getattr(obj, name, None) is wrapper:
            if slot is None:
                delattr(obj, name)
            else:
                setattr(obj, name, slot)

    def _restore_all(self):
        for session, ref in list(self._sessions.items()):
            if wrapper := ref():
                self._restore(session, "run_turn", wrapper.previous_slot, wrapper)
        self._sessions.clear()
        while self._restore_stack:
            self._restore(*self._restore_stack.pop())
        self._installed = False
        self._guarded = False
