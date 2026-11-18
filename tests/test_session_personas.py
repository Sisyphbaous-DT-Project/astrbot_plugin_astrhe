"""会话人格：白天人格与夜间人格不同的会话，汇报换人格、规划审稿带对照、白天提醒不注入。"""

from __future__ import annotations

from types import SimpleNamespace
from zoneinfo import ZoneInfo

from astrhe.night.personas import (
    PersonaInfo,
    SessionPersonas,
    resolve_session_personas,
    selected_persona,
    session_persona,
)
from astrhe.night.planner import Planner
from astrhe.night.reporter import Reporter
from astrhe.night.reviewer import Reviewer
from astrhe.prompts.persona import build_persona_material
from astrhe.timeutil import local_date_str, utc_now
from tests.conftest import FakeContext, FakeLogger, arun, make_storage
from tests.test_injector import _injector
from tests.test_night_pipeline import (
    _build_pipeline,
    _make_handler,
    _planner_handler,
    _reporter_handler,
    _seed_message,
)

TZ = ZoneInfo("Asia/Shanghai")
GROUP_UMO = "default:GroupMessage:20002"
OTHER_UMO = "default:GroupMessage:33333"
SELECTED = PersonaInfo(name="清漪", prompt="你是清漪。")
SERVICE = PersonaInfo(name="客服", prompt="你是耐心的客服小助手。")


class PersonaManagerByUmo:
    """按 UMO 返回人格的假 PersonaManager；raise_for 中的会话模拟查询出错。"""

    def __init__(self, mapping: dict, raise_for=()):
        self.mapping = mapping
        self.raise_for = set(raise_for)
        self.calls: list[dict] = []

    def get_persona_v3_by_id(self, persona_id):
        return (
            {"name": "清漪", "prompt": "你是清漪。"} if persona_id == "清漪" else None
        )

    async def get_default_persona_v3(self, umo=None):
        return {"name": "default", "prompt": "默认人格"}

    async def resolve_selected_persona(
        self, *, umo, conversation_persona_id, platform_name, provider_settings=None
    ):
        self.calls.append(
            {"umo": umo, "conv": conversation_persona_id, "platform": platform_name}
        )
        if umo in self.raise_for:
            raise RuntimeError("查询失败")
        value = self.mapping.get(umo, {"name": "清漪", "prompt": "你是清漪。"})
        if value is None:
            return "[%None]", None, None, False
        return value["name"], value, None, False


class FakeConversations:
    def __init__(self, persona_ids: dict):
        self.persona_ids = persona_ids

    async def get_curr_conversation_id(self, umo):
        return f"cid-{umo}" if umo in self.persona_ids else None

    async def get_conversation(self, umo, cid):
        return SimpleNamespace(persona_id=self.persona_ids[umo])


def _context(mapping, raise_for=(), conv_ids=None):
    ctx = FakeContext(api_actions={"get_login_info": {"user_id": 99999}})
    ctx.persona_manager = PersonaManagerByUmo(mapping, raise_for)
    ctx.conversation_manager = FakeConversations(conv_ids or {})
    return ctx


def test_session_persona_follows_astrbot_resolution():
    async def check():
        ctx = _context(
            {
                GROUP_UMO: {"name": "客服", "prompt": "你是耐心的客服小助手。"},
                OTHER_UMO: None,
            },
            conv_ids={GROUP_UMO: "客服"},
        )
        info = await session_persona(ctx, GROUP_UMO)
        assert info == SERVICE
        # 夜间自己读当前对话，把对话自带的人格交给 AstrBot 解析
        assert ctx.persona_manager.calls[-1] == {
            "umo": GROUP_UMO,
            "conv": "客服",
            "platform": "aiocqhttp",
        }
        none = await session_persona(ctx, OTHER_UMO)
        assert none.none and none.label == "没有设置人格"
        assert ctx.persona_manager.calls[-1]["conv"] is None  # 没有对话
        # 查询出错返回 None（调用方按相同处理）
        ctx.persona_manager.raise_for.add(GROUP_UMO)
        assert await session_persona(ctx, GROUP_UMO) is None
        # 旧版 AstrBot 没有 conversation_manager / resolve_selected_persona：同样返回 None
        assert await session_persona(FakeContext(), GROUP_UMO) is None

    arun(check())


def test_resolve_keeps_only_differing_sessions():
    async def check():
        third = "default:FriendMessage:10002"
        ctx = _context(
            {
                GROUP_UMO: {"name": "客服", "prompt": "你是耐心的客服小助手。"},
                # 名字不同但提示词相同，算同一个人格
                OTHER_UMO: {"name": "清漪副本", "prompt": "你是清漪。\n"},
            },
            raise_for={third},
        )
        selected = await selected_persona(ctx, "清漪")
        assert selected == SELECTED
        personas = await resolve_session_personas(
            ctx, selected, [GROUP_UMO, OTHER_UMO, third]
        )
        assert list(personas.differing) == [GROUP_UMO]
        assert personas.for_umo(OTHER_UMO) == SELECTED
        assert personas.for_umo(third) == SELECTED  # 查不到按相同处理

    arun(check())


def test_real_astrbot_persona_resolution(monkeypatch):
    """用 AstrBot 真实的 resolve_selected_persona 核对调用方式与返回结构。"""
    import astrbot.core.persona_mgr as pm

    if hasattr(pm, "sp"):
        # AstrBot 4.29.x：会话规则与对话自带人格
        class FakeSp:
            async def get_async(self, scope, scope_id, key, default=None):
                if scope_id == GROUP_UMO and key == "session_service_config":
                    return {"persona_id": "客服"}
                return default

        monkeypatch.setattr(pm, "sp", FakeSp())
        cfg = {
            "agent_runner": {
                "runner_type": "local",
                "config": {"persona": {"persona_id": "清漪"}},
            }
        }
        acm = SimpleNamespace(default_conf=cfg, get_conf=lambda umo: cfg)
        manager = pm.PersonaManager(None, acm)
        manager.personas_v3 = [
            {"name": "清漪", "prompt": "你是清漪。"},
            {"name": "客服", "prompt": "你是耐心的客服小助手。"},
        ]
        ctx = SimpleNamespace(persona_manager=manager)

        async def check():
            # 会话管理规则指定的人格
            assert await session_persona(ctx, GROUP_UMO, conversation=None) == SERVICE
            # 对话自带人格
            conv = SimpleNamespace(persona_id="客服")
            assert await session_persona(ctx, OTHER_UMO, conversation=conv) == SERVICE
            # 配置文件默认人格
            assert await session_persona(ctx, OTHER_UMO, conversation=None) == SELECTED
            # 明确不用人格
            none = await session_persona(
                ctx, OTHER_UMO, conversation=SimpleNamespace(persona_id="[%None]")
            )
            assert none.none and none.prompt == ""

        arun(check())
    else:
        # AstrBot 4.30：会话人格在有效配置覆盖里（acm.get_conf 应用覆盖），conversation 人格不再生效
        THIRD_UMO = "default:FriendMessage:10003"

        def _overrides(umo):
            if umo == GROUP_UMO:
                return {"agent.config.persona.persona_id": "客服"}
            if umo == THIRD_UMO:
                return {"agent.config.persona.persona_id": "[%None]"}
            return {}

        async def fake_get_overrides(umo):
            return _overrides(umo)

        monkeypatch.setattr(pm, "get_overrides", fake_get_overrides)
        from astrbot.core.session_overrides import apply_overrides

        cfg = {
            "agent": {
                "runtime": "astrbot",
                "config": {"persona": {"persona_id": "清漪"}},
            }
        }
        acm = SimpleNamespace(
            default_conf=cfg,
            get_conf=lambda umo: apply_overrides(cfg, _overrides(umo)),
        )
        manager = pm.PersonaManager(None, acm)
        manager.personas_v3 = [
            {"name": "清漪", "prompt": "你是清漪。"},
            {"name": "客服", "prompt": "你是耐心的客服小助手。"},
        ]
        ctx = SimpleNamespace(persona_manager=manager)

        async def check():
            # 会话覆盖指定的人格
            assert await session_persona(ctx, GROUP_UMO, conversation=None) == SERVICE
            # 旧对话人格参数被忽略，按配置默认解析
            conv = SimpleNamespace(persona_id="客服")
            assert await session_persona(ctx, OTHER_UMO, conversation=conv) == SELECTED
            # 配置文件默认人格
            assert await session_persona(ctx, OTHER_UMO, conversation=None) == SELECTED
            # 明确不用人格
            none = await session_persona(ctx, THIRD_UMO, conversation=None)
            assert none.none and none.prompt == ""

        arun(check())


def test_persona_material_only_when_differing():
    personas = SessionPersonas(SELECTED)
    assert build_persona_material(personas, "planner") == ""
    assert build_persona_material(None, "reviewer") == ""
    personas.differing = {
        GROUP_UMO: SERVICE,
        OTHER_UMO: SERVICE,
        "default:FriendMessage:10002": PersonaInfo(
            name="[%None]", prompt="", none=True
        ),
    }
    planner_text = build_persona_material(personas, "planner")
    assert "人格「清漪」" in planner_text
    assert f"- {GROUP_UMO}：人格「客服」" in planner_text
    assert planner_text.count("你是耐心的客服小助手。") == 1  # 同一人格只列一次
    assert "default:FriendMessage:10002：没有设置人格" in planner_text
    assert "day_reminders 只会出现在和你同一个人格的会话里" in planner_text
    reviewer_text = build_persona_material(personas, "reviewer")
    assert "审查时额外检查" in reviewer_text and "day_reminders" in reviewer_text


def test_reporter_uses_session_persona(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            now = utc_now()
            rid = await s.runs.start_run(plan_date="2026-10-12")
            for umo, sid in ((GROUP_UMO, "20002"), (OTHER_UMO, "33333")):
                await s.ledger.add_message(
                    umo=umo,
                    platform_id="default",
                    message_type="GroupMessage",
                    session_id=sid,
                    sender_id="10001",
                    text="你好",
                    ts=now - 1,
                    platform_message_id=f"m{sid}",
                )
            ctx = FakeContext()
            ctx.tool_loop_agent.handlers = [
                _make_handler(_reporter_handler),
                _make_handler(_reporter_handler),
            ]
            personas = SessionPersonas(SELECTED, {GROUP_UMO: SERVICE})
            reporter = Reporter(
                s, ctx, "fake", SELECTED.prompt, TZ, [], personas=personas
            )
            await reporter.report_session(rid, GROUP_UMO, now - 10, now + 1)
            await reporter.report_session(rid, OTHER_UMO, now - 10, now + 1)
            group_sys, other_sys = (
                c["system_prompt"] for c in ctx.tool_loop_agent.calls
            )
            assert (
                "你是耐心的客服小助手。" in group_sys and "你是清漪。" not in group_sys
            )
            assert "【本会话的人格】" in group_sys and "人格「客服」" in group_sys
            assert "你是清漪。" in other_sys and "【本会话的人格】" not in other_sys
            # 没有设置人格的会话换成说明，不写成「采用 AstrBot 默认人格」
            none = PersonaInfo(name="[%None]", prompt="", none=True)
            reporter.personas = SessionPersonas(SELECTED, {GROUP_UMO: none})
            prompt, note = reporter.persona_for(GROUP_UMO)
            assert "没有设置人格" in prompt and "没有设置人格" in note
        finally:
            await s.close()

    arun(check())


def test_planner_and_reviewer_get_persona_table(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            ctx = FakeContext()
            reports = [
                {"umo": GROUP_UMO, "report": {"summary": "客服群"}, "parse_ok": True},
                {"umo": OTHER_UMO, "report": {"summary": "清漪群"}, "parse_ok": True},
            ]
            plain = await Planner(s, ctx, "fake", "p", ["10001"], TZ).build_materials(
                reports, "", False
            )
            personas = SessionPersonas(SELECTED, {GROUP_UMO: SERVICE})
            materials = await Planner(
                s, ctx, "fake", "p", ["10001"], TZ, personas=personas
            ).build_materials(reports, "", False)
            assert "【会话人格】" not in plain
            assert materials.index("【会话人格】") < materials.index("【各会话汇报】")
            assert f"--- {GROUP_UMO}（白天用的是人格「客服」） ---" in materials
            assert f"--- {OTHER_UMO} ---" in materials
            # 没有不同人格时材料与不传对照完全一样
            same = await Planner(
                s, ctx, "fake", "p", ["10001"], TZ, personas=SessionPersonas(SELECTED)
            ).build_materials(reports, "", False)
            assert same == plain

            rid = await s.runs.start_run(plan_date="2026-10-12")
            ctx.tool_loop_agent.handlers = [_make_handler(lambda kw: "结论：通过")]
            reviewer = Reviewer(s, ctx, "fake", "p", ["10001"], [], personas=personas)
            await reviewer.review(rid, "全面")
            prompt = ctx.tool_loop_agent.calls[-1]["prompt"]
            assert prompt.startswith("【会话人格】") and "审查时额外检查" in prompt
        finally:
            await s.close()

    arun(check())


def test_night_run_switches_persona_end_to_end(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            ctx = _context(
                {GROUP_UMO: {"name": "客服", "prompt": "你是耐心的客服小助手。"}}
            )
            await _seed_message(s)
            ctx.tool_loop_agent.handlers = [
                _make_handler(_reporter_handler),
                _make_handler(_planner_handler),
                _make_handler(lambda kw: "结论：通过"),
            ]
            pipeline = _build_pipeline(s, ctx, FakeLogger())
            pipeline.config["persona_id"] = "清漪"
            result = await pipeline.run("manual")
            assert result["status"] == "committed", result
            reporter_call, planner_call, reviewer_call = ctx.tool_loop_agent.calls[:3]
            assert "你是耐心的客服小助手。" in reporter_call["system_prompt"]
            assert "你是清漪。" in planner_call["system_prompt"]
            assert "【会话人格】" in planner_call["prompt"]
            assert "审查时额外检查" in reviewer_call["prompt"]
            assert result["stats"]["persona_sessions"] == {GROUP_UMO: "人格「客服」"}
        finally:
            await s.close()

    arun(check())


def test_day_reminders_skip_sessions_with_other_persona(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            today = local_date_str(utc_now(), TZ)
            rid = await s.runs.start_run(plan_date=today, mode="auto")
            plan_id = await s.plans.save_plan(
                run_id=rid,
                plan_date=today,
                full_text="全文",
                reflection="",
                day_reminders="别太复读",
            )
            await s.plans.save_sections(plan_id, {GROUP_UMO: "这个群的段落"})
            inj = _injector(s)
            calls = []

            async def differs():
                calls.append(1)
                return False

            async def same():
                return True

            text = await inj.build_injection(GROUP_UMO, "10001", same_persona=differs)
            assert "这个群的段落" in text and "别太复读" not in text
            assert "别太复读" in await inj.build_injection(
                GROUP_UMO, "10001", same_persona=same
            )
            assert "别太复读" in await inj.build_injection(GROUP_UMO, "10001")
            # 没有白天提醒时不去查人格
            await s.db.execute("UPDATE plans SET day_reminders=''")
            calls.clear()
            await inj.build_injection(GROUP_UMO, "10001", same_persona=differs)
            assert calls == []
        finally:
            await s.close()

    arun(check())


def test_injector_same_persona_check(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            ctx = _context(
                {GROUP_UMO: {"name": "客服", "prompt": "你是耐心的客服小助手。"}}
            )
            inj = _injector(s)
            inj.context = ctx
            inj._persona_id_getter = lambda: "清漪"
            req = SimpleNamespace(conversation=SimpleNamespace(persona_id="客服"))

            def event(umo):
                return SimpleNamespace(
                    unified_msg_origin=umo, get_platform_name=lambda: "aiocqhttp"
                )

            assert await inj._same_persona(event(GROUP_UMO), req) is False
            # 白天直接用请求里的对话，不再自己读
            assert ctx.persona_manager.calls[-1]["conv"] == "客服"
            assert await inj._same_persona(event(OTHER_UMO), req) is True
            ctx.persona_manager.raise_for.add(GROUP_UMO)
            assert await inj._same_persona(event(GROUP_UMO), req) is True
            # 没接上 context（旧调用方式）照旧注入
            assert await _injector(s)._same_persona(event(GROUP_UMO), req) is True
        finally:
            await s.close()

    arun(check())


def test_empty_persona_setting_compares_with_daytime_default():
    """设置人格留空：比较时以 AstrBot 白天给「走默认」的会话解析出的人格为准。"""

    class MismatchManager(PersonaManagerByUmo):
        # 模拟 beta.1：读默认人格得到内置提示词，但白天解析找不到名为 default 的人格
        async def get_default_persona_v3(self, umo=None):
            return {
                "name": "default",
                "prompt": "You are a helpful and friendly assistant.",
            }

        async def resolve_selected_persona(
            self, *, umo, conversation_persona_id, platform_name, provider_settings=None
        ):
            self.calls.append(
                {"umo": umo, "conv": conversation_persona_id, "platform": platform_name}
            )
            if umo == GROUP_UMO:
                return (
                    "客服",
                    {"name": "客服", "prompt": "你是耐心的客服小助手。"},
                    None,
                    False,
                )
            return "default", None, None, False

    async def check():
        ctx = _context({})
        ctx.persona_manager = MismatchManager({})
        selected = await selected_persona(ctx, "")
        # 夜间分身拿到的提示词不变
        assert selected.prompt == "You are a helpful and friendly assistant."
        personas = await resolve_session_personas(ctx, selected, [GROUP_UMO, OTHER_UMO])
        assert list(personas.differing) == [GROUP_UMO]  # 走默认的会话不再被误判
        # 选定的人格存在时不做这层换算
        ctx.persona_manager.calls.clear()
        assert (await selected_persona(ctx, "清漪")).match is None
        assert ctx.persona_manager.calls == []

    arun(check())


def test_empty_persona_setting_with_real_manager(monkeypatch):
    """真实 PersonaManager：设置人格留空时不把走默认的会话误判成不同人格。"""
    import astrbot.core.persona_mgr as pm

    if hasattr(pm, "sp"):
        # AstrBot 4.29.x：库里有别的人格、没有名为 default 的人格
        class FakeSp:
            async def get_async(self, scope, scope_id, key, default=None):
                return default

        monkeypatch.setattr(pm, "sp", FakeSp())
        cfg = {
            "agent_runner": {
                "runner_type": "local",
                "config": {"persona": {"persona_id": "default"}},
            }
        }
        acm = SimpleNamespace(default_conf=cfg, get_conf=lambda umo: cfg)
        manager = pm.PersonaManager(None, acm)
        manager.personas_v3 = [{"name": "客服", "prompt": "你是耐心的客服小助手。"}]
        conversations = FakeConversations({GROUP_UMO: "客服"})
        ctx = SimpleNamespace(
            persona_manager=manager, conversation_manager=conversations
        )
    else:
        # AstrBot 4.30：默认人格可在库里编辑，走默认与会话覆盖仍能区分
        def _overrides(umo):
            if umo == GROUP_UMO:
                return {"agent.config.persona.persona_id": "客服"}
            return {}

        async def fake_get_overrides(umo):
            return _overrides(umo)

        monkeypatch.setattr(pm, "get_overrides", fake_get_overrides)
        from astrbot.core.session_overrides import apply_overrides

        cfg = {
            "agent": {
                "runtime": "astrbot",
                "config": {"persona": {"persona_id": "default"}},
            }
        }
        acm = SimpleNamespace(
            default_conf=cfg,
            get_conf=lambda umo: apply_overrides(cfg, _overrides(umo)),
        )
        manager = pm.PersonaManager(None, acm)
        manager.personas_v3 = [
            {"name": "default", "prompt": "You are a helpful and friendly assistant."},
            {"name": "客服", "prompt": "你是耐心的客服小助手。"},
        ]
        conversations = FakeConversations({})
        ctx = SimpleNamespace(
            persona_manager=manager, conversation_manager=conversations
        )

    async def check():
        selected = await selected_persona(ctx, "")
        personas = await resolve_session_personas(ctx, selected, [GROUP_UMO, OTHER_UMO])
        assert list(personas.differing) == [GROUP_UMO]
        assert personas.for_umo(GROUP_UMO) == SERVICE

    arun(check())
