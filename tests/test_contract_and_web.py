"""配置契约、面板输入与记录时间。"""

import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import yaml

from astrhe.recorder import Recorder
from astrhe.timeutil import utc_now
from astrhe.webapi import WebApi
from tests.conftest import FakeContext, FakeEvent, FakeLogger, Plain, arun, make_storage


def test_version_and_only_approved_settings():
    root = Path(__file__).resolve().parents[1]
    metadata = yaml.safe_load((root / "metadata.yaml").read_text())
    assert str(metadata["version"]) == "0.4.0" and "astrbot_version" not in metadata
    settings = json.loads((root / "_conf_schema.json").read_text())
    assert set(settings) == {
        "enabled",
        "provider_id",
        "persona_id",
        "session_whitelist",
        "session_blacklist",
        "frontend_landscape",
        "frontend_portrait",
    }
    assert settings["persona_id"]["_special"] == "select_persona"
    assert settings["persona_id"]["default"] == ""
    for key in ("session_whitelist", "session_blacklist"):
        assert settings[key]["type"] == "list"
        assert settings[key]["items"] == {"type": "string"}
        assert settings[key]["default"] == []
    for key, default in (
        ("frontend_landscape", "SOUL OS"),
        ("frontend_portrait", "NORMAL"),
    ):
        assert settings[key]["type"] == "string"
        assert settings[key]["options"] == ["SOUL OS", "NORMAL"]
        assert settings[key]["default"] == default


@pytest.mark.parametrize(
    "getter, expected",
    [
        (None, {"landscape": "SOUL OS", "portrait": "NORMAL"}),
        (lambda: (None, "乱填"), {"landscape": "SOUL OS", "portrait": "NORMAL"}),
        (lambda: ("NORMAL", "SOUL OS"), {"landscape": "NORMAL", "portrait": "SOUL OS"}),
    ],
    ids=["未传读取函数", "非法值回退默认", "正常值"],
)
def test_web_frontend_choice(tmp_path, monkeypatch, getter, expected):
    async def check():
        import astrhe.webapi as module

        monkeypatch.setattr(
            module, "astrbot_web", SimpleNamespace(json_response=lambda value: value)
        )
        s = await make_storage(tmp_path)
        try:
            api = WebApi(
                s,
                FakeContext(),
                lambda: ZoneInfo("Asia/Shanghai"),
                lambda: {},
                frontend_getter=getter,
            )
            assert await api._frontend() == expected
        finally:
            await s.close()

    arun(check())


@pytest.mark.parametrize(
    "value", ["生日 10-11", '"生日 10-11"', None, [{"name": "生日", "date": "14-01"}]]
)
def test_web_person_save_rejects_invalid_anniversary(tmp_path, monkeypatch, value):
    async def check():
        import astrhe.webapi as module

        fake_web = SimpleNamespace(
            error_response=lambda text, status_code=400: {
                "error": text,
                "status": status_code,
            },
            json_response=lambda value: value,
        )
        monkeypatch.setattr(module, "astrbot_web", fake_web)
        s = await make_storage(tmp_path)
        try:
            api = WebApi(
                s, FakeContext(), lambda: ZoneInfo("Asia/Shanghai"), lambda: {}
            )

            async def body():
                return {"qq": "50005", "anniversaries": value}

            api._json_body = body
            result = await api._person_save()
            assert result["status"] == 400
            assert await s.persons.get_person("50005") is None
        finally:
            await s.close()

    arun(check())


def test_record_sent_uses_send_time(tmp_path):
    async def check():
        s = await make_storage(tmp_path)
        try:
            event = FakeEvent()
            event.message_obj.timestamp = 1700000000
            event.set_result(SimpleNamespace(chain=[Plain("回复")]))
            recorder = Recorder(s, FakeLogger(), lambda: True)
            before = utc_now()
            await recorder.record_sent(event)
            messages = await s.ledger.get_messages(
                "default:GroupMessage:20002", 0, utc_now() + 1
            )
            assert len(messages) == 1 and messages[0]["ts"] >= before
        finally:
            await s.close()

    arun(check())
