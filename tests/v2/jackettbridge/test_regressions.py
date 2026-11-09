"""JackettBridge V2 回归测试：真实导入插件，仅在网络发送边界隔离。

根 conftest 自动隔离配置目录并阻止真实出站；用例仅使用占位凭据和内存响应。
"""
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, urlparse

import pytest
import requests

import jackettbridge
from jackettbridge import JackettBridge


@pytest.fixture
def plugin():
    """使用真实类与方法，绕过无关的宿主链构造，不读取插件配置或启动服务。"""
    instance = object.__new__(JackettBridge)
    instance._host = "https://jackett.invalid"
    instance._api_key = "test-api-key"
    instance._password = "test-password & example"
    instance._selected_indexers = []
    instance._indexer_catalog = []
    instance._indexers = []
    return instance


def _response(content):
    """构造符合 requests 接口的内存响应，不发送真实请求。"""
    response = requests.Response()
    response.status_code = 200
    response._content = content.encode("utf-8")
    response.encoding = "utf-8"
    return response


def test_login_password_is_only_in_post_body(plugin, monkeypatch):
    """密码不得进入最终请求 URL，表单登录和 Cookie 提取仍应正常。"""
    prepared = []
    with requests.Session() as session:
        def send(request, **kwargs):
            prepared.append(request)
            session.cookies.set("session", "test-cookie")
            return _response("登录成功")

        monkeypatch.setattr(session, "send", send)
        monkeypatch.setattr(jackettbridge.requests, "session", lambda: session)
        cookies = plugin._JackettBridge__login_cookies()

    assert cookies == {"session": "test-cookie"}
    assert len(prepared) == 1
    request = prepared[0]
    assert request.method == "POST"
    assert request.url == "https://jackett.invalid/UI/Dashboard"
    assert parse_qs(urlparse(request.url).query) == {}
    assert parse_qs(request.body) == {"password": ["test-password & example"]}


class _FixedLocalDatetime(datetime):
    """将无参数 astimezone 固定到 UTC+8，使测试不依赖宿主或 CI 时区。"""

    def astimezone(self, tz=None):
        return super().astimezone(tz or timezone(timedelta(hours=8)))


@pytest.fixture
def fixed_local_timezone(monkeypatch):
    """RFC822 与 ISO 解析保留真实算法，只固定 datetime 的本地时区转换。"""
    def parse_rfc822(value):
        parsed = parsedate_to_datetime(value)
        return _FixedLocalDatetime(
            parsed.year, parsed.month, parsed.day,
            parsed.hour, parsed.minute, parsed.second,
            parsed.microsecond, tzinfo=parsed.tzinfo,
        )

    monkeypatch.setattr(jackettbridge, "datetime", _FixedLocalDatetime)
    monkeypatch.setattr(jackettbridge, "parsedate_to_datetime", parse_rfc822)


@pytest.mark.parametrize("value, expected", [
    ("Thu, 01 Jan 2026 01:30:00 -0500", "2026-01-01 14:30:00"),
    ("Thu, 01 Jan 2026 01:30:00 +0000", "2026-01-01 09:30:00"),
    ("2026-01-01T01:30:00-05:00", "2026-01-01 14:30:00"),
    ("2026-01-01T01:30:00+03:00", "2026-01-01 06:30:00"),
    ("2026-01-01T01:30:00Z", "2026-01-01 09:30:00"),
])
def test_aware_pubdate_is_converted_to_local_time(value, expected, fixed_local_timezone):
    """带偏移及 Z 的发布时间应先转换到本地时区，避免排序与时间展示错位。"""
    assert JackettBridge._JackettBridge__parse_pubdate(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("Thu, 01 Jan 2026 01:30:00 -0000", "2026-01-01 01:30:00"),
    ("2026-01-01 01:30:00", "2026-01-01 01:30:00"),
    ("unrecognized date", "unrecognized date"),
    ("", None),
    (None, None),
])
def test_naive_or_unrecognized_pubdate_keeps_existing_semantics(value, expected, fixed_local_timezone):
    """未知时区、无时区和无法解析的日期保持原值，空输入保持 None。"""
    assert JackettBridge._JackettBridge__parse_pubdate(value) == expected


@pytest.mark.parametrize("content", ["<indexers/>", "<indexers></indexers>"])
def test_empty_torznab_indexers_is_authoritative(plugin, monkeypatch, content):
    """有效空列表应清空目录并标为权威结果，无需回退管理接口。"""
    response = _response(content)
    monkeypatch.setattr(requests, "request", lambda *args, **kwargs: response)
    plugin._indexer_catalog = [{"title": "旧索引器", "value": "old"}]

    def unexpected_admin_request():
        pytest.fail("有效空列表不应回退管理接口")

    monkeypatch.setattr(plugin, "_JackettBridge__fetch_indexers_admin_api", unexpected_admin_request)
    assert plugin.get_indexers() == []
    assert plugin._indexers_authoritative is True
    assert plugin._indexer_catalog == []


@pytest.mark.parametrize("content", [
    '<error code="100" description="无效 API Key"/>',
    "<rss><channel/></rss>",
    '<response><indexer id="unexpected"><title>伪索引器</title></indexer></response>',
    "<html><body>登录页面</body></html>",
    "<indexers>",
])
def test_invalid_torznab_response_is_not_authoritative(plugin, monkeypatch, content):
    """错误、其他 XML 根节点及损坏 XML 均应返回 None，以免误清已有索引器。"""
    response = _response(content)
    monkeypatch.setattr(requests, "request", lambda *args, **kwargs: response)
    assert plugin._JackettBridge__fetch_indexers_torznab() is None


def test_valid_torznab_indexer_still_parses(plugin, monkeypatch):
    """修复空列表语义后，正常索引器的名称和公开类型仍应保留。"""
    response = _response(
        '<indexers><indexer id="example" configured="true">'
        '<title>示例索引器</title><type>public</type></indexer></indexers>'
    )
    monkeypatch.setattr(requests, "request", lambda *args, **kwargs: response)
    assert plugin._JackettBridge__fetch_indexers_torznab() == [{
        "id": "example", "name": "示例索引器", "type": "public", "configured": True,
    }]
