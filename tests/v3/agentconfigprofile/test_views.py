"""
检查 MP Switch 数据详情页符合宿主 PageRender 的组件合同。

测试只遍历 component、props、text、content、html 和 events，不依赖具名槽、
表单双向模型或动态前端执行；页面 API 请求始终使用模拟令牌。
"""

from __future__ import annotations

import json
from copy import deepcopy

import pytest

from . import test_plugin as regression
from .test_plugin import PLUGIN_ID, profile

SOURCE_URL = "https://github.com/farion1231/cc-switch?tab=readme-ov-file"


@pytest.fixture(name="plugin")
def view_plugin_fixture(monkeypatch):
    """
    复用内存设置和存储，避免页面检查触及真实账号。

    :param monkeypatch: 自动撤销宿主设置替换的 pytest 夹具
    :return: 插件实例、内存存储和模拟设置
    """
    return regression.plugin.__wrapped__(monkeypatch)


def nodes(value):
    """
    按 PageRender 的 content 结构递归遍历所有组件节点。

    :param value: 页面根列表或单个节点
    :yields: 页面中的组件字典
    """
    if isinstance(value, list):
        for child in value:
            yield from nodes(child)
    elif isinstance(value, dict):
        yield value
        yield from nodes(value.get("content", []))


def actions(page, endpoint=None):
    """
    读取 PageRender 支持的事件 API 调用。

    :param page: 页面组件树
    :param endpoint: 可选接口末段名；不填时返回全部动作
    :return: 组件节点和动作配置组成的列表
    """
    result = []
    for node in nodes(page):
        for action in node.get("events", {}).values():
            if endpoint is None or action.get("api", "").endswith("/" + endpoint):
                result.append((node, action))
    return result


def visible_text(page):
    """
    收集用户可读文字，包括组件默认插槽和提示属性。

    :param page: 页面组件树
    :return: 文字、标题与辅助提示组成的文本
    """
    parts = []
    for node in nodes(page):
        parts.append(str(node.get("text", "")))
        props = node.get("props", {})
        parts.extend(str(props.get(key, "")) for key in ("text", "title", "aria-label", "hint"))
    return "\n".join(parts)


def test_page_uses_supported_renderer_keys_and_one_primary_save(plugin):
    """
    页面只使用宿主支持的节点字段，并仅显示一个保存当前配置的主操作。
    """
    p, _store, _settings = plugin
    page = p.get_page()

    allowed = {"component", "props", "text", "content", "html", "events"}
    assert all(set(node) <= allowed for node in nodes(page))
    assert all("component" in node for node in nodes(page))
    assert all("model" not in node.get("props", {}) for node in nodes(page))
    assert "MP Switch" in visible_text(page)
    saves = [item for item in actions(page, "save_current") if not item[1]["params"].get("name")]
    assert len(saves) == 1
    assert "保存" in saves[0][0]["text"]


def test_source_link_is_accessible_and_safe(plugin):
    """
    页面保留可点击的上游源码链接，并防止新窗口取得页面上下文。
    """
    p, _store, _settings = plugin
    links = [node for node in nodes(p.get_page()) if node.get("props", {}).get("href") == SOURCE_URL]

    assert links
    for link in links:
        assert link["component"] in {"a", "VBtn"}
        if link["props"].get("target") == "_blank":
            assert "noopener" in link["props"].get("rel", "").split()


def test_provider_cards_show_current_primary_queue_and_health(plugin):
    """
    供应商列表显示当前生效、首选和队列状态，并区分已失败与未探活。
    """
    p, store, settings = plugin
    excluded = profile("excluded", name="不参加队列", model="excluded")
    excluded["in_failover_queue"] = False
    active = profile("active", name="当前供应商", model=settings.LLM_MODEL,
                     key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL)
    active["health"] = {"status": "ok", "checked_at": p._now(), "duration_ms": 17}
    failed = profile("failed", name="故障供应商", model="failed")
    failed["health"] = {"status": "fail", "checked_at": p._now(), "duration_ms": 2}
    store.data[(PLUGIN_ID, "profiles")] = [excluded, active, failed]

    page = p.get_page()
    text = visible_text(page)

    assert "供应商配置（3）" in text
    assert "首选" in text
    assert "生效" in text
    assert "队列" in text
    assert "失败" in text or "异常" in text
    assert "未探活" in text or "未检查" in text
    toggles = actions(page, "toggle_queue")
    assert {action["params"]["pid"] for _node, action in toggles} == {"excluded", "active", "failed"}
    active_apply = next(node for node, action in actions(page, "apply") if action["params"]["pid"] == "active")
    assert active_apply["props"].get("disabled") is True


@pytest.mark.parametrize("stored_page,expected_page", [(-2, 1), (1, 1), (2, 2), (999, 2)])
def test_page_pagination_clamps_boundaries_and_disables_unavailable_actions(plugin, stored_page, expected_page):
    """
    页码越界时收敛到有效页，边界翻页按钮禁用且参数不越界。
    """
    p, store, _settings = plugin
    total = p._PAGE_SIZE + 1
    store.data[(PLUGIN_ID, "profiles")] = [profile(str(index), name=f"供应商 {index}") for index in range(total)]
    p._runtime["page"] = stored_page

    page = p.get_page()
    calls = actions(page, "set_page")
    assert calls
    assert all(1 <= int(action["params"]["p"]) <= 2 for _node, action in calls)
    assert f"{expected_page}/2" in visible_text(page) or f"{expected_page} / 2" in visible_text(page)
    if expected_page == 1:
        assert any(node["props"].get("disabled") and int(action["params"]["p"]) == 1
                   for node, action in calls)
    else:
        assert any(node["props"].get("disabled") and int(action["params"]["p"]) == 2
                   for node, action in calls)
    applies = actions(page, "apply")
    assert len(applies) == (p._PAGE_SIZE if expected_page == 1 else 1)
    expected_ids = {str(index) for index in range(p._PAGE_SIZE)} if expected_page == 1 else {str(total - 1)}
    assert {action["params"]["pid"] for _node, action in applies} == expected_ids


def test_single_page_does_not_show_pagination_controls(plugin):
    """
    单页供应商列表不显示无用翻页操作。
    """
    p, store, _settings = plugin
    store.data[(PLUGIN_ID, "profiles")] = [profile("one")]

    assert not actions(p.get_page(), "set_page")
    assert "供应商配置（1）" in visible_text(p.get_page())


def test_secondary_actions_are_inside_native_disclosures(plugin):
    """
    次要操作使用 details 与 summary 保持可展开访问，不依赖具名插槽。
    """
    p, store, _settings = plugin
    store.data[(PLUGIN_ID, "profiles")] = [profile("one")]
    page = p.get_page()
    disclosures = [node for node in nodes(page) if node["component"] == "details"]

    assert disclosures
    assert all(any(child["component"] == "summary" for child in node.get("content", []))
               for node in disclosures)
    assert all(not node.get("props", {}).get("open") for node in disclosures)
    enclosed = {action["api"].rsplit("/", 1)[-1] for disclosure in disclosures
                for _node, action in actions(disclosure)}
    assert {"delete", "move", "normalize_names", "prune_offlist", "apply_global"} <= enclosed
    assert {"discover", "probe_all", "clear_log"} <= {
        action["api"].rsplit("/", 1)[-1] for _node, action in actions(page)
    }


def test_model_discovery_and_logs_remain_accessible(plugin):
    """
    重排界面保留模型发现结果、单模型导入和运行日志入口。
    """
    p, store, _settings = plugin
    p._runtime["log"] = [{"time": p._now(), "text": "模拟切换完成", "level": "info"}]
    store.data[(PLUGIN_ID, "discovery")] = {
        "status": "done", "base_url": "https://a.example/v1", "finished_at": p._now(),
        "providers": {"openai": {"ok": True, "models": [{"id": "test-model"}], "message": "OK"}},
    }

    page = p.get_page()
    text = visible_text(page)

    assert "模拟切换完成" in text
    assert "test-model" in text
    add = actions(page, "add_model")
    assert any(action["params"].get("model") == "test-model" for _node, action in add)
    assert actions(page, "clear_log")
    assert actions(page, "discover")


def test_long_untrusted_text_stays_out_of_html_and_masks_credentials(plugin):
    """
    长配置名和错误内容通过文字插槽显示，凭据不会进入标题、日志或原始 HTML。
    """
    p, store, settings = plugin
    secret = "sk-template-private-secret"
    hostile = '<img src=x onerror="alert(1)">'
    candidate = profile("one", name="长名称" * 100 + hostile, key=secret, model="long-model" * 30)
    candidate["health"] = {"status": "fail", "message": f"错误 {secret} {hostile}", "checked_at": p._now()}
    store.data[(PLUGIN_ID, "profiles")] = [candidate]
    p._runtime["log"] = [{"time": p._now(), "text": f"日志 {secret} {settings.LLM_API_KEY} {hostile}"}]
    store.configs[PLUGIN_ID] = {"last_action_message": f"操作 {secret} {hostile}"}
    original = deepcopy(store.data[(PLUGIN_ID, "profiles")])

    page = p.get_page()
    rendered = json.dumps(page, ensure_ascii=False)
    text = visible_text(page)

    assert secret not in rendered
    assert settings.LLM_API_KEY not in rendered
    assert settings.API_TOKEN not in text
    assert all(hostile not in str(node.get("html", "")) for node in nodes(page))
    assert store.data[(PLUGIN_ID, "profiles")] == original
    assert any("overflow-wrap" in node.get("props", {}).get("style", "") for node in nodes(page))


def test_page_api_actions_keep_method_token_namespace_and_profile_ids(plugin):
    """
    所有按钮保留兼容的请求方式、鉴权令牌及原始配置 ID。
    """
    p, store, settings = plugin
    candidate = profile("特殊-pid", name="供应商")
    store.data[(PLUGIN_ID, "profiles")] = [candidate]
    page = p.get_page()

    for _node, action in actions(page):
        assert action["api"].startswith("plugin/agentconfigprofile/")
        assert action["method"].lower() == "get"
        assert action["params"]["apikey"] == settings.API_TOKEN
        if "pid" in action["params"]:
            assert action["params"]["pid"] == "特殊-pid"


def test_api_button_retains_legacy_call_and_accepts_accessibility_attributes(plugin):
    """
    按钮辅助属性扩展不破坏旧调用，也不修改调用方传入参数。
    """
    p, _store, settings = plugin
    params = {"pid": "one"}
    original = deepcopy(params)
    basic = p._api_btn("应用", "apply", params)
    extended = p._api_btn("应用", "apply", params, icon="mdi-check", disabled=True,
                          attrs={"aria-label": "应用供应商配置"})

    assert params == original
    assert basic["events"]["click"]["params"] == {"pid": "one", "apikey": settings.API_TOKEN}
    assert extended["props"]["disabled"] is True
    assert extended["props"]["aria-label"] == "应用供应商配置"
    assert "mdi-check" in json.dumps(extended)


def test_dashboard_masks_historical_error_credentials(plugin):
    """
    自动刷新的仪表盘与详情页同样遮盖历史探活错误里的密钥。
    """
    p, store, settings = plugin
    secret = "sk-backup-private-secret"
    store.data[(PLUGIN_ID, "profiles")] = [profile("backup", key=secret)]
    p._runtime["last_check_ok"] = False
    p._runtime["last_check_message"] = f"错误 {settings.LLM_API_KEY} {secret}"

    _columns, _options, content = p.get_dashboard()
    rendered = json.dumps(content, ensure_ascii=False)

    assert settings.LLM_API_KEY not in rendered
    assert secret not in rendered
    assert "***" in visible_text(content)
