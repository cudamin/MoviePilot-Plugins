"""MP Switch 三模式页面的状态、动作和安全保存合同，全部使用内存夹具。"""

from __future__ import annotations

import json
from copy import deepcopy

import pytest
from agentconfigprofile.forms import build_form

from . import test_plugin as regression
from .test_plugin import PLUGIN_ID, profile
from .test_views import actions, nodes, visible_text


@pytest.fixture(name="plugin")
def mode_plugin_fixture(monkeypatch):
    """使用假配置、假令牌和假存储，不读取真实用户数据。"""
    return regression.plugin.__wrapped__(monkeypatch)


def set_modes(plugin, monkeypatch, *, active="direct", view="direct", members=None,
              route_id="route", selected_model="default", catalog=None):
    """注入模式服务只读状态，观察页面是否误写配置。"""
    p, store, settings = plugin
    current = profile("direct", name="直连供应商", model=settings.LLM_MODEL,
                      key=settings.LLM_API_KEY, base_url=settings.LLM_BASE_URL)
    store.data[(PLUGIN_ID, "profiles")] = [
        current, profile("route", name="路由供应商", model="route-model"),
        profile("other", name="其他供应商", model="other-model"),
    ]
    status = {"active": active, "view": view, "direct_id": "direct", "route_id": route_id,
              "stack_default_id": route_id, "stack_members": members or [route_id],
              "stack_keys": {"stable-route": "route", "stable-other": "other"},
              "selected_model": selected_model, "gateway": {"base_url": "http://127.0.0.1:19999/v1"},
              "revision": 3}
    store.data[(PLUGIN_ID, "switch_mode")] = deepcopy(status)
    monkeypatch.setattr(p, "_mode_ready", lambda: True, raising=False)
    monkeypatch.setattr(p, "_mode_status", lambda: deepcopy(status), raising=False)
    monkeypatch.setattr(p, "_mode_active", lambda: active, raising=False)
    monkeypatch.setattr(p, "_mode_view", lambda: view, raising=False)
    monkeypatch.setattr(p, "_gateway_catalog", lambda: deepcopy(catalog or []), raising=False)
    return status


def test_mode_tabs_browse_only_with_active_dot_independent_of_selection(plugin, monkeypatch):
    """选中路由页签时，仍以直连圆点和当前卡表达实际生效模式。"""
    status = set_modes(plugin, monkeypatch, active="direct", view="route")
    p, store, settings = plugin
    before = deepcopy(store.data)

    page = p.get_page()
    tabs = actions(page, "mode_view")
    assert [(next(child["text"] for child in node["content"] if child.get("text")),
             call["params"]["mode"]) for node, call in tabs] == [
        ("直连", "direct"), ("路由", "route"), ("聚合", "stack"),
    ]
    assert [call["params"]["mode"] for node, call in tabs if node["props"]["aria-pressed"]] == ["route"]
    active_tabs = [call["params"]["mode"] for node, call in tabs
                   if any(child.get("props", {}).get("aria-label") == "当前生效"
                          for child in nodes(node.get("content", [])))]
    assert active_tabs == ["direct"]
    current = next(node for node in nodes(page) if node.get("props", {}).get("aria-label") == "当前生效配置")
    assert "直连" in visible_text(current)
    assert "直连供应商" in visible_text(current)
    assert "路由供应商" not in visible_text(current)
    activation = actions(page, "mode_activate")
    assert len(activation) == 1
    assert activation[0][0]["text"] == "启用路由"
    assert activation[0][1]["params"]["mode"] == "route"
    assert status["active"] == "direct"
    assert store.data == before
    assert settings.writes == []


def test_route_card_uses_pointer_without_relabeling_direct_profile(plugin, monkeypatch):
    """路由中的配置由独立路由指针决定，旧直连配置不显示正在使用。"""
    set_modes(plugin, monkeypatch, active="route", view="route")
    p, _store, _settings = plugin
    page = p.get_page()
    calls = actions(page, "route_select")
    assert {call["params"]["pid"] for _node, call in calls} == {"direct", "route", "other"}
    target = next(node for node, call in calls if call["params"]["pid"] == "route")
    assert target["props"]["disabled"] is True
    assert "路由中" in visible_text(page)
    assert not actions(page, "apply")
    assert not actions(page, "toggle_queue")


def test_route_failover_is_separate_control_and_exposes_queue_actions(plugin, monkeypatch):
    """路由页故障转移为单独开关，开启后每行操作变为队列加入或移出。"""
    set_modes(plugin, monkeypatch, active="route", view="route")
    p, store, _settings = plugin
    p._auto_failover = True
    values = store.data[(PLUGIN_ID, "profiles")]
    values[2]["in_failover_queue"] = False
    page = p.get_page()
    failover = actions(page, "toggle_failover")
    assert len(failover) == 1
    assert failover[0][0]["props"]["role"] == "switch"
    assert failover[0][0]["props"]["aria-checked"] is True
    assert "model" not in failover[0][0]["props"]
    queue_calls = actions(page, "toggle_queue")
    assert {call["params"]["pid"] for _node, call in queue_calls} == {"direct", "route", "other"}
    assert next(node["text"] for node, call in queue_calls if call["params"]["pid"] == "other") == "加入队列"
    assert not actions(page, "route_select")


def test_browsing_route_does_not_allow_failover_before_route_activation(plugin, monkeypatch):
    """故障转移只适用于已启用路由，浏览其他模式不会发无效切换请求。"""
    set_modes(plugin, monkeypatch, active="direct", view="route")
    p, _store, _settings = plugin
    switch = actions(p.get_page(), "toggle_failover")[0][0]
    assert switch["props"]["disabled"] is True
    assert "启用路由" in switch["props"]["title"]


def test_current_stack_model_displays_selected_member_identity(plugin, monkeypatch):
    """聚合当前模型的端点来自被选成员，不错误沿用默认供应商端点。"""
    set_modes(plugin, monkeypatch, active="stack", view="stack", members=["route", "other"],
              selected_model="stable-other/other-model", catalog=[
                  {"id": "stable-other/other-model", "name": "其他模型", "provider_id": "other",
                   "provider_name": "其他供应商", "model": "other-model"}])
    p, store, _settings = plugin
    store.data[(PLUGIN_ID, "profiles")][2]["snapshot"]["llm"]["LLM_BASE_URL"] = "https://other.example/v1"
    page = p.get_page()
    current = next(node for node in nodes(page) if node.get("props", {}).get("aria-label") == "当前生效配置")
    assert "other.example" in visible_text(current)
    assert "a.example" not in visible_text(current)


def test_browsing_stack_disables_model_switch_until_stack_is_active(plugin, monkeypatch):
    """目录在浏览时可查看，模型选择需要先明确启用聚合。"""
    set_modes(plugin, monkeypatch, active="direct", view="stack", catalog=[
        {"id": "stable-route/route-model", "name": "路由模型", "provider_id": "route",
         "provider_name": "路由供应商", "model": "route-model"},
    ])
    p, _store, _settings = plugin
    select = actions(p.get_page(), "stack_model")[0][0]
    assert select["props"]["disabled"] is True
    assert "启用聚合" in select["props"]["title"]


def test_stack_default_compatibility_entry_shows_real_model_without_alias_selection(plugin, monkeypatch):
    """未选择别名时，页面展示默认真实模型，不声称已选中任一聚合别名。"""
    set_modes(plugin, monkeypatch, active="stack", view="stack", selected_model="", catalog=[
        {"id": "stable-route/route-model", "name": "路由模型", "provider_id": "route",
         "provider_name": "路由供应商", "model": "route-model"},
    ])
    p, _store, _settings = plugin
    page = p.get_page()
    current = next(node for node in nodes(page) if node.get("props", {}).get("aria-label") == "当前生效配置")
    assert "默认 · route-model" in visible_text(current)
    assert "当前选择：默认 · route-model" in visible_text(page)
    assert all(node["text"] == "选择模型" for node, _call in actions(page, "stack_model"))


def test_stack_protects_default_and_offers_model_catalog_selection(plugin, monkeypatch):
    """聚合页不使用故障转移，默认成员不可移出，模型别名原样提交。"""
    catalog = [{"id": "default", "name": "默认模型", "provider_id": "route", "provider_name": "路由供应商", "model": "route-model"},
               {"id": "stable-other/other-model", "name": "其他模型", "provider_id": "other", "provider_name": "其他供应商", "model": "other-model"}]
    set_modes(plugin, monkeypatch, active="stack", view="stack", members=["route", "other"], catalog=catalog)
    p, _store, _settings = plugin
    p._auto_failover = True
    page = p.get_page()
    assert not actions(page, "toggle_failover")
    assert not actions(page, "toggle_queue")
    assert not actions(page, "failover_now")
    toggles = actions(page, "stack_toggle")
    default = next(node for node, call in toggles if call["params"]["pid"] == "route")
    assert default["props"]["disabled"] is True
    assert "默认" in default["props"]["title"]
    assert next(node["text"] for node, call in toggles if call["params"]["pid"] == "other") == "移出聚合"
    assert next(node["text"] for node, call in toggles if call["params"]["pid"] == "direct") == "加入聚合"
    models = actions(page, "stack_model")
    assert {call["params"]["model"] for _node, call in models} == {"default", "stable-other/other-model"}
    assert "聚合模型目录" in visible_text(page)
    enclosed = [call for disclosure in nodes(page) if disclosure.get("component") == "details"
                for _node, call in actions(disclosure, "stack_default")]
    assert {call["params"]["pid"] for call in enclosed} == {"direct", "other"}


def test_browsing_direct_while_proxy_active_requires_explicit_activation(plugin, monkeypatch):
    """apply 在代理模式会更换路由指针，所以直连页使用按钮要先启用直连。"""
    set_modes(plugin, monkeypatch, active="stack", view="direct")
    p, _store, _settings = plugin
    page = p.get_page()
    assert all(node["props"]["disabled"] for node, _call in actions(page, "apply"))
    activate = actions(page, "mode_activate")
    assert activate[0][0]["text"] == "启用直连"
    assert activate[0][1]["params"]["mode"] == "direct"


def test_unattached_proxy_keeps_mode_identity_and_offers_restart(plugin, monkeypatch):
    """网关未就绪不回退伪装成直连，仍可显式重新启用当前代理模式。"""
    set_modes(plugin, monkeypatch, active="route", view="route")
    p, _store, _settings = plugin
    monkeypatch.setattr(p, "_mode_ready", lambda: False)
    page = p.get_page()
    current = next(node for node in nodes(page) if node.get("props", {}).get("aria-label") == "当前生效配置")
    assert "当前生效 · 路由" in visible_text(current)
    activation = actions(page, "mode_activate")
    assert activation[0][1]["params"]["mode"] == "route"
    assert activation[0][0]["props"]["disabled"] is False
    assert "网关尚未就绪" in visible_text(page)


def test_mode_form_is_readonly_and_points_to_supported_details_actions(plugin, monkeypatch):
    """FormRender 不执行按钮 API，设置页只展示状态并指向数据详情页。"""
    set_modes(plugin, monkeypatch, active="route", view="stack")
    p, store, settings = plugin
    before = deepcopy(store.data)
    form, defaults = build_form(p)
    assert all(node.get("props", {}).get("model") not in {"active_mode", "_view_mode"}
               for node in nodes(form))
    assert "active_mode" not in defaults
    assert "当前生效：路由" in visible_text(form)
    assert "正在查看：聚合" in visible_text(form)
    assert "在数据详情页查看和启用模式" in visible_text(form)
    assert not actions(form, "mode_activate")
    assert not actions(form, "mode_view")
    assert settings.writes == []
    assert store.data == before


def test_mode_page_masks_secrets_in_status_catalog_and_visible_action_labels(plugin, monkeypatch):
    """模型目录和模式状态中的不可信文字也经过凭据清洗，令牌仅存在请求参数。"""
    secret = "sk-model-catalog-secret"
    hostile = '<img onerror="bad()">'
    set_modes(plugin, monkeypatch, active="stack", view="stack", catalog=[
        {"id": "safe-model", "name": f"目录 {secret} {hostile}", "provider_name": secret,
         "provider_id": "route", "model": "safe-model"},
    ])
    p, store, settings = plugin
    store.data[(PLUGIN_ID, "profiles")][1]["snapshot"]["llm"]["LLM_API_KEY"] = secret
    page = p.get_page()
    assert secret not in json.dumps(page, ensure_ascii=False)
    assert settings.API_TOKEN not in visible_text(page)
    assert all(hostile not in str(node.get("html", "")) for node in nodes(page))
    for _node, call in actions(page):
        assert call["params"]["apikey"] == settings.API_TOKEN
        assert call["method"] == "get"
        assert call["api"].startswith("plugin/agentconfigprofile/")



def test_selected_stack_members_removal_button_is_disabled(plugin, monkeypatch):
    set_modes(plugin, monkeypatch, active="stack", view="stack", members=["route", "other"])
    p, _store, _settings = plugin
    status = p._mode_status()
    status["selected_model"] = "ccs-stable-other/other-model"
    monkeypatch.setattr(p, "_mode_status", lambda: status)
    page = p.get_page()
    selected = next(node for node, action in actions(page, "stack_toggle") if action["params"]["pid"] == "other")
    assert selected["props"]["disabled"] is True


def test_mode_icons_are_shipped_without_remote_icon_requests(plugin):
    p, _store, _settings = plugin
    tabs = actions(p.get_page(), "mode_view")
    for tab, _action in tabs:
        assert "prepend-icon" not in tab["props"]
        assert any("<svg" in child.get("html", "") for child in tab.get("content", []))
