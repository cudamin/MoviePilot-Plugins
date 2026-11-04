"""
MP Switch 配置页：基础、自动切换、模型发现和全局参数。

界面声明已有配置字段，只读展示模式状态；模式操作在数据详情页执行。
"""

from __future__ import annotations

from typing import Any

from .presets import get_user_agent_presets

# 文本和链接在窄窗口中允许断行，避免长仓库地址撑宽配置面板。
_WRAP_STYLE = "min-width: 0; overflow-wrap: anywhere;"


def _text(text: str, classes: str = "text-body-2 text-medium-emphasis") -> dict:
    return {"component": "div", "props": {"class": classes, "style": _WRAP_STYLE}, "text": text}


def _row(*controls: dict) -> dict:
    return {"component": "VRow", "props": {"dense": True}, "content": list(controls)}


def _control(model: str, label: str, *, component: str = "VTextField", md: int = 6,
             hint: str = "", **props: Any) -> dict:
    """
    构造带固定提示的响应式配置项，模型键保持宿主原有保存约定。
    """
    control_props = {"model": model, "label": label, "color": "primary", "style": _WRAP_STYLE}
    if component == "VSwitch":
        control_props.update({"density": "compact", "inset": True})
    else:
        control_props.update({"variant": "outlined", "density": "comfortable"})
    if hint:
        control_props.update({"hint": hint, "persistent-hint": True})
    control_props.update(props)
    return {"component": "VCol", "props": {"cols": 12, "md": md, "class": "pb-3", "style": _WRAP_STYLE},
            "content": [{"component": component, "props": control_props}]}


def _switch(model: str, label: str, hint: str, md: int = 4) -> dict:
    return _control(model, label, component="VSwitch", md=md, hint=hint)


def _select(model: str, label: str, items: list[dict], *, md: int = 6, hint: str = "", **props: Any) -> dict:
    return _control(model, label, component="VSelect", md=md, hint=hint, items=items, **props)


def _options(values) -> list[dict]:
    return [{"title": title, "value": value} for value, title in values]


def _section(title: str, description: str, *rows: dict) -> dict:
    """
    用窄标题栏和宽输入栏区分职责，在移动端依次堆叠。
    """
    return {"component": "VRow", "props": {"class": "py-4 ma-0"}, "content": [
        {"component": "VCol", "props": {"cols": 12, "md": 3, "class": "px-0 pe-md-5", "style": _WRAP_STYLE},
         "content": [_text(title, "text-subtitle-1 font-weight-medium mb-1"), _text(description, "text-caption text-medium-emphasis")]},
        {"component": "VCol", "props": {"cols": 12, "md": 9, "class": "px-0", "style": _WRAP_STYLE},
         "content": list(rows)},
    ]}


def _panel(*sections: dict) -> dict:
    content = []
    for section in sections:
        if content:
            content.append({"component": "VDivider"})
        content.append(section)
    return {"component": "div", "props": {"style": _WRAP_STYLE}, "content": content}


def _mode_summary(plugin) -> dict:
    """模式只读展示，普通表单保存不会携带可修改的生效模式字段。"""
    names = {"direct": "直连", "route": "路由", "stack": "聚合"}
    reader = getattr(plugin, "_mode_status", None)
    available = bool(getattr(plugin, "_lock", None) is not None and callable(reader))
    status = (reader() or {}) if available else {}
    active = status.get("active", "direct")
    view = status.get("view", active)
    active = active if active in names else "direct"
    view = view if view in names else active
    children = [_text(f"当前生效：{names[active]} · 正在查看：{names[view]}",
                      "text-body-2 font-weight-medium"),
                _text("在数据详情页查看和启用模式。保存这些设置不改变生效模式。",
                      "text-caption text-medium-emphasis mt-1")]
    return {"component": "div", "props": {"class": "mb-4", "style": _WRAP_STYLE}, "content": children}


def build_form(plugin) -> tuple[list[dict], dict[str, Any]]:
    """
    返回四个标签页、只读模式状态及兼容旧版的默认配置，不改写当前配置。

    :param plugin: 提供插件名称、说明和现有选项目录的插件实例
    :return: MoviePilot 免构建表单结构与原有配置键的默认值
    """
    basic = _panel(
        _section("运行与凭据", "启用后按配置管理智能助手的供应商。",
                 _row(_switch("enabled", "启用 MP Switch", "运行定时检查与配置接管。"),
                      _switch("include_credentials", "保存 API Key", "API Key 随配置以明文保存。"),
                      _switch("notify", "切换通知", "切换完成后发送插件通知。")),
                 _text("不保存 API Key 时，仅相同供应商与端点可复用当前密钥。跨端点配置需提供独立密钥。",
                       "text-caption text-medium-emphasis")),
        _section("保存时的动作", "日常切换、排序与探活在数据详情页操作。",
                 _row(_select("action", "执行动作", [
                     {"title": "不执行", "value": "none"},
                     {"title": "保存当前配置", "value": "save"},
                     {"title": "发现模型并导入", "value": "discover"},
                 ], hint="仅在本次保存插件时执行一次。"),
                      _control("new_profile_name", "配置名称", hint="保存当前配置时可填；同名会更新原配置。"))),
    )
    failover = _panel(
        _section("切换策略", "仅尝试自动切换队列中的配置，按列表顺序选择。",
                 _row(_switch("auto_failover", "自动切换", "当前配置连续失败后切换。"),
                      _switch("takeover", "即时接管", "失效期间用健康备选接管调用。"),
                      _switch("auto_recover", "恢复首选", "首选配置恢复后自动切回。"))),
        _section("探活计划", "检查以最小模型请求确认配置可用。",
                 _row(_control("check_cron", "检查周期", hint="五段 cron 表达式。", placeholder=plugin._DEFAULT_CRON),
                      _control("fail_threshold", "连续失败阈值", hint="达到此次数后触发切换，至少 1 次。", type="number", min=1)),
                 _row(_control("test_timeout", "请求超时", hint="单位为秒，至少 5 秒。", type="number", min=5),
                      _control("probe_prompt", "探活提示词", hint="尽量使用简短文本，减少请求消耗。", placeholder="请只回复 OK")),
                 _text("熔断后冷却 60 秒，再通过两次成功探测确认恢复。探活可能产生模型调用费用。",
                       "text-caption text-medium-emphasis")),
    )
    discover = _panel(
        _section("连接端点", "留空时沿用当前生效配置的地址与密钥。",
                 _row(_control("discover_base_url", "基础 URL", placeholder="https://api.example.com/v1",
                               hint="填写完整 API 基础地址。"),
                      _control("discover_api_key", "API Key", type="password", autocomplete="new-password",
                               hint="更换端点时需填写该端点的密钥。"))),
        _section("模型范围", "白名单优先；填写后仅导入名单内的模型。",
                 _row(_control("whitelist", "模型白名单", component="VTextarea", md=7, rows=3,
                               **{"auto-grow": True}, hint="用逗号或换行分隔；白名单不受导入上限限制。",
                               placeholder="claude-sonnet, gpt-model, deepseek-model"),
                      _select("discover_providers", "探测协议", _options(plugin._DISCOVER_PROVIDERS.items()),
                              md=5, hint="选择端点支持的协议。", multiple=True, chips=True))),
        _section("发现与导入", "白名单为空时，按过滤条件和协议上限挑选模型。",
                 _row(_control("discover_filter", "模型名过滤", hint="可填正则表达式；白名单为空时生效。",
                               placeholder="claude|gpt|deepseek"),
                      _control("discover_limit", "每协议上限", hint="0 表示不限；白名单不受此项限制。", type="number", min=0)),
                 _row(_switch("discover_auto_import", "自动导入", "发现完成后生成供应商配置。", md=6),
                      _switch("discover_smart_match", "智能匹配协议", "白名单为空时按模型名选择协议。", md=6))),
    )
    global_options = _panel(
        _section("统一配置", "开启并保存后，覆盖已有及后续新建配置，同时写入系统设置。",
                 _row(_switch("apply_global", "启用全局参数", "也可在数据详情页手动应用一次。", md=12))),
        _section("模型能力", "这些开关需与所选模型实际支持的能力一致。",
                 _row(_select("g_thinking_level", "思考模式", _options(plugin._THINKING_LEVEL_OPTIONS), md=6),
                      _switch("g_use_proxy", "系统代理", "模型请求使用系统代理设置。", md=6)),
                 _row(_switch("g_image_input", "图片输入", "允许提交图片。"),
                      _switch("g_audio_input", "音频输入", "允许提交音频。"),
                      _switch("g_audio_output", "音频输出", "允许模型返回音频。"))),
        _section("请求参数", "统一请求协议、联网方式与 User-Agent。",
                 _row(_select("g_api_protocol", "API 协议", _options(plugin._API_PROTOCOL_OPTIONS)),
                      _select("g_web_search_mode", "联网搜索", _options(plugin._WEB_SEARCH_MODE_OPTIONS))),
                 _row(_control("g_user_agent", "User-Agent（预设或自定义）", component="VCombobox", md=12,
                               hint="可选 CC Switch 预设或输入自定义值；留空使用 SDK 默认。",
                               items=list(get_user_agent_presets()), clearable=True))),
        _section("智能助手", "这些参数只写入系统设置，不属于供应商配置。",
                 _row(_select("g_job_interval", "定时唤醒", [{"title": "不启用", "value": 0}] + [
                     {"title": f"{hours} 小时", "value": hours} for hours in plugin._JOB_INTERVAL_OPTIONS
                 ], md=12, hint="按选定间隔唤醒智能助手。")),
                 _row(_switch("g_retry_transfer", "整理失败接管", "整理失败时交给智能助手。", md=6),
                      _switch("g_ai_recommend", "搜索智能推荐", "让智能助手推荐搜索结果。", md=6))),
    )
    tabs = [("basic", "基础", basic), ("failover", "自动切换", failover),
            ("discover", "模型发现", discover), ("global", "全局参数", global_options)]
    form = [
        {"component": "div", "props": {"class": "mb-4", "style": _WRAP_STYLE}, "content": [
            _text(plugin.plugin_name, "text-h6 font-weight-medium mb-1"),
            _text(plugin.plugin_desc, "text-caption text-medium-emphasis"),
        ]},
        _mode_summary(plugin),
        {"component": "VTabs", "props": {"model": "_active_tab", "color": "primary", "grow": True},
         "content": [{"component": "VTab", "props": {"value": value}, "text": title} for value, title, _panel_data in tabs]},
        {"component": "VDivider", "props": {"class": "mb-1"}},
        {"component": "VWindow", "props": {"model": "_active_tab"}, "content": [
            {"component": "VWindowItem", "props": {"value": value}, "content": [panel]} for value, _title, panel in tabs
        ]},
    ]
    default_config = {
        "_active_tab": "basic", "enabled": False, "include_credentials": True, "notify": True,
        "auto_failover": False, "takeover": True, "auto_recover": False,
        "check_cron": plugin._DEFAULT_CRON, "fail_threshold": 2, "test_timeout": 20, "probe_prompt": "请只回复 OK",
        "discover_base_url": "", "discover_api_key": "", "discover_providers": list(plugin._DISCOVER_PROVIDERS),
        "discover_filter": "", "discover_limit": 20, "discover_auto_import": True, "discover_smart_match": True,
        "whitelist": "", "apply_global": False, "g_thinking_level": "off", "g_use_proxy": True,
        "g_api_protocol": "auto", "g_web_search_mode": "local", "g_user_agent": "", "g_job_interval": 0,
        "g_image_input": True, "g_audio_input": False, "g_audio_output": False,
        "g_retry_transfer": False, "g_ai_recommend": False, "action": "none", "new_profile_name": "",
    }
    return form, default_config
