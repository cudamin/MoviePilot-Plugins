"""
MP Switch 三模式供应商页：浏览模式、当前生效配置和模式对应的操作。

详情页只使用宿主 PageRender 已支持的组件与事件合同；配置页由 forms 构造。
"""

from typing import Any, Dict, List, Optional, Tuple

from .forms import build_form
from .mode_icons import mode_svg
from .page_style import PAGE_STYLE

SOURCE_URL = "https://github.com/farion1231/cc-switch?tab=readme-ov-file"
_WRAP = "min-width: 0; overflow-wrap: anywhere;"
_MODES = {"direct": ("直连", "mdi-power-plug-outline"),
          "route": ("路由", "mdi-routes"), "stack": ("聚合", "mdi-layers-outline")}


def _node(component: str, text: Any = "", *, classes: str = "", children=None, **props) -> dict:
    """
    构造宿主支持的普通组件节点，动态文字始终通过文本插槽渲染。
    """
    if classes:
        props["class"] = classes
    node = {"component": component, "props": props}
    if text != "":
        node["text"] = str(text)
    if children is not None:
        node["content"] = children
    return node


def _text(value: Any, classes: str = "ms-wrap") -> dict:
    return _node("div", value, classes=classes, style=_WRAP)


def _tag(value: str, primary: bool = False) -> dict:
    return _node("span", value, classes="ms-tag ms-tag-primary" if primary else "ms-tag")


def _disclosure(title: str, children: list[dict], *, small: bool = False) -> dict:
    """
    使用原生键盘可操作的折叠区域，不依赖宿主未支持的具名插槽。
    """
    return _node("details", classes="ms-more" if small else "ms-disclosure", children=[
        _node("summary", title), *children,
    ])


class ViewMixin:
    """
    以浏览模式组织供应商；展示过程不修改配置，不发起模型请求。
    """

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        提供保持原配置键与默认值的四分区设置表单。
        """
        return build_form(self)

    def _api_btn(self, text: str, path: str, params: Dict[str, Any], color: str = "primary",
                 variant: str = "text", size: str = "small", title: str = "",
                 *, icon: str = "", disabled: bool = False, attrs: Optional[dict] = None) -> dict:
        """
        构造受鉴权保护的宿主动作按钮，附带可访问名称并保留旧调用签名。

        PageRender 的 GET 参数可被宿主鉴权读取；事件成功后会通知宿主刷新数据。
        """
        query = {**(params or {}), "apikey": self._settings.API_TOKEN}
        props = {"color": color, "variant": variant, "size": size, "class": "ms-button",
                 "disabled": disabled, "aria-label": title or text, "title": title or text,
                 "style": "text-transform: none; letter-spacing: 0;"}
        if icon:
            props["prepend-icon"] = icon
        props.update(attrs or {})
        return {"component": "VBtn", "props": props, "text": text,
                "events": {"click": {"api": f"plugin/{self.__class__.__name__}/{path}",
                                     "method": "get", "params": query}}}

    def _page_cleaner(self, profiles):
        """
        为一轮渲染集中收集已保存密钥，清洗旧日志、错误和提示，不改变原数据。
        """
        secrets = sorted({str(self._profile_llm(profile).get("LLM_API_KEY") or "")
                          for profile in profiles} - {""}, key=len, reverse=True)

        def clean(value):
            text = str(value or "")
            for secret in secrets:
                text = text.replace(secret, "***")
            return self._sanitize(text)

        return clean

    def _health_label(self, profile: dict) -> Tuple[str, str]:
        """
        同时表达探活有效期和熔断状态，过期成功不会显示为可接管状态。
        """
        llm = self._effective_llm(self._profile_llm(profile))
        circuit = self._circuit_for(llm).get_state().value
        if circuit == "open":
            return "熔断冷却中", "error"
        if circuit == "half_open":
            return "恢复确认中", "warning"
        health = profile.get("health") or {}
        if health.get("status") == "fail":
            return "探活失败", "error"
        if self._health_is_fresh(profile):
            return f"正常 · {self._to_int(health.get('duration_ms'), 0)} ms", "success"
        if health.get("status") == "ok":
            return "检查已过期", "muted"
        return "未探活", "muted"

    def _page_modes(self, profiles: list, current: dict) -> dict:
        """读取独立的模式状态，未初始化的旧实例只显示直连兼容状态。"""
        ready = getattr(self, "_mode_ready", None)
        status_reader = getattr(self, "_mode_status", None)
        available = bool(getattr(self, "_lock", None) is not None and callable(status_reader))
        stored = (status_reader() or {}) if available else {}
        gateway_ready = bool(available and callable(ready) and ready())
        active = stored.get("active", "direct")
        view = stored.get("view", active)
        active = active if active in _MODES else "direct"
        view = view if view in _MODES else active
        current_profile = next((profile for profile in profiles
                                if self._profile_is_active(profile, current)), {})
        legacy = not available or (active == view == "direct" and not stored.get("revision")
                                   and not stored.get("direct_id") and not stored.get("route_id"))
        return {**stored, "active": active, "view": view, "available": available,
                "ready": gateway_ready, "legacy": legacy,
                "direct_id": stored.get("direct_id") or current_profile.get("id") or "",
                "route_id": stored.get("route_id") or "",
                "stack_members": stored.get("stack_members") or [],
                "stack_default_id": stored.get("stack_default_id") or stored.get("route_id") or ""}

    def _mode_tabs(self, modes: dict) -> dict:
        """三段页签只浏览；凸起标正在查看，模式色和圆点标实际生效。"""
        tabs = []
        for mode, (name, icon) in _MODES.items():
            selected = modes["view"] == mode
            active = modes["active"] == mode
            tab = self._api_btn(name, "mode_view", {"mode": mode}, color="default",
                                size="default", variant="text", title=f"查看{name}模式",
                                attrs={"class": f"ms-mode-tab ms-mode-{mode}" +
                                       (" is-selected" if selected else "") + (" is-current" if active else ""),
                                       "aria-pressed": selected})
            tab.pop("text", None)
            tab["content"] = [{"component": "span", "props": {"class": "ms-mode-icon"}, "html": mode_svg(mode)},
                              _node("span", name)]
            if active:
                tab["content"].append(_node("span", classes="ms-mode-dot", role="img",
                                           **{"aria-label": "当前生效"}))
            tabs.append(tab)
        controls = []
        if modes["view"] == "route":
            on = bool(self._auto_failover)
            controls.append(self._api_btn("故障转移", "toggle_failover", {}, color="default",
                                          title="请先启用路由模式" if modes["active"] != "route" else
                                          "关闭故障转移" if on else "开启故障转移",
                                          disabled=not modes["available"] or modes["active"] != "route",
                                          attrs={"role": "switch", "aria-checked": on,
                                                 "class": "ms-failover" + (" is-on" if on else ""),
                                                 "append-icon": "mdi-toggle-switch" if on else "mdi-toggle-switch-off-outline"}))
        return _node("div", classes="ms-mode-row", children=[
            _node("div", classes="ms-mode-tabs", role="group", **{"aria-label": "供应商模式"}, children=tabs),
            _node("div", classes="ms-mode-controls", children=controls),
        ])

    def _mode_activation(self, modes: dict) -> dict:
        """浏览与启用分开，只有明确的启用请求改变生效模式。"""
        view, active = modes["view"], modes["active"]
        name = _MODES[view][0]
        descriptions = {"direct": "直接使用一家供应商的配置。",
                        "route": "通过本地网关路由到所选供应商，可独立开启故障转移。",
                        "stack": "将聚合成员的模型发布到同一目录，按所选模型调用。"}
        return _node("section", classes=f"ms-activation ms-mode-{view}", children=[
            _node("div", classes="ms-wrap", children=[
                _text(f"{name}模式已生效" if view == active and (active == "direct" or modes["ready"])
                      else f"{name}网关尚未就绪" if view == active
                      else f"正在查看{name} · 当前生效{_MODES[active][0]}",
                      "ms-activation-title"),
                _text(descriptions[view], "ms-muted"),
            ]),
            self._api_btn(f"启用{name}", "mode_activate", {"mode": view}, color="default", variant="flat",
                          disabled=(view == active and (active == "direct" or modes["ready"])) or not modes["available"],
                          title=f"启用{name}模式" if view != active else f"{name}模式已生效",
                          attrs={"class": f"ms-mode-activate ms-mode-{view}"}),
        ])

    def _stack_catalog(self, modes: dict, clean, catalog: Optional[list] = None) -> dict:
        """只读取已发布模型目录，不因展开页面触发上游发现或配置写入。"""
        if catalog is None:
            reader = getattr(self, "_gateway_catalog", None)
            catalog = reader() if modes["available"] and callable(reader) else []
        selected = str(modes.get("selected_model") or "")
        selected_label = clean(selected) if selected else f"默认 · {clean(modes.get('default_model') or '尚未配置模型')}"
        rows = []
        for item in catalog or []:
            alias = str(item.get("id") or "")
            if not alias:
                continue
            rows.append(_node("div", classes="ms-catalog-row", children=[
                _node("div", classes="ms-wrap", children=[
                    _text(clean(item.get("name") or alias), "ms-catalog-name"),
                    _text(clean(f"{item.get('provider_name') or '默认供应商'} · {item.get('model') or alias}"), "ms-muted"),
                ]),
                self._api_btn("已选择" if alias == selected else "选择模型", "stack_model", {"model": alias},
                              color="default", variant="tonal" if alias == selected else "outlined",
                              disabled=alias == selected or modes["active"] != "stack",
                              title="请先启用聚合模式" if modes["active"] != "stack" else
                              f"选择聚合模型 {clean(item.get('name') or alias)}"),
            ]))
        return _node("section", classes="ms-catalog ms-mode-stack", children=[
            _node("div", classes="ms-section-heading", children=[_node("h2", "聚合模型目录"),
                                                                     _tag(f"{len(rows)} 个模型")]),
            _text(f"当前选择：{selected_label}", "ms-muted"),
            *(rows or [_text("加入聚合成员后，已配置的模型会出现在这里。", "ms-muted mt-3")]),
        ])

    def _provider_card(self, profile: dict, index: int, total: int, current: dict,
                       primary_id: str, clean, modes: Optional[dict] = None) -> dict:
        """
        展示供应商身份、健康与主要操作，覆盖、删除和排序收纳在更多操作中。
        """
        llm = self._effective_llm(self._profile_llm(profile))
        pid = profile.get("id")
        name = clean(profile.get("name") or "未命名配置")
        provider = self._provider_label(llm.get("LLM_PROVIDER"))
        modes = modes or {"active": "direct", "view": "direct", "available": False}
        view = modes["view"]
        is_direct = pid == modes.get("direct_id") if modes["available"] else self._profile_is_active(profile, current)
        is_route = pid == modes.get("route_id")
        is_default = pid == modes.get("stack_default_id")
        member = pid in modes.get("stack_members", []) or is_default
        stack_key = next((key for key, owner in (modes.get("stack_keys") or {}).items() if owner == pid), "")
        selected_member = modes["active"] == "stack" and stack_key and str(
            modes.get("selected_model") or "").startswith(f"ccs-{stack_key}/")
        active = ((view == "direct" and modes["active"] == "direct" and is_direct) or
                  (view == "route" and modes["active"] == "route" and is_route) or
                  (view == "stack" and modes["active"] == "stack" and is_default))
        queued = self._in_failover_queue(profile)
        health, tone = self._health_label(profile)
        tags = []
        if pid == primary_id and (view == "route" and self._auto_failover or modes.get("legacy")):
            tags.append(_tag("首选", True))
        if active:
            tags.append(_tag({"direct": "使用中 · 生效", "route": "路由中", "stack": "当前默认"}[view], True))
        if view == "direct" and is_direct and modes["active"] != "direct":
            tags.append(_tag("直连时使用"))
        if view == "route" and is_route and modes["active"] != "route":
            tags.append(_tag("路由目标"))
        if view == "stack":
            tags.append(_tag("默认成员" if is_default else "聚合成员" if member else "未加入聚合"))
        elif view == "route" and self._auto_failover or modes.get("legacy"):
            tags.append(_tag("自动切换队列" if queued else "仅手动"))
        avatar = {"openai": "O", "anthropic": "A", "deepseek": "D"}.get(
            str(llm.get("LLM_PROVIDER")), str(provider or "M")[:1])
        metadata = f"{provider} · {self._host_of(llm.get('LLM_BASE_URL') or '')}"
        main = _node("div", classes="ms-provider-head", children=[
            _node("div", avatar, classes="ms-avatar", **{"aria-hidden": "true"}),
            _node("div", classes="ms-wrap", children=[
                _node("div", classes="ms-provider-title", children=[
                    _node("h3", name, style=_WRAP), *tags,
                ]),
                _text(clean(llm.get("LLM_MODEL") or "未设置模型"), "ms-model"),
                _text(clean(metadata), "ms-muted"),
            ]),
        ])
        primary = []
        if view == "direct":
            primary.append(self._api_btn("使用中" if active else "使用", "apply", {"pid": pid},
                                         color="default", variant="tonal" if active else "outlined",
                                         disabled=active or modes["active"] != "direct",
                                         title="请先启用直连模式" if modes["active"] != "direct" else
                                         f"{'正在使用' if active else '使用'} {name}"))
        elif view == "route" and not self._auto_failover:
            primary.append(self._api_btn("路由中" if active else "已选择" if is_route else "路由到此",
                                         "route_select", {"pid": pid}, color="default",
                                         variant="tonal" if active else "outlined", disabled=is_route,
                                         title=f"路由到 {name}"))
        elif view == "stack":
            primary.append(self._api_btn("移出聚合" if member else "加入聚合", "stack_toggle", {"pid": pid},
                                         color="default", variant="outlined", disabled=bool(is_default or selected_member),
                                         title="请先选择其他模型，再移出当前模型所属供应商" if selected_member else
                                         "默认成员不能移出聚合，请先设置其他默认成员" if is_default else
                                         f"{'移出' if member else '加入'}聚合：{name}"))
        if view == "route" and self._auto_failover or modes.get("legacy"):
            primary.append(self._api_btn("移出队列" if queued else "加入队列", "toggle_queue", {"pid": pid},
                                         color="default", title=f"{'移出' if queued else '加入'}自动切换队列：{name}"))
        primary.append(self._api_btn("探活", "probe", {"pid": pid}, color="default", variant="text",
                                     title=f"检查 {name} 的连接"))
        actions = _node("div", classes="ms-actions", children=primary)
        mode_more = []
        if view == "stack" and not is_default:
            mode_more.append(self._api_btn("设为默认", "stack_default", {"pid": pid}, color="default",
                                           disabled=modes["active"] == "route",
                                           title="路由运行中不能更改聚合默认成员" if modes["active"] == "route"
                                           else f"将 {name} 设为聚合默认成员"))
        details = _disclosure("更多操作", [
            _text(clean(f"协议：{llm.get('LLM_API_PROTOCOL') or 'auto'} · "
                        f"最近检查：{(profile.get('health') or {}).get('checked_at') or '尚未检查'}"), "ms-muted"),
            _text(clean((profile.get("health") or {}).get("message") or ""), "ms-muted"),
            _node("div", classes="ms-more-body", children=[
                *mode_more,
                self._api_btn("上移", "move", {"pid": pid, "dir": "up"}, color="default", disabled=index == 0),
                self._api_btn("下移", "move", {"pid": pid, "dir": "down"}, color="default", disabled=index == total - 1),
                self._api_btn("发现此端点模型", "discover", {"pid": pid}, color="default"),
                self._api_btn("用当前配置覆盖", "save_current", {"name": profile.get("name")}, color="warning",
                              title="把当前系统配置保存到此模板，替换它的原配置"),
                self._api_btn("删除配置", "delete", {"pid": pid}, color="error", title=f"删除配置 {name}"),
            ]),
        ], small=True)
        return _node("article", classes=f"ms-provider ms-mode-{view}" + (" is-active" if active else ""), children=[
            main,
            _node("div", classes="ms-provider-bottom", children=[
                _node("span", health, classes=f"ms-status ms-tone-{tone}"), actions,
            ]), details,
        ])

    def _queue_panel(self, queue: list, clean) -> dict:
        """
        显示真实队列顺序和启用条件，不把不参加队列的配置计为备选。
        """
        items = []
        for index, profile in enumerate(queue[:8]):
            health, tone = self._health_label(profile)
            items.append(_node("li", children=[
                _node("span", index + 1, classes="ms-queue-index"),
                _node("div", classes="ms-wrap", children=[
                    _text(clean(profile.get("name") or "未命名配置"), "ms-queue-name"),
                    _node("span", "首选" if index == 0 else health, classes=f"ms-muted ms-tone-{tone}"),
                ]),
            ]))
        if len(queue) > 8:
            items.append(_node("li", f"另有 {len(queue) - 8} 个配置，顺序见供应商列表", classes="ms-muted"))
        return _node("section", classes="ms-aside-section", children=[
            _node("div", classes="ms-section-heading", children=[
                _node("h2", "故障转移"), _tag("已开启" if self._enabled and self._auto_failover else "未开启"),
            ]),
            _text("按队列顺序尝试可用配置", "ms-muted"),
            _node("ol", classes="ms-queue", children=items) if items else
            _text("暂无备选配置，在供应商列表中点击“加入队列”。", "ms-muted"),
            self._api_btn("立即检查", "failover_now", {}, color="default", variant="outlined",
                          disabled=not self._enabled, title="检查当前配置，必要时执行故障转移"),
        ])

    def _policy_panel(self) -> dict:
        """
        将影响切换的策略放在同一处，配置编辑仍由宿主设置页承接。
        """
        rules = [
            ("失败阈值", f"连续 {self._fail_threshold} 次"),
            ("探活周期", self._check_cron),
            ("即时接管", "开启" if self._takeover and self._enabled else "关闭"),
            ("恢复首选", "开启" if self._auto_recover and self._enabled else "关闭"),
            ("全局参数", "开启" if self._apply_global else "关闭"),
        ]
        return _node("section", classes="ms-aside-section", children=[
            _node("h2", "运行策略"),
            *[_node("div", classes="ms-rule", children=[
                _node("span", label, classes="ms-muted"), _node("span", value, classes="ms-rule-value"),
            ]) for label, value in rules],
            _text("可在插件设置中调整检查与切换策略。", "ms-muted mt-3"),
        ])

    def _discovery_card(self, discovery: Dict[str, Any]) -> dict:
        """
        将已发现模型按协议分组，保留整批导入与单模型导入入口。
        """
        clean = self._page_cleaner(self._load_profiles())
        providers = discovery.get("providers") or {}
        model_count = sum(len(info.get("models") or []) for info in providers.values())
        running = discovery.get("status") == "running"
        body = [
            _node("div", classes="ms-section-heading", children=[
                _text(clean(self._host_of(discovery.get("base_url") or self._discover_base_url)), "ms-muted"),
                _node("div", classes="ms-actions", children=[
                    self._api_btn("发现模型", "discover", {}, variant="tonal", disabled=running),
                    self._api_btn("导入全部", "import_discovered", {}, color="default", variant="outlined",
                                  disabled=not model_count or running),
                    self._api_btn("清除结果", "clear_discovery", {}, color="default", disabled=running or not providers),
                ]),
            ]),
        ]
        if not providers:
            body.append(_text("还没有发现模型。先在插件设置的“模型发现”中填写端点，再开始发现。", "ms-muted"))
        if discovery.get("message"):
            body.append(_text(clean(discovery.get("message")), "ms-muted"))
        if discovery.get("import_message"):
            body.append(_text(clean(discovery.get("import_message")), "ms-muted"))
        if self._has_whitelist():
            status = self.whitelist_status(discovery)
            body.append(_text(f"白名单命中 {len(status.get('hit') or [])} 个；"
                              f"尚未发现 {len(status.get('miss') or [])} 个", "ms-muted"))
        for provider, label in self._DISCOVER_PROVIDERS.items():
            info = providers.get(provider)
            if not info:
                continue
            models = info.get("models") or []
            group = [
                _node("div", classes="ms-section-heading", children=[
                    _node("h3", f"{label} · {len(models)} 个模型"),
                    self._api_btn("导入此组", "import_discovered", {"provider": provider}, color="default",
                                  disabled=not models or running),
                ]),
                _text(clean(info.get("message") or ""), "ms-muted mb-3"),
                _node("div", classes="ms-actions", children=[
                    self._api_btn(clean(model.get("id")), "add_model",
                                  {"provider": provider, "model": str(model.get("id"))}, variant="outlined",
                                  color="default", title=f"添加模型 {clean(model.get('id'))}")
                    for model in models[:60] if model.get("id")
                ]),
            ]
            if len(models) > 60:
                group.append(_text("显示前 60 个模型，其余可用“导入此组”按配置范围导入。", "ms-muted mt-3"))
            body.append(_node("div", classes="ms-model-group", children=group))
        return _disclosure(f"模型发现 · {model_count} 个模型" if model_count else "模型发现", body)

    def _progress_card(self) -> Optional[dict]:
        """
        只在后台任务运行时显示进度，保留显式刷新操作。
        """
        clean = self._page_cleaner(self._load_profiles())
        probe = (self._runtime or {}).get("probe_all") or {}
        discovery = self._load_discovery()
        tasks = []
        if probe.get("status") == "running":
            total = max(1, self._to_int(probe.get("total"), 1))
            done = self._to_int(probe.get("done"), 0)
            tasks.append((f"正在检查配置 · {done}/{total}", done, total))
        if discovery.get("status") == "running":
            total = max(1, len(self._discover_providers))
            tasks.append(("正在发现端点模型", len(discovery.get("providers") or {}), total))
        if not tasks:
            return None
        rows = []
        for label, done, total in tasks:
            rows.extend([_text(clean(label), "ms-muted mb-2"), _node("VProgressLinear", **{
                "model-value": min(100, max(0, round(done * 100 / total))), "height": 4,
                "rounded": True, "color": "primary", "class": "mb-3",
            })])
        return _node("div", classes="ms-progress", **{"role": "status", "aria-live": "polite"}, children=[
            *rows, self._api_btn("刷新进度", "noop", {}, color="default"),
        ])

    def get_page(self) -> List[dict]:
        """
        构造三模式供应商页，浏览与激活操作由不同接口执行与刷新。
        """
        profiles = self._load_profiles()
        current = self._current_llm_config()
        runtime = self._runtime or {}
        config = self.get_config() or {}
        clean = self._page_cleaner(profiles)
        modes = self._page_modes(profiles, current)
        default_profile = next((profile for profile in profiles if profile.get("id") == modes["route_id"]), {})
        modes["default_model"] = self._profile_llm(default_profile).get("LLM_MODEL") or ""
        catalog_reader = getattr(self, "_gateway_catalog", None)
        catalog = (catalog_reader() if modes["available"] and callable(catalog_reader) and
                   (modes["active"] == "stack" or modes["view"] == "stack") else [])
        queue = [profile for profile in profiles if self._in_failover_queue(profile)]
        primary_id = queue[0].get("id") if queue else ""
        current_id = modes["direct_id"] if modes["active"] == "direct" else modes["route_id"]
        if modes["active"] == "stack":
            selected_entry = next((item for item in catalog or []
                                   if item.get("id") == modes.get("selected_model")), {})
            current_id = selected_entry.get("provider_id") or current_id
        active = next((profile for profile in profiles if profile.get("id") == current_id), None)
        total = len(profiles)
        page = self._current_page(total)
        pages = self._total_pages(total)
        offset = (page - 1) * self._PAGE_SIZE
        current_name = clean(active.get("name")) if active else "当前系统配置"
        current_llm = self._effective_llm(self._profile_llm(active)) if active else current
        current_model = clean(current_llm.get("LLM_MODEL") or "尚未配置模型")
        if modes["active"] == "stack":
            current_model = clean(modes.get("selected_model")) if modes.get("selected_model") else f"默认 · {current_model}"
            members = (set(modes["stack_members"]) | {modes["stack_default_id"]}) - {""}
            current_name = f"聚合 · {len(members)} 家供应商"
        checked = runtime.get("last_check_ok")
        if active and checked is None and self._health_is_fresh(active):
            checked = True
        current_health = (active or {}).get("health") or {}
        status = f"连接正常 · {runtime.get('last_check_duration') or current_health.get('duration_ms') or 0} ms" if checked else (
            "探活失败" if checked is False else "尚未检查连接")
        header = _node("header", classes="ms-header", children=[
            _node("div", classes="ms-identity", children=[
                _node("div", "⇄", classes="ms-brand", **{"aria-hidden": "true"}),
                _node("div", classes="ms-wrap", children=[
                    _node("div", classes="ms-row", children=[
                        _node("h2", self.plugin_name, classes="ms-title"),
                        _tag("已启用" if self._enabled else "已停用"),
                    ]),
                    _text(self.plugin_desc, "ms-muted"),
                ]),
            ]),
            _node("div", classes="ms-actions", children=[
                self._api_btn("刷新", "noop", {}, color="default"),
                self._api_btn("保存当前配置", "save_current", {}, variant="flat", icon="mdi-plus"),
            ]),
        ])
        notices = []
        if not self._enabled:
            notices.append(_node("VAlert", "插件已停用。可以管理配置，自动检查和接管将在启用后运行。",
                                 classes="ms-notice", type="info", variant="tonal", density="compact"))
        if config.get("last_action_message"):
            notices.append(_node("VAlert", clean(config.get("last_action_message")), classes="ms-notice",
                                 type="info", variant="tonal", density="compact"))
        current_card = _node("section", classes=f"ms-current ms-mode-{modes['active']}",
                             **{"aria-label": "当前生效配置"}, children=[
            _node("div", classes="ms-wrap", children=[
                _node("div", classes="ms-current-label", children=[
                    _node("span", classes="ms-mode-dot", **{"aria-hidden": "true"}),
                    _node("span", f"当前生效 · {_MODES[modes['active']][0]}"),
                    _node("span", current_name, style=_WRAP),
                ]),
                _node("h2", current_model, style=_WRAP),
                _text(clean(f"{self._provider_label(current_llm.get('LLM_PROVIDER'))} · "
                            f"{self._host_of(current_llm.get('LLM_BASE_URL') or '')} · "
                            f"{current_llm.get('LLM_API_PROTOCOL') or 'auto'}"), "ms-muted"),
                *([_text(clean((modes.get("gateway") or {}).get("base_url") or "本地网关尚未就绪"), "ms-muted")]
                  if modes["active"] != "direct" else []),
            ]),
            _node("div", classes="ms-current-side", children=[
                _node("span", status, classes=f"ms-status ms-tone-{'success' if checked else 'error' if checked is False else 'muted'}"),
                self._api_btn("检查连接", "probe", {}, color="default", variant="outlined"),
            ]),
        ])
        cards = [self._provider_card(profile, offset + index, total, current, primary_id, clean, modes)
                 for index, profile in enumerate(profiles[offset:offset + self._PAGE_SIZE])]
        if not cards:
            cards = [_node("div", classes="ms-empty", children=[
                _text("添加第一个供应商配置", "ms-empty-title"),
                _text("点击上方“保存当前配置”，或展开下方“模型发现”导入配置。", "ms-muted"),
            ])]
        list_content = [
            _node("div", classes="ms-section-heading", children=[
                _node("h2", f"供应商配置（{total}）"),
                self._api_btn("检查全部", "probe_all", {}, color="default", variant="outlined", disabled=not total),
            ]),
            _node("div", classes="ms-provider-list", children=cards),
        ]
        if pages > 1:
            list_content.append(_node("nav", classes="ms-pagination", **{"aria-label": "供应商列表分页"}, children=[
                _node("span", f"第 {page} / {pages} 页"),
                _node("div", classes="ms-actions", children=[
                    self._api_btn("上一页", "set_page", {"p": max(1, page - 1)}, color="default", disabled=page == 1),
                    self._api_btn("下一页", "set_page", {"p": min(pages, page + 1)}, color="default", disabled=page == pages),
                ]),
            ]))
        list_content.append(_disclosure("配置管理", [
            _node("div", classes="ms-more-body", children=[
                self._api_btn("应用全局参数", "apply_global", {}, color="default"),
                self._api_btn("规范配置名称", "normalize_names", {"scope": "discover"}, color="default"),
                self._api_btn("清理名单外配置", "prune_offlist", {}, color="warning", disabled=not self._has_whitelist()),
            ]),
        ], small=True))
        main = _node("div", classes="ms-main", children=[
            _node("section", classes="ms-wrap", children=list_content),
            *([self._queue_panel(queue, clean)] if modes["view"] == "route" and self._auto_failover else []),
            _disclosure("运行策略", [self._policy_panel()]),
        ])
        log_rows = [_node("div", classes="ms-log", children=[
            _text(item.get("time") or "—", "ms-muted"), _text(clean(item.get("text") or "")),
        ]) for item in (runtime.get("log") or [])[:20]]
        logs = _disclosure(f"运行记录 · {len(runtime.get('log') or [])} 条", [
            _node("div", classes="ms-section-heading", children=[
                _text("切换、探活与模型发现的最近记录", "ms-muted"),
                self._api_btn("清空记录", "clear_log", {}, color="default", disabled=not log_rows),
            ]),
            *(log_rows or [_text("暂无运行记录", "ms-muted")]),
        ])
        footer = _node("footer", classes="ms-footer", children=[
            _node("div", classes="ms-wrap", children=[
                _node("span", "参考CC Switch制作，仓库地址"),
                _node("a", "CC Switch 源码", href=SOURCE_URL, target="_blank", rel="noopener noreferrer"),
            ]),
            _node("span", f"MP Switch {self.plugin_version}"),
        ])
        content = [{"component": "style", "html": PAGE_STYLE}, header, self._mode_tabs(modes),
                   *notices, current_card, self._mode_activation(modes)]
        progress = self._progress_card()
        if progress:
            content.append(progress)
        content.append(main)
        if modes["view"] == "stack":
            content.append(self._stack_catalog(modes, clean, catalog))
        content.extend([self._discovery_card(self._load_discovery()), logs, footer])
        return [_node("div", classes="mp-switch", style=_WRAP, children=content)]

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """
        声明一个可自动刷新的进度仪表盘。
        """
        if not self.get_state():
            return []
        return [{"key": "progress", "name": f"{self.plugin_name} · 任务进度"}]

    def get_dashboard(self, key: str = "", **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any],
                                                                      Optional[List[dict]]]]:
        """
        自动刷新的进度面板。

        详情页的 Vuetify 配置不支持自行轮询，这里用仪表盘的 refresh 能力做到
        探测/探活过程中每 5 秒自动刷新一次。
        """
        clean = self._page_cleaner(self._load_profiles())
        runtime = self._runtime or {}
        discovery = self._load_discovery()
        probe = runtime.get("probe_all") or {}
        current = self._current_llm_config()

        col = {"cols": 12, "md": 6}
        # 忙时 5 秒刷新跟踪进度；空闲时降到 30 秒，减少无谓轮询
        busy = (probe.get("status") == "running" or discovery.get("status") == "running")
        global_config = {"refresh": 5 if busy else 30, "border": True,
                         "title": self.plugin_name, "subtitle": "探测与探活进度"}

        items: List[dict] = [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center mb-2"}, "content": [
                {"component": "VChip",
                 "props": {"size": "small", "color": "primary", "variant": "flat", "class": "me-2 mb-1"},
                 "text": clean(f"{current.get('LLM_PROVIDER') or '-'} / {current.get('LLM_MODEL') or '-'}")},
                {"component": "VChip", "props": {"size": "small", "variant": "tonal", "class": "me-2 mb-1"},
                 "text": clean(self._host_of(current.get("LLM_BASE_URL") or ""))},
            ]},
        ]

        if probe.get("status") == "running":
            total = max(1, self._to_int(probe.get("total"), 1))
            done = self._to_int(probe.get("done"), 0)
            items.append({"component": "div", "props": {"class": "text-body-2 mb-1"},
                          "text": clean(f"探活中 {done}/{total}：{probe.get('current') or ''}")})
            items.append({"component": "VProgressLinear",
                          "props": {"model-value": round(done * 100 / total), "height": 6,
                                    "rounded": True, "color": "info", "class": "mb-2"}})
        if discovery.get("status") == "running":
            total = max(1, len(self._discover_providers))
            done = len(discovery.get("providers") or {})
            items.append({"component": "div", "props": {"class": "text-body-2 mb-1"},
                          "text": f"模型探测中 {done}/{total} 个协议："
                                  f"{self._host_of(discovery.get('base_url') or '')}"})
            items.append({"component": "VProgressLinear",
                          "props": {"model-value": round(done * 100 / total), "height": 6,
                                    "rounded": True, "color": "success", "class": "mb-2"}})

        if probe.get("status") != "running" and discovery.get("status") != "running":
            last_ok = runtime.get("last_check_ok")
            if last_ok is None:
                text = "空闲｜尚未探活"
            elif last_ok:
                text = f"空闲｜当前配置正常 {runtime.get('last_check_duration') or 0}ms" \
                       f"（{runtime.get('last_check_at') or '-'}）"
            else:
                text = f"空闲｜当前配置探活失败：{clean(runtime.get('last_check_message') or '')[:60]}"
            items.append({"component": "div", "props": {"class": "text-body-2"}, "text": clean(text)})
            if probe.get("finished_at"):
                items.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis"},
                              "text": f"上次全量探活 {probe.get('finished_at')}：正常 "
                                      f"{self._to_int(probe.get('ok'), 0)} 个，失败 "
                                      f"{self._to_int(probe.get('fail'), 0)} 个"})

        return col, global_config, [{"component": "div", "props": {"class": "pa-3"}, "content": items}]
