"""
参考 CC Switch 管理直连、代理路由和聚合模型的独立状态与客户端投影。

代理模式只替换宿主连接字段，真实供应商配置保存在模板中。聚合模型使用永久 key，
成员移除或删除不回收身份，失效的聚合请求不会回退其他供应商。
"""

from copy import deepcopy

MODE_NAMES = {"direct": "直连", "route": "路由", "stack": "聚合"}
MODE_DATA_KEY = "switch_mode"


def _slug(value: str) -> str:
    """
    按 CC Switch 的规则生成最长二十四字符的 ASCII 身份前缀。
    """
    out = ""
    for char in str(value).lower():
        if char.isascii() and char.isalnum():
            out += char
        elif out and not out.endswith("-"):
            out += "-"
        if len(out) >= 24:
            break
    return out.strip("-")


class ModeMixin:
    """
    保存模式意图、直连与路由指针、永久聚合 key，并保护代理进入退出的一致性。
    """

    def _mode_state(self) -> dict:
        """
        惰性读取本插件状态，保留未识别字段，不把凭据状态放进公共页面。
        """
        with self._lock:
            if not hasattr(self, "_mode"):
                state = self.get_data(MODE_DATA_KEY)
                self._mode = deepcopy(state) if isinstance(state, dict) else {}
            for key, value in {"active": "direct", "view": "direct", "direct_id": "", "route_id": "",
                               "stack_members": [], "stack_keys": {}, "selected_model": "",
                               "revision": 0, "gateway": {}, "attached": False}.items():
                self._mode.setdefault(key, deepcopy(value))
            return self._mode

    def _save_mode(self) -> None:
        """
        将受同一把状态锁保护的模式资料保存到本插件数据命名空间。
        """
        with self._lock:
            self.save_data(MODE_DATA_KEY, self._mode_state())

    def _mode_active(self) -> str:
        """
        返回生效模式，未知模式按直连处理。
        """
        value = self._mode_state().get("active")
        return value if value in MODE_NAMES else "direct"

    def _mode_view(self) -> str:
        """
        返回正在浏览的模式页签，与生效模式分离。
        """
        value = self._mode_state().get("view")
        return value if value in MODE_NAMES else self._mode_active()

    def _mode_status(self) -> dict:
        """
        导出不包含网关凭据和直连快照的公共模式资料。
        """
        state = self._mode_state()
        public = {key: deepcopy(state.get(key)) for key in
                  ("active", "view", "direct_id", "route_id", "stack_members", "stack_keys",
                   "selected_model", "revision", "attached")}
        public["stack_default_id"] = state.get("route_id")
        public["gateway"] = {"base_url": (state.get("gateway") or {}).get("base_url", "")}
        return public

    def _mode_ready(self) -> bool:
        """
        代理就绪需同时具有正在监听的实例和已发布的连接投影。
        """
        return self._mode_active() == "direct" or bool(
            getattr(self, "_gateway", None) and self._mode_state().get("attached"))

    def gateway_mode_revision(self) -> int:
        """
        返回请求开始时需要保存的模式修订，用于拒绝旧模式的迟到结果。
        """
        return int(self._mode_state().get("revision") or 0)

    def _validate_mode(self, mode: str) -> str:
        if mode not in MODE_NAMES:
            raise ValueError("模式只能是直连、路由或聚合")
        return mode

    def _set_mode_view(self, mode: str) -> str:
        """
        只切换浏览页签，不启动代理或写入系统 LLM 设置。
        """
        self._validate_mode(mode)
        with self._lock:
            self._mode_state()["view"] = mode
            self._save_mode()
        return f"正在查看{MODE_NAMES[mode]}模式"

    def _raw_llm_config(self) -> dict:
        """
        读取宿主真实连接字段；代理模式下这些字段指向本机网关。
        """
        return {key: getattr(self._settings, key, None) for key in self._LLM_SETTING_KEYS
                if hasattr(self._settings, key)}

    def _direct_llm(self) -> dict:
        """
        退出代理重新读取直连供应商当前配置，无保存模板时使用进入前的连接快照。
        """
        state = self._mode_state()
        direct = self._find_by_id(self._load_profiles(), state.get("direct_id"))
        snapshot = deepcopy(state.get("direct_snapshot") or {})
        if direct:
            config = deepcopy(self._profile_llm(direct))
            if not config.get("LLM_API_KEY"):
                identity = {**snapshot, **config}
                compatible = all(self._identity_text(identity.get(field)) == self._identity_text(snapshot.get(field))
                                 for field in ("LLM_PROVIDER", "LLM_BASE_URL", "LLM_BASE_URL_PRESET"))
                if compatible:
                    config["LLM_API_KEY"] = snapshot.get("LLM_API_KEY")
                else:
                    config["LLM_API_KEY"] = ""
            return {**snapshot, **config}
        return snapshot

    def _route_candidate(self, pid: str) -> dict:
        """
        验证真实 API Key 配置，避免使用网关占位凭据或递归指向本机网关。
        """
        profile = self._find_by_id(self._load_profiles(), pid)
        if not profile:
            raise ValueError("路由供应商配置不存在")
        llm = self._overlay_global(self._effective_llm(self._profile_llm(profile)))
        llm = dict(llm)
        direct = self._direct_llm()
        if not llm.get("LLM_API_KEY") and self._profile_key_compatible(llm, direct):
            llm["LLM_API_KEY"] = direct.get("LLM_API_KEY")
        if str(llm.get("LLM_PROVIDER") or "") not in {"openai", "deepseek", "anthropic"}:
            raise ValueError("本地代理目前支持 OpenAI 兼容、DeepSeek 与 Anthropic API Key 配置")
        if not llm.get("LLM_MODEL") or not llm.get("LLM_API_KEY"):
            raise ValueError("路由配置需要模型和所属端点的 API Key")
        gateway = self._mode_state().get("gateway") or {}
        if gateway.get("api_key") and llm.get("LLM_API_KEY") == gateway.get("api_key"):
            raise ValueError("网关凭据不能作为上游 API Key")
        base = str(llm.get("LLM_BASE_URL") or "").rstrip("/")
        if base and base == str(gateway.get("base_url") or "").rstrip("/"):
            raise ValueError("不能将代理上游配置指向自身")
        return {"id": profile["id"], "name": profile.get("name") or profile["id"], "llm": llm}

    def _real_llm_config(self) -> dict:
        """
        探活和页面状态使用真实路由配置，避免把网关占位配置当供应商。
        """
        if self._mode_active() == "direct":
            return self._raw_llm_config()
        return self._route_candidate(self._mode_state().get("route_id"))["llm"]

    def _allocate_stack_key(self, profile: dict) -> str:
        """
        复用或永久分配 key，不回收已移除或删除供应商占据的 key。
        """
        keys = self._mode_state()["stack_keys"]
        for key, pid in keys.items():
            if pid == profile["id"]:
                return key
        base = _slug(profile.get("name") or "") or _slug(profile["id"]) or "provider"
        key, suffix = base, 2
        while key in keys:
            key = f"{base}-{suffix}"
            suffix += 1
        keys[key] = profile["id"]
        return key

    def _gateway_catalog(self) -> list[dict]:
        """
        发布聚合成员模型的永久 ID；不含凭据，默认供应商同时保留普通模型入口。
        """
        state = self._mode_state()
        catalog = []
        for pid in state["stack_members"]:
            profile = self._find_by_id(self._load_profiles(), pid)
            if not profile:
                continue
            llm = self._profile_llm(profile)
            model = str(llm.get("LLM_MODEL") or "")
            if not model:
                continue
            key = next((key for key, owner in state["stack_keys"].items() if owner == pid), "")
            if not key:
                continue
            catalog.append({"id": f"ccs-{key}/{model}", "name": f"{profile.get('name') or pid} / {model}",
                            "provider_id": pid, "provider_name": profile.get("name") or pid, "model": model})
        return catalog

    def gateway_models(self) -> list[dict]:
        """
        为 OpenAI 模型目录提供当前路由模型及聚合成员别名。
        """
        if self._mode_active() == "direct":
            return []
        candidate = self._route_candidate(self._mode_state().get("route_id"))
        models = [{"id": candidate["llm"]["LLM_MODEL"], "object": "model", "owned_by": candidate["name"]}]
        if self._mode_active() == "stack":
            models.extend({"id": item["id"], "object": "model", "owned_by": item["provider_name"]}
                          for item in self._gateway_catalog())
        return models

    def gateway_candidates(self, model: str) -> list[dict]:
        """
        路由按队列选择；聚合只解析固定成员，任何保留前缀错误都拒绝回退。
        """
        state = self._mode_state()
        active = self._mode_active()
        if active == "direct" or not state.get("attached"):
            raise ValueError("本地代理模式未启用")
        if str(model).startswith("ccs-"):
            if active != "stack" or "/" not in model:
                raise ValueError("该聚合模型在当前模式不可用")
            key, upstream_model = model[4:].split("/", 1)
            pid = state["stack_keys"].get(key)
            if not pid or not upstream_model or pid not in state["stack_members"]:
                raise ValueError("聚合模型所属供应商已移除或模型标识无效")
            candidate = self._route_candidate(pid)
            candidate["llm"]["LLM_MODEL"] = upstream_model
            return [candidate]
        if active == "stack" or not self._auto_failover:
            return [self._route_candidate(state.get("route_id"))]
        candidates = []
        for profile in self._load_profiles():
            if not self._in_failover_queue(profile):
                continue
            try:
                candidate = self._route_candidate(profile["id"])
            except ValueError:
                continue
            if self._circuit_for(candidate["llm"]).is_available():
                candidates.append(candidate)
        if not candidates:
            raise ValueError("自动切换队列没有可用供应商")
        return candidates[:self._FAILOVER_MAX_TRY]

    def gateway_permit(self, candidate: dict) -> bool:
        """
        实际上游请求前取得熔断许可，聚合和未开启故障转移时不使用熔断器。
        """
        with self._lock:
            if self._mode_active() != "route" or not self._auto_failover:
                return True
            circuit = self._circuit_for(candidate["llm"])
            permit = circuit.allow_request()
            candidate["_gateway_circuit"] = circuit
            candidate["_gateway_halfopen"] = permit.used_half_open_permit
            return permit.allowed

    def gateway_record(self, candidate: dict, success: bool | None) -> None:
        """
        只在同一模式修订中更新路由健康，聚合请求不改变路由或熔断状态。
        """
        with self._lock:
            circuit = candidate.pop("_gateway_circuit", None)
            halfopen = candidate.pop("_gateway_halfopen", False)
            current = candidate.get("_gateway_revision", self.gateway_mode_revision()) == self.gateway_mode_revision()
            if circuit:
                if success is None or not current:
                    circuit.release_permit_neutral(halfopen)
                else:
                    (circuit.record_success if success else circuit.record_failure)(halfopen)
            if not current:
                return
            if self._mode_active() == "route" and success is True:
                self._mode_state()["route_id"] = candidate["id"]
                self._save_mode()
            if success is not None:
                self._add_log(f"{'调用成功' if success else '调用失败'}：{candidate.get('name') or candidate['id']}")
                self._save_runtime()

    def _select_route(self, pid: str) -> str:
        """
        改变代理上游指针，不改写宿主连接契约和直连指针。
        """
        with self._apply_lock, self._lock:
            candidate = self._route_candidate(pid)
            state = self._mode_state()
            state["route_id"] = pid
            state["revision"] += 1
            if self._mode_active() == "stack" and pid not in state["stack_members"]:
                state["stack_members"].append(pid)
                self._allocate_stack_key(self._find_by_id(self._load_profiles(), pid))
            self._save_mode()
        return f"已选择路由供应商 [{candidate['name']}]"

    def _toggle_stack(self, pid: str) -> str:
        """
        保存聚合成员，不允许直接移除默认供应商；永久 key 始终保留。
        """
        with self._apply_lock, self._lock:
            self._route_candidate(pid)
            state = self._mode_state()
            if pid == state.get("route_id") and pid in state["stack_members"]:
                raise ValueError("请先更换默认供应商，再移出当前默认供应商")
            if pid in state["stack_members"]:
                selected = state.get("selected_model") or ""
                key = next((key for key, owner in state["stack_keys"].items() if owner == pid), "")
                if self._mode_active() == "stack" and key and selected.startswith(f"ccs-{key}/"):
                    raise ValueError("请先选择其他聚合模型，再移出当前模型所属供应商")
                state["stack_members"].remove(pid)
                message = "已移出聚合"
            else:
                state["stack_members"].append(pid)
                self._allocate_stack_key(self._find_by_id(self._load_profiles(), pid))
                message = "已加入聚合"
            state["revision"] += 1
            self._save_mode()
        return message

    def _select_stack_model(self, model: str) -> str:
        """
        为内置智能助手选择聚合模型别名，上游地址与凭据仍由代理解析。
        """
        with self._apply_lock, self._lock:
            if self._mode_active() != "stack":
                raise ValueError("请先启用聚合模式")
            self.gateway_candidates(model)
            if model not in {item["id"] for item in self._gateway_catalog()}:
                raise ValueError("请选择聚合目录中已发布的模型")
            self._update_settings_checked({"LLM_MODEL": model}, "聚合模型选择")
            self._mode_state()["selected_model"] = model
            self._mode_state()["revision"] += 1
            self._save_mode()
        return "已选择聚合模型"

    def _new_gateway(self):
        """
        在首次进入代理时建立监听实例，测试可替换工厂而不启动服务。
        """
        factory = getattr(self, "_gateway_factory", None)
        if factory:
            return factory()
        from .gateway import LocalGateway
        return LocalGateway(self)

    def _proxy_projection(self, gateway: dict, mode: str) -> dict:
        """
        只投影本机连接契约，真实上游协议和凭据不会写进代理客户端配置。
        """
        state = self._mode_state()
        model = state.get("selected_model") if mode == "stack" else "mp-switch-route"
        if mode == "stack" and model and model not in {item["id"] for item in self._gateway_catalog()}:
            state["selected_model"] = ""
            model = "mp-switch-route"
        # 路由模式可能在固定客户端契约下切到任意支持的配置，按最小能力投影。
        ids = state.get("stack_members", []) if mode == "stack" else [p["id"] for p in self._load_profiles()]
        members = []
        for pid in ids:
            try:
                members.append(self._route_candidate(pid)["llm"])
            except ValueError:
                continue
        limits = [max(1, self._to_int(llm.get("LLM_MAX_CONTEXT_TOKENS"), 128)) for llm in members] or [128]
        return {"LLM_PROVIDER": "openai", "LLM_MODEL": model or "mp-switch-route",
                "LLM_API_KEY": gateway["api_key"], "LLM_BASE_URL": gateway["base_url"],
                "LLM_BASE_URL_PRESET": "", "LLM_USER_AGENT": "", "LLM_USE_PROXY": False,
                "LLM_THINKING_LEVEL": "off", "LLM_API_PROTOCOL": "chat_completions",
                "LLM_WEB_SEARCH_MODE": "local", "LLM_MAX_CONTEXT_TOKENS": min(limits),
                "LLM_SUPPORT_IMAGE_INPUT": bool(members) and all(bool(llm.get("LLM_SUPPORT_IMAGE_INPUT")) for llm in members),
                "LLM_SUPPORT_AUDIO_INPUT": False, "LLM_SUPPORT_AUDIO_OUTPUT": False}

    def _activate_mode(self, mode: str) -> str:
        """
        监听就绪和设置写入成功后才发布模式，失败回滚；退出恢复独立直连配置。
        """
        self._validate_mode(mode)
        gateway_to_stop = []
        try:
            with self._apply_lock, self._lock:
                before = deepcopy(self._mode_state())
                old = self._mode_active()
                if mode == "direct":
                    if old != "direct":
                        self._restore_direct_owned()
                        if getattr(self, "_gateway", None):
                            gateway_to_stop.append(self._gateway)
                            self._gateway = None
                    self._mode_state().update({"active": mode, "view": mode, "attached": False, "explicit_mode": True})
                    self._mode_state()["revision"] += 1
                    self._save_mode()
                    return "已启用直连模式"
                if not self._enabled:
                    raise ValueError("请先启用 MP Switch，再启用本地代理模式")
                state = self._mode_state()
                if old == "direct":
                    raw = self._raw_llm_config()
                    state["direct_snapshot"] = deepcopy(raw)
                    matched = next((p for p in self._load_profiles()
                                    if self._same_endpoint(self._profile_llm(p), raw)), None)
                    state["direct_id"] = matched.get("id") if matched else ""
                    if not state.get("route_id"):
                        state["route_id"] = (matched or next(iter(self._load_profiles()), {})).get("id", "")
                self._route_candidate(state.get("route_id"))
                if mode == "stack":
                    pid = state["route_id"]
                    if pid not in state["stack_members"]:
                        state["stack_members"].append(pid)
                        self._allocate_stack_key(self._find_by_id(self._load_profiles(), pid))
                started = False
                previous_settings = self._raw_llm_config()
                try:
                    if not getattr(self, "_gateway", None):
                        self._gateway = self._new_gateway()
                        gateway = self._gateway.start()
                        started = True
                    else:
                        gateway = state["gateway"]
                    projection = self._proxy_projection(gateway, mode)
                    state["pending"] = {"mode": mode, "projection": projection}
                    state["gateway"] = gateway
                    self._save_mode()
                    self._update_settings_checked(projection, "代理模式连接投影")
                    state.update({"active": mode, "view": mode, "attached": True, "gateway": gateway, "explicit_mode": True})
                    state["revision"] += 1
                    state.pop("pending", None)
                    self._save_mode()
                except Exception:
                    # 连接投影可能已经成功发布，但最后的状态保存失败。先恢复连接再关监听。
                    if self._raw_llm_config() != previous_settings:
                        self._update_settings_checked(previous_settings, "代理模式失败恢复")
                    if started and getattr(self, "_gateway", None):
                        gateway_to_stop.append(self._gateway)
                        self._gateway = None
                    self._mode = before
                    self._save_mode()
                    raise
                return f"已启用{MODE_NAMES[mode]}模式"

        finally:
            # 关闭监听会等待在途请求结算，必须在插件状态锁之外执行。
            for gateway in gateway_to_stop:
                gateway.stop()

    def _restore_direct_owned(self) -> None:
        """
        仅恢复仍属于本插件网关的设置，手工替换后的连接不会被停用操作覆盖。
        """
        gateway = self._mode_state().get("gateway") or {}
        raw = self._raw_llm_config()
        if raw.get("LLM_BASE_URL") == gateway.get("base_url") and raw.get("LLM_API_KEY") == gateway.get("api_key"):
            direct = self._direct_llm()
            if not direct:
                raise RuntimeError("无法恢复直连配置，暂不关闭代理")
            if self._provider_needs_api_key(direct.get("LLM_PROVIDER")) and not direct.get("LLM_API_KEY"):
                raise RuntimeError("直连供应商地址已变更但未提供所属密钥，请先修正配置")
            self._update_settings_checked(direct, "恢复直连配置")

    def _stop_mode_gateway(self) -> None:
        """
        先恢复连接并撤销旧请求的修订，再在锁外关闭网关；保留模式意图供重载重建。
        """
        gateway = None
        try:
            with self._apply_lock, self._lock:
                if not getattr(self, "_gateway", None):
                    return
                self._restore_direct_owned()
                gateway = self._gateway
                self._gateway = None
                self._mode_state()["attached"] = False
                self._mode_state()["revision"] += 1
                self._save_mode()
        finally:
            if gateway:
                gateway.stop()

    def _init_modes(self) -> None:
        """
        恢复保存的模式意图，崩溃遗留代理连接先回到直连再重建监听。
        """
        with self._apply_lock, self._lock:
            self._mode = deepcopy(self.get_data(MODE_DATA_KEY) or {})
            state = self._mode_state()
            wanted = self._mode_active()
            if state.get("pending"):
                state["gateway"] = state.get("gateway") or {}
                self._restore_direct_owned()
                state.pop("pending", None)
            if wanted != "direct":
                self._restore_direct_owned()
                state["attached"] = False
                self._save_mode()
        if wanted != "direct" and self._enabled:
            self._activate_mode(wanted)

    def _toggle_route_failover(self) -> str:
        """
        故障转移为路由独立开关，聚合期间不允许启用。
        """
        with self._apply_lock, self._lock:
            if self._mode_active() != "route":
                raise ValueError("请先启用路由模式，聚合模式不进行故障转移")
            enabling = not self._auto_failover
            if enabling:
                queue = [p for p in self._load_profiles() if self._in_failover_queue(p)]
                if not queue:
                    pid = self._mode_state()["route_id"]
                    self._toggle_queue(pid)
                    queue = [self._find_by_id(self._load_profiles(), pid)]
                self._select_route(queue[0]["id"])
            self._auto_failover = enabling
            self._save_persistent_config()
        return "已开启故障转移" if enabling else "已关闭故障转移"

    def _mode_apis(self) -> list[dict]:
        """
        声明模式浏览、激活和聚合模型选择的宿主接口。
        """
        return [{"path": f"/{name}", "endpoint": getattr(self, f"api_{name}"),
                 "methods": ["GET", "POST"], "summary": title}
                for name, title in (("mode_view", "查看运行模式"), ("mode_activate", "启用运行模式"),
                                    ("route_select", "选择代理路由"), ("stack_toggle", "切换聚合成员"),
                                    ("stack_default", "选择聚合默认供应商"), ("stack_model", "选择聚合模型"),
                                    ("toggle_failover", "切换路由故障转移"))]

    def api_mode_view(self, mode: str, apikey: str):
        """
        验证权限后仅切换浏览页签。
        """
        return self._api_run(apikey, self._set_mode_view, mode)

    def api_mode_activate(self, mode: str, apikey: str):
        """
        验证权限后变更生效模式及客户端连接契约。
        """
        return self._api_run(apikey, self._activate_mode, mode)

    def api_route_select(self, pid: str, apikey: str):
        """
        验证权限后选择当前或待启用的代理路由目标。
        """
        return self._api_run(apikey, self._select_route, pid)

    def api_stack_toggle(self, pid: str, apikey: str):
        """
        验证权限后将供应商加入或移出聚合。
        """
        return self._api_run(apikey, self._toggle_stack, pid)

    def api_stack_default(self, pid: str, apikey: str):
        """
        验证权限后选择聚合默认供应商，不退出代理模式。
        """
        return self._api_run(apikey, self._select_route, pid)

    def api_stack_model(self, model: str, apikey: str):
        """
        验证权限后为智能助手选择已发布的聚合模型。
        """
        return self._api_run(apikey, self._select_stack_model, model)

    def api_toggle_failover(self, apikey: str):
        """
        验证权限后切换路由的故障转移开关。
        """
        return self._api_run(apikey, self._toggle_route_failover)
