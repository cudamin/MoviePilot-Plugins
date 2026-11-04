"""
端点模型发现、白名单匹配与模板导入。
"""

import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

from app.sdk.logging import logger


class DiscoveryMixin:
    """
    端点模型发现、白名单匹配与模板导入。
    """

    def _load_discovery(self) -> Dict[str, Any]:
        with self._lock:
            data = self.get_data(self.DATA_KEY_DISCOVERY)
            if isinstance(data, dict):
                if "api_key" in data:
                    data = dict(data)
                    data.pop("api_key", None)
                    self.save_data(self.DATA_KEY_DISCOVERY, data)
                return data
            return {}

    def _save_discovery(self, data: Dict[str, Any]):
        with self._lock:
            self.save_data(self.DATA_KEY_DISCOVERY, data)

    @staticmethod
    def _host_of(url: str) -> str:
        """
        从 base_url 中提取主机名，用于模板命名。
        """
        text = str(url or "").strip()
        if not text:
            return "-"
        try:
            from urllib.parse import urlparse

            parsed = urlparse(text if "//" in text else f"//{text}")
            return parsed.hostname or text
        except Exception:
            return text

    def _match_filter(self, model_id: str) -> bool:
        """
        按用户设置的正则过滤模型。
        """
        if not self._discover_filter:
            return True
        try:
            import re

            return bool(re.search(self._discover_filter, str(model_id or ""), re.IGNORECASE))
        except Exception:
            return True

    def _list_models(self, provider: str, base_url: str, api_key: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
        """
        列出指定协议在该端点下可用的模型。
        """
        from app.agent.llm.helper import LLMHelper

        try:
            models = self._run_coro(lambda: LLMHelper().get_models(
                provider=provider,
                api_key=api_key,
                base_url=base_url,
                use_proxy=bool(getattr(self._settings, "LLM_USE_PROXY", False)),
                force_refresh=True,
            ))
        except Exception as err:  # noqa: BLE001
            message = str(err) or err.__class__.__name__
            if api_key and len(api_key) > 6:
                message = self._sanitize(message)
            return False, message[:180], []

        records = []
        for item in models or []:
            model_id = str((item or {}).get("id") or "").strip()
            if not model_id:
                continue
            records.append({
                "id": model_id,
                "name": (item or {}).get("name") or model_id,
                "context_tokens_k": (item or {}).get("context_tokens_k"),
                "supports_reasoning": bool((item or {}).get("supports_reasoning")),
                "supports_image_input": bool((item or {}).get("supports_image_input")),
                "supports_audio_input": bool((item or {}).get("supports_audio_input")),
            })
        if not records:
            return False, "未返回任何模型", []
        return True, f"发现 {len(records)} 个模型", records[:200]

    def _resolve_discovery_key(self, base_url: str, api_key: str = "") -> str:
        """
        解析探测或导入使用的密钥，只在密钥所属端点一致时自动复用。
        """
        key = str(api_key or "").strip()
        if key:
            return key
        endpoint = self._identity_text(base_url)
        key = str(getattr(self, "_discovery_api_key_runtime", "") or "").strip()
        if key and endpoint and endpoint == self._identity_text(
                getattr(self, "_discovery_base_url_runtime", "")):
            return key
        key = str(self._discover_api_key or "").strip()
        configured_endpoint = self._identity_text(self._discover_base_url) or self._identity_text(
            getattr(self._settings, "LLM_BASE_URL", ""))
        if key and endpoint and endpoint == configured_endpoint:
            return key
        current = self._current_llm_config()
        if base_url and self._identity_text(base_url) == self._identity_text(
                current.get("LLM_BASE_URL")):
            return str(current.get("LLM_API_KEY") or "").strip()
        return ""

    def _start_discovery(self, base_url: str = "", api_key: str = "", pid: str = "") -> str:
        """
        启动后台探测任务，避免阻塞插件加载或页面请求。
        """
        if pid:
            profile = self._find_by_id(self._load_profiles(), pid)
            if not profile:
                return "探测失败：模板不存在"
            llm = self._effective_llm(self._profile_llm(profile))
            profile_base_url = str(llm.get("LLM_BASE_URL") or "").strip()
            if base_url and self._identity_text(base_url) != self._identity_text(profile_base_url):
                # 不允许“拿模板 A 的 Key 去探测任意 B 地址”；需要换地址就显式填 Key。
                return "探测失败：为模板指定不同地址时不会复用其 API Key，请显式填写 API Key"
            base_url = base_url or profile_base_url
            api_key = api_key or str(llm.get("LLM_API_KEY") or "")
        base_url = (base_url or self._discover_base_url
                    or str(getattr(self._settings, "LLM_BASE_URL", "") or "")).strip()
        api_key = self._resolve_discovery_key(base_url, api_key)
        if not base_url:
            return "探测失败：请先填写 LLM 基础 URL"
        if "://" in base_url and not base_url.lower().startswith(("http://", "https://")):
            return "探测失败：仅支持 http/https 端点"
        if not api_key:
            return "探测失败：请先填写 API Key"

        with self._lock:
            discovery = self._load_discovery()
            if discovery.get("status") == "running":
                return "探测正在进行中，请稍后刷新查看结果"
            self._discovery_api_key_runtime = api_key
            self._discovery_base_url_runtime = base_url
            generation = self._generation
            stop_event = self._stop_event
            self._save_discovery({
                "status": "running",
                "base_url": base_url,
                "started_at": self._now(),
                "providers": {},
            })
        threading.Thread(
            target=self._discovery_worker,
            args=(base_url, api_key, generation, stop_event),
            daemon=True,
        ).start()
        return f"已开始探测 {self._host_of(base_url)} 的可用模型，稍后在数据详情页查看结果"

    def _discovery_worker(
            self,
            base_url: str,
            api_key: str,
            generation: int,
            stop_event: threading.Event,
    ):
        """
        依次用各协议探测端点模型目录，可选自动导入模板。
        """
        result: Dict[str, Any] = {
            "status": "running",
            "base_url": base_url,
            "started_at": self._now(),
            "providers": {},
        }
        try:
            for provider in self._discover_providers:
                if self._is_cancelled(generation, stop_event):
                    return
                ok, message, models = self._list_models(provider, base_url, api_key)
                if self._is_cancelled(generation, stop_event):
                    return
                result["providers"][provider] = {
                    "label": self._DISCOVER_PROVIDERS.get(provider, provider),
                    "ok": ok,
                    "message": message,
                    "count": len(models),
                    "models": models,
                }
                logger.info(f"{self.plugin_name}：探测 [{provider}] {self._host_of(base_url)} - {message}")
                self._save_discovery({**result, "status": "running"})
            result["status"] = "done"
            result["finished_at"] = self._now()
            if self._is_cancelled(generation, stop_event):
                return
            self._save_discovery(result)

            if self._discover_auto_import:
                if self._is_cancelled(generation, stop_event):
                    return
                import_message = self._import_discovered(api_key=api_key)
                result["import_message"] = import_message
                result["imported_at"] = self._now()
                if not self._is_cancelled(generation, stop_event):
                    self._save_discovery(result)
            if not self._is_cancelled(generation, stop_event):
                summary = "、".join(
                    f"{(info or {}).get('label') or pid}：{(info or {}).get('count') or 0} 个"
                    for pid, info in (result.get("providers") or {}).items()
                    if (info or {}).get("ok")
                ) or "没有协议探测成功"
                self._notify_safe(
                    "智能助手模型探测完成",
                    f"端点：{self._host_of(base_url)}\n{summary}\n"
                    f"{result.get('import_message') or '未自动导入模板'}",
                )
        except Exception as err:  # noqa: BLE001
            result["status"] = "error"
            result["message"] = self._sanitize(err)[:180]
            result["finished_at"] = self._now()
            if not self._is_cancelled(generation, stop_event):
                self._save_discovery(result)
            logger.error(f"{self.plugin_name}：模型探测失败 - {err}")
        finally:
            if generation == getattr(self, "_generation", generation) \
                    and self._is_cancelled(generation, stop_event):
                current = self._load_discovery()
                if current.get("status") == "running":
                    current["status"] = "error"
                    current["message"] = "任务已停止"
                    current["finished_at"] = self._now()
                    self._save_discovery(current)

    def _model_snapshot(self, provider: str, base_url: str, api_key: str,
                        model: Dict[str, Any]) -> Dict[str, Any]:
        """
        基于探测到的模型元数据构造模板快照。
        """
        llm: Dict[str, Any] = {
            "LLM_PROVIDER": provider,
            "LLM_MODEL": str(model.get("id")),
            "LLM_BASE_URL": base_url,
            "LLM_BASE_URL_PRESET": None,
            "LLM_USER_AGENT": None,
            "LLM_USE_PROXY": bool(getattr(self._settings, "LLM_USE_PROXY", False)),
            # 未启用全局参数时，继承当前系统设置，避免新建模板被强制回到最保守值
            "LLM_THINKING_LEVEL": str(getattr(self._settings, "LLM_THINKING_LEVEL", "off") or "off"),
            "LLM_API_PROTOCOL": str(getattr(self._settings, "LLM_API_PROTOCOL", "auto") or "auto"),
            "LLM_WEB_SEARCH_MODE": str(getattr(self._settings, "LLM_WEB_SEARCH_MODE", "local") or "local"),
            "LLM_TEMPERATURE": float(getattr(self._settings, "LLM_TEMPERATURE", 0.3) or 0.3),
            "LLM_SUPPORT_IMAGE_INPUT": bool(model.get("supports_image_input")),
            "LLM_SUPPORT_AUDIO_INPUT": bool(model.get("supports_audio_input")),
            "LLM_SUPPORT_AUDIO_OUTPUT": False,
        }
        if self._include_credentials and api_key:
            llm["LLM_API_KEY"] = api_key
        context_k = self._to_int(model.get("context_tokens_k"), 0)
        llm["LLM_MAX_CONTEXT_TOKENS"] = context_k or self._to_int(
            getattr(self._settings, "LLM_MAX_CONTEXT_TOKENS", 256), 256)
        llm = self._overlay_global(llm)
        return {"snapshot": {"llm": llm}}

    def _profile_name_for(self, profiles: List[Dict[str, Any]], provider: str, base_url: str,
                          model_id: str) -> str:
        """
        生成带协议与站点信息且不冲突的模板名称。
        """
        label = self._DISCOVER_PROVIDERS.get(provider, provider)
        base_name = f"{model_id} · {label} · {self._host_of(base_url)}"
        if not self._find_by_name(profiles, base_name):
            return base_name
        index = 2
        while self._find_by_name(profiles, f"{base_name} #{index}"):
            index += 1
        return f"{base_name} #{index}"

    @staticmethod
    def _parse_list(text: Any) -> List[str]:
        """
        把逗号/换行分隔的文本解析为去重列表。
        """
        if isinstance(text, (list, tuple)):
            items = [str(item).strip() for item in text]
        else:
            raw = str(text or "")
            for sep in ("\n", "\r", "、", "；", ";", "|"):
                raw = raw.replace(sep, ",")
            items = [part.strip() for part in raw.split(",")]
        result = []
        for item in items:
            if item and item not in result:
                result.append(item)
        return result

    def _has_whitelist(self) -> bool:
        """
        是否配置了模型白名单。
        """
        return bool(self._whitelist)

    def _preferred_provider(self, model_id: str, available: List[str]) -> Optional[str]:
        """
        按模型名推断最合适的协议，避免同一模型在多个协议下重复建模板。
        """
        if not available:
            return None
        lower = str(model_id or "").lower()
        for provider, keywords in self._MODEL_PROVIDER_HINTS:
            if provider in available and any(key in lower for key in keywords):
                return provider
        for provider in self._PROVIDER_FALLBACK_ORDER:
            if provider in available:
                return provider
        return available[0]

    def _collect_discovered(self, discovery: Dict[str, Any],
                            provider_filter: str = "") -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], List[str]]:
        """
        汇总探测结果为 模型 -> {协议: 元数据}。
        """
        model_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
        order: List[str] = []
        for provider, info in (discovery.get("providers") or {}).items():
            if provider_filter and provider != provider_filter:
                continue
            if not (info or {}).get("ok"):
                continue
            for model in (info.get("models") or []):
                model_id = str(model.get("id") or "")
                if not model_id:
                    continue
                if model_id not in model_map:
                    model_map[model_id] = {}
                    order.append(model_id)
                model_map[model_id][provider] = model
        return model_map, order

    def whitelist_status(self, discovery: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        返回白名单在探测结果中的命中与未命中情况，供页面展示。
        """
        model_map, _order = self._collect_discovered(discovery)
        known = {model_id.lower() for model_id in model_map}
        names = self._whitelist
        return {
            "hit": [name for name in names if name.lower() in known],
            "miss": [name for name in names if name.lower() not in known],
        }

    def _import_discovered(self, provider_filter: str = "", api_key: str = "") -> str:
        """
        把探测结果中的模型批量建成模板。白名单优先，其次智能匹配。
        """
        discovery = self._load_discovery()
        base_url = discovery.get("base_url") or self._discover_base_url
        api_key = self._resolve_discovery_key(base_url, api_key)
        if not (discovery.get("providers") or {}):
            return "导入失败：没有探测结果"

        model_map, order = self._collect_discovered(discovery, provider_filter)
        if not model_map:
            return "导入完成：没有可用的探测结果"

        # 计划导入项：(协议, 模型ID, 模型元数据)
        plan: List[Tuple[str, str, Dict[str, Any]]] = []
        missed: List[str] = []
        use_whitelist = self._has_whitelist()
        if use_whitelist:
            lower_map = {mid.lower(): mid for mid in model_map}
            for name in self._whitelist:
                model_id = lower_map.get(name.lower())
                if not model_id:
                    missed.append(name)
                    continue
                # 同名模型可能在多个协议下都存在，按模型名智能匹配归属协议
                record = model_map[model_id]
                provider = self._preferred_provider(model_id, list(record))
                if not provider:
                    missed.append(name)
                    continue
                plan.append((provider, model_id, record[provider]))
        else:
            smart = self._discover_smart_match and not provider_filter
            for model_id in order:
                if not self._match_filter(model_id):
                    continue
                record = model_map[model_id]
                if smart:
                    provider = self._preferred_provider(model_id, list(record))
                    if provider:
                        plan.append((provider, model_id, record[provider]))
                else:
                    for provider, model in record.items():
                        plan.append((provider, model_id, model))

        added = 0
        skipped = 0
        per_provider: Dict[str, int] = {}
        needs_key = any(self._provider_needs_api_key(provider) for provider, _model_id, _model in plan)
        if needs_key and not api_key:
            return "导入失败：探测密钥已失效（插件重载后需重新探测或填写 API Key），已拒绝创建无密钥模板"
        with self._lock:
            profiles = self._load_profiles()
            for provider, model_id, model in plan:
                # 白名单是用户显式指定的清单，不再受每协议上限限制
                if not use_whitelist and self._discover_limit \
                        and per_provider.get(provider, 0) >= self._discover_limit:
                    continue
                candidate = {
                    "LLM_PROVIDER": provider,
                    "LLM_MODEL": model_id,
                    "LLM_BASE_URL": base_url,
                    "LLM_API_PROTOCOL": self._g_api_protocol if self._apply_global
                    else str(getattr(self._settings, "LLM_API_PROTOCOL", "auto") or "auto"),
                    "LLM_API_KEY": api_key,
                }
                duplicated = next(
                    (p for p in profiles if self._same_endpoint(
                        self._effective_llm(self._profile_llm(p)),
                        self._effective_llm(candidate))),
                    None,
                )
                if duplicated:
                    skipped += 1
                    continue
                profiles.append({
                    "id": uuid.uuid4().hex,
                    "name": self._profile_name_for(profiles, provider, base_url, model_id),
                    "created_at": self._now(),
                    "updated_at": self._now(),
                    "health": {"status": "unknown", "fail_count": 0},
                    "source": "discover",
                    **self._model_snapshot(provider, base_url, api_key, model),
                })
                per_provider[provider] = per_provider.get(provider, 0) + 1
                added += 1
            if added:
                self._save_profiles(profiles)

        text = f"已导入 {added} 个模板，跳过 {skipped} 个已存在"
        if use_whitelist:
            text += "（按白名单）"
            if missed:
                text += f"，白名单未探测到：{'、'.join(missed[:6])}"
        if added:
            with self._lock:
                self._add_log(text)
                self._save_runtime()
        logger.info(f"{self.plugin_name}：{text}")
        return text

    def _normalize_names(self, scope: str = "discover") -> str:
        """
        把模板名统一改成「模型 · 协议 · 站点」格式。
        """
        renamed = 0
        with self._lock:
            profiles = self._load_profiles()
            for profile in profiles:
                if scope != "all" and profile.get("source") != "discover":
                    continue
                llm = self._profile_llm(profile)
                provider = llm.get("LLM_PROVIDER")
                model_id = llm.get("LLM_MODEL")
                if not provider or not model_id:
                    continue
                others = [item for item in profiles if item is not profile]
                new_name = self._profile_name_for(others, provider, llm.get("LLM_BASE_URL") or "", model_id)
                if new_name != profile.get("name"):
                    profile["name"] = new_name
                    profile["updated_at"] = self._now()
                    renamed += 1
            if renamed:
                self._save_profiles(profiles)
        text = f"已规范 {renamed} 个模板名称"
        logger.info(f"{self.plugin_name}：{text}（范围 {scope}）")
        return text

    def _prune_offlist(self) -> str:
        """
        删除探测生成但不在白名单内的模板。
        """
        if not self._has_whitelist():
            return "未配置白名单，未做清理"
        allow = {name.lower() for name in self._whitelist}
        removed: List[str] = []
        with self._lock:
            profiles = self._load_profiles()
            kept = []
            for profile in profiles:
                if profile.get("source") != "discover":
                    kept.append(profile)
                    continue
                llm = self._profile_llm(profile)
                model_id = str(llm.get("LLM_MODEL") or "").lower()
                if model_id in allow:
                    kept.append(profile)
                    continue
                removed.append(profile.get("name") or model_id)
            if removed:
                self._save_profiles(kept)
        text = f"已清理 {len(removed)} 个名单外模板" if removed else "没有需要清理的名单外模板"
        if removed:
            self._add_log(text)
            self._save_runtime()
        logger.info(f"{self.plugin_name}：{text}")
        return text

    def _add_model_profile(self, provider: str, model_id: str) -> str:
        """
        把探测结果中的单个模型加为模板。
        """
        discovery = self._load_discovery()
        base_url = discovery.get("base_url") or self._discover_base_url
        api_key = self._resolve_discovery_key(base_url)
        if self._provider_needs_api_key(provider) and not api_key:
            return "添加失败：探测密钥已失效，请重新探测或填写 API Key"
        info = (discovery.get("providers") or {}).get(provider) or {}
        model = next((m for m in (info.get("models") or []) if str(m.get("id")) == model_id), None)
        if not model:
            return "添加失败：探测结果中没有该模型"
        candidate = {
            "LLM_PROVIDER": provider,
            "LLM_MODEL": model_id,
            "LLM_BASE_URL": base_url,
            "LLM_API_PROTOCOL": self._g_api_protocol if self._apply_global
            else str(getattr(self._settings, "LLM_API_PROTOCOL", "auto") or "auto"),
            "LLM_API_KEY": api_key,
        }
        with self._lock:
            profiles = self._load_profiles()
            duplicated = next(
                (p for p in profiles if self._same_endpoint(
                    self._effective_llm(self._profile_llm(p)),
                    self._effective_llm(candidate))),
                None,
            )
            if duplicated:
                return f"模板已存在：{duplicated.get('name')}"
            name = self._profile_name_for(profiles, provider, base_url, model_id)
            profiles.append({
                "id": uuid.uuid4().hex,
                "name": name,
                "created_at": self._now(),
                "updated_at": self._now(),
                "health": {"status": "unknown", "fail_count": 0},
                "source": "discover",
                **self._model_snapshot(provider, base_url, api_key, model),
            })
            self._save_profiles(profiles)
        logger.info(f"{self.plugin_name}：已添加模板 [{name}]")
        return f"已添加模板 [{name}]"
