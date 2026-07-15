from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType

from .core import (
    CacheEntry,
    SiteProfile,
    SubtitleDecision,
    evaluate_torrent_attributes,
    extract_torrent_id,
    normalize_torrent_payload,
    parse_profiles,
)


class Unit3dChineseSubFilter(_PluginBase):
    plugin_name = "UNIT3D 中文字幕过滤"
    plugin_desc = "在 MoviePilot 选择或下载 UNIT3D RSS 资源前，通过 API 检查 MediaInfo/BDInfo/外挂字幕。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "0.1.0"
    plugin_author = "Community"
    author_url = ""
    plugin_config_prefix = "unit3d_chinese_sub_filter_"
    plugin_order = 35
    auth_level = 1

    _enabled = False
    _only_rss = True
    _fail_closed = True
    _inspect_mediainfo = True
    _inspect_bdinfo = True
    _inspect_files = True
    _inspect_free_text = False
    _timeout = 8.0
    _verify_ssl = True
    _cache_ttl = 1440
    _max_workers = 4
    _profiles: List[SiteProfile] = []
    _profile_error = ""

    _cache: Dict[str, CacheEntry] = {}
    _cache_lock = threading.RLock()
    _stats_lock = threading.RLock()
    _stats = {"checked": 0, "allowed": 0, "blocked": 0, "errors": 0, "cache_hits": 0}

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._only_rss = bool(config.get("only_rss", True))
        self._fail_closed = bool(config.get("fail_closed", True))
        self._inspect_mediainfo = bool(config.get("inspect_mediainfo", True))
        self._inspect_bdinfo = bool(config.get("inspect_bdinfo", True))
        self._inspect_files = bool(config.get("inspect_files", True))
        self._inspect_free_text = bool(config.get("inspect_free_text", False))
        self._verify_ssl = bool(config.get("verify_ssl", True))
        self._timeout = self._to_float(config.get("timeout"), 8.0, minimum=1.0, maximum=60.0)
        self._cache_ttl = self._to_int(config.get("cache_ttl"), 1440, minimum=1, maximum=43200)
        self._max_workers = self._to_int(config.get("max_workers"), 4, minimum=1, maximum=12)
        self._profile_error = ""
        with self._cache_lock:
            self._cache.clear()
        try:
            self._profiles = parse_profiles(config.get("profiles_json") or "[]")
        except Exception as exc:
            self._profiles = []
            self._profile_error = str(exc)
            logger.error(f"[{self.plugin_name}] 站点配置解析失败：{exc}")

        if self._enabled and not self._profiles:
            logger.warning(f"[{self.plugin_name}] 插件已启用，但没有可用的 UNIT3D 站点配置")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        sample = json.dumps(
            [
                {
                    "name": "Aither",
                    "match": ["aither.cc", "Aither"],
                    "base_url": "https://aither.cc",
                    "api_token": "在这里填写 API Token",
                    "auth_mode": "query",
                    "token_param": "api_token",
                    "endpoint": "/api/torrents/{id}",
                },
                {
                    "name": "BLU",
                    "match": ["blutopia.cc", "BLU"],
                    "base_url": "https://blutopia.cc",
                    "api_token": "在这里填写 API Token",
                    "auth_mode": "query",
                    "token_param": "api_token",
                    "endpoint": "/api/torrents/{id}",
                },
            ],
            ensure_ascii=False,
            indent=2,
        )
        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "only_rss", "label": "仅检查 RSS 候选"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "fail_closed", "label": "API 失败时拦截"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "verify_ssl", "label": "验证 HTTPS 证书"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "inspect_mediainfo", "label": "检查 MediaInfo"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "inspect_bdinfo", "label": "检查 BDInfo"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "inspect_files", "label": "检查外挂字幕文件"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [{"component": "VSwitch", "props": {"model": "inspect_free_text", "label": "标题/描述兜底（可能误判）"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "timeout", "label": "单次 API 超时（秒）", "type": "number"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "cache_ttl", "label": "结果缓存（分钟）", "type": "number"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{"component": "VTextField", "props": {"model": "max_workers", "label": "并发检查数", "type": "number"}}],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "profiles_json",
                                            "label": "UNIT3D 站点配置（JSON）",
                                            "rows": 16,
                                            "hint": "match 用于匹配 MP 的站点名、详情链接或下载链接。API Token 只保存在 MoviePilot 插件配置中。",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ]
        defaults = {
            "enabled": False,
            "only_rss": True,
            "fail_closed": True,
            "verify_ssl": True,
            "inspect_mediainfo": True,
            "inspect_bdinfo": True,
            "inspect_files": True,
            "inspect_free_text": False,
            "timeout": 8,
            "cache_ttl": 1440,
            "max_workers": 4,
            "profiles_json": sample,
        }
        return form, defaults

    def get_page(self) -> List[dict]:
        with self._stats_lock:
            stats = dict(self._stats)
        profile_names = "、".join(profile.name for profile in self._profiles) or "未配置"
        status = (
            f"状态：{'已启用' if self._enabled else '未启用'}；站点：{profile_names}；"
            f"已检查 {stats['checked']}，放行 {stats['allowed']}，拦截 {stats['blocked']}，"
            f"API/解析错误 {stats['errors']}，缓存命中 {stats['cache_hits']}。"
        )
        page = [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": status}}]
        if self._profile_error:
            page.append(
                {
                    "component": "VAlert",
                    "props": {"type": "error", "variant": "tonal", "text": f"站点配置错误：{self._profile_error}"},
                }
            )
        return page

    def stop_service(self):
        with self._cache_lock:
            self._cache.clear()

    @eventmanager.register(ChainEventType.ResourceSelection, priority=35)
    def filter_resource_selection(self, event: Event):
        if not self._enabled or not self._profiles or not event:
            return
        data = event.event_data
        contexts = self._read(data, "updated_contexts") if self._read(data, "updated") else None
        if contexts is None:
            contexts = self._read(data, "contexts")
        if not contexts:
            return

        contexts = list(contexts)
        results: Dict[int, SubtitleDecision] = {}
        jobs = {}
        with ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="u3d-sub-filter") as pool:
            for index, context in enumerate(contexts):
                if not self._should_check_context(context):
                    continue
                jobs[pool.submit(self._check_context, context)] = index
            for future in as_completed(jobs):
                index = jobs[future]
                try:
                    results[index] = future.result()
                except Exception as exc:
                    results[index] = self._error_decision(f"检查异常：{exc}")

        if not results:
            return
        filtered = []
        removed = 0
        for index, context in enumerate(contexts):
            decision = results.get(index)
            if decision is None or decision.allowed:
                filtered.append(context)
            else:
                removed += 1
                title = self._context_value(context, "torrent_info", "title") or "未知资源"
                logger.info(f"[{self.plugin_name}] 已从候选中剔除：{title}；原因：{decision.reason}")
        if removed:
            self._write(data, "updated", True)
            self._write(data, "updated_contexts", filtered)
            self._write(data, "source", self.plugin_name)

    @eventmanager.register(ChainEventType.ResourceDownload, priority=35)
    def filter_resource_download(self, event: Event):
        if not self._enabled or not self._profiles or not event:
            return
        data = event.event_data
        if self._read(data, "cancel"):
            return
        context = self._read(data, "context")
        if not context or not self._should_check_context(context):
            return
        decision = self._check_context(context)
        if decision.allowed:
            return
        self._write(data, "cancel", True)
        self._write(data, "source", self.plugin_name)
        self._write(data, "reason", decision.reason)
        title = self._context_value(context, "torrent_info", "title") or "未知资源"
        logger.warning(f"[{self.plugin_name}] 已拦截下载：{title}；原因：{decision.reason}")

    def _check_context(self, context: Any) -> SubtitleDecision:
        torrent_info = self._read(context, "torrent_info")
        site_name = self._read(torrent_info, "site_name")
        page_url = self._read(torrent_info, "page_url")
        enclosure = self._read(torrent_info, "enclosure")
        title = self._read(torrent_info, "title")
        profile = self._find_profile(site_name, page_url, enclosure)
        if not profile:
            return SubtitleDecision(True, "未匹配到 UNIT3D 配置，保持原流程")
        torrent_id = extract_torrent_id(page_url, enclosure)
        if not torrent_id:
            return self._error_decision(f"{profile.name} 无法从详情/下载链接提取种子 ID")

        cache_key = f"{profile.name}:{torrent_id}"
        cached = self._get_cache(cache_key)
        if cached:
            self._inc("cache_hits")
            return cached

        self._inc("checked")
        try:
            payload = self._request_torrent(profile, torrent_id)
            attrs = normalize_torrent_payload(payload)
            if not attrs:
                decision = self._error_decision(f"{profile.name} API 返回中没有种子属性")
            else:
                decision = evaluate_torrent_attributes(
                    attrs,
                    inspect_mediainfo=self._inspect_mediainfo,
                    inspect_bdinfo=self._inspect_bdinfo,
                    inspect_files=self._inspect_files,
                    inspect_free_text=self._inspect_free_text,
                )
                if decision.allowed:
                    evidence = "；".join(decision.evidence[:3])
                    decision = SubtitleDecision(True, f"{profile.name} 检测到中文字幕：{evidence}", decision.evidence)
                else:
                    decision = SubtitleDecision(False, f"{profile.name} API 详情未检测到中文字幕")
        except Exception as exc:
            decision = self._error_decision(f"{profile.name} API 检查失败：{exc}")

        self._put_cache(cache_key, decision)
        self._inc("allowed" if decision.allowed else "blocked")
        logger.debug(f"[{self.plugin_name}] {title or torrent_id} => {'放行' if decision.allowed else '拦截'}：{decision.reason}")
        return decision

    def _request_torrent(self, profile: SiteProfile, torrent_id: str) -> Dict[str, Any]:
        if not profile.api_token or profile.api_token.startswith("在这里"):
            raise ValueError("API Token 未填写")
        url = profile.detail_url(torrent_id)
        params: Dict[str, str] = {}
        headers = {
            "Accept": "application/json",
            "User-Agent": f"MoviePilot-{self.__class__.__name__}/{self.plugin_version}",
            **dict(profile.headers),
        }
        if profile.auth_mode == "query":
            params[profile.token_param or "api_token"] = profile.api_token
        elif profile.auth_mode == "bearer":
            headers["Authorization"] = f"Bearer {profile.api_token}"
        elif profile.auth_mode == "header":
            headers[profile.token_header or "X-API-Key"] = profile.api_token
        else:
            raise ValueError(f"不支持的 auth_mode：{profile.auth_mode}")

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        if response.status_code == 401:
            raise RuntimeError("401 未授权，请检查 API Token 和认证方式")
        if response.status_code == 403:
            raise RuntimeError("403 无权限，站点可能未开放该 API")
        if response.status_code == 404:
            raise RuntimeError("404 未找到种子或 API 路径不兼容")
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("API 未返回 JSON，可能被登录页或 Cloudflare 拦截") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("API JSON 顶层不是对象")
        return payload

    def _should_check_context(self, context: Any) -> bool:
        if self._only_rss:
            source_obj = self._read(context, "resource_source")
            source = str(getattr(source_obj, "value", source_obj) or "").lower()
            if source and source != "rss" and not source.endswith(".rss"):
                return False
        torrent_info = self._read(context, "torrent_info")
        if not torrent_info:
            return False
        return self._find_profile(
            self._read(torrent_info, "site_name"),
            self._read(torrent_info, "page_url"),
            self._read(torrent_info, "enclosure"),
        ) is not None

    def _find_profile(self, site_name: str, page_url: str, enclosure: str) -> Optional[SiteProfile]:
        for profile in self._profiles:
            if profile.matches(site_name, page_url, enclosure):
                return profile
        return None

    def _error_decision(self, reason: str) -> SubtitleDecision:
        self._inc("errors")
        return SubtitleDecision(not self._fail_closed, reason, api_error=True)

    def _get_cache(self, key: str) -> Optional[SubtitleDecision]:
        with self._cache_lock:
            entry = self._cache.get(key)
            if not entry:
                return None
            if not entry.valid():
                self._cache.pop(key, None)
                return None
            return entry.decision

    def _put_cache(self, key: str, decision: SubtitleDecision):
        with self._cache_lock:
            self._cache[key] = CacheEntry(decision=decision, expires_at=time.time() + self._cache_ttl * 60)
            if len(self._cache) > 3000:
                expired = [cache_key for cache_key, entry in self._cache.items() if not entry.valid()]
                for cache_key in expired:
                    self._cache.pop(cache_key, None)
                while len(self._cache) > 2500:
                    self._cache.pop(next(iter(self._cache)), None)

    def _inc(self, key: str):
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + 1

    @staticmethod
    def _read(obj: Any, key: str, default: Any = None) -> Any:
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    @classmethod
    def _write(cls, obj: Any, key: str, value: Any):
        if isinstance(obj, dict):
            obj[key] = value
        else:
            setattr(obj, key, value)

    @classmethod
    def _context_value(cls, context: Any, section: str, key: str) -> Any:
        return cls._read(cls._read(context, section), key)

    @staticmethod
    def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _to_float(value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))
