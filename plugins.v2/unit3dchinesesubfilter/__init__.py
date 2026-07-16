from __future__ import annotations

import datetime
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.chain.download import DownloadChain
from app.core.context import Context, MediaInfo, TorrentInfo
from app.core.event import Event, eventmanager
from app.core.meta.words import WordsMatcher
from app.core.metainfo import MetaInfo
from app.db.subscribe_oper import SubscribeOper
from app.db.systemconfig_oper import SystemConfigOper
from app.helper.rss import RssHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, MediaType, SystemConfigKey

from .core import (
    CacheEntry,
    SiteConfig,
    SubtitleDecision,
    evaluate_mediainfo,
    extract_torrent_id,
    normalize_torrent_payload,
    safe_regex_match,
    split_custom_words,
    subscription_has_pending_episode,
)


class unit3dChineseSubFilter(_PluginBase):
    plugin_name = "UNIT3D 简中 RSS 后备订阅"
    plugin_desc = "BLU/ATR 独立 RSS 后备源；正常站点无资源时，经订阅识别与规则过滤后用 API 检查 MediaInfo 简体中文字幕。"
    plugin_icon = "rss.png"
    plugin_version = "0.2.0"
    plugin_author = "Community"
    author_url = "https://github.com/cudamin/MoviePilot-Plugins"
    plugin_config_prefix = "unit3d_chinese_sub_filter_"
    plugin_order = 35
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _verify_ssl = True
    _timeout = 10
    _cache_ttl = 1440
    _fallback_delay = 120
    _safety_interval = 30
    _site_tab = "blu"
    _sites: List[SiteConfig] = []

    _refresh_lock = threading.RLock()
    _refreshing = False
    _timer: Optional[threading.Timer] = None
    _last_schedule = 0.0
    _cache: Dict[str, CacheEntry] = {}
    _cache_lock = threading.RLock()
    _normal_candidates: Dict[str, float] = {}
    _normal_candidates_lock = threading.RLock()
    _stats_lock = threading.RLock()
    _stats = {
        "rss_runs": 0,
        "rss_items": 0,
        "subscription_matches": 0,
        "rule_rejected": 0,
        "api_checked": 0,
        "subtitle_rejected": 0,
        "downloaded": 0,
        "errors": 0,
    }

    def init_plugin(self, config: dict = None):
        config = config or {}
        self.stop_service()
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._verify_ssl = bool(config.get("verify_ssl", True))
        self._timeout = self._to_int(config.get("timeout"), 10, 2, 60)
        self._cache_ttl = self._to_int(config.get("cache_ttl"), 1440, 5, 43200)
        self._fallback_delay = self._to_int(config.get("fallback_delay"), 120, 15, 3600)
        self._safety_interval = self._to_int(config.get("safety_interval"), 30, 5, 1440)
        self._site_tab = str(config.get("site_tab") or "blu")
        self._sites = []
        for key, name, default_url in (
            ("blu", "BLU", "https://blutopia.cc"),
            ("atr", "ATR", "https://aither.cc"),
        ):
            if not config.get(f"{key}_enabled"):
                continue
            rss_url = str(config.get(f"{key}_rss_url") or "").strip()
            base_url = str(config.get(f"{key}_base_url") or default_url).strip().rstrip("/")
            token = str(config.get(f"{key}_api_token") or "").strip()
            endpoint = str(config.get(f"{key}_endpoint") or "/api/torrents/{id}").strip()
            auth_mode = str(config.get(f"{key}_auth_mode") or "query").strip().lower()
            token_param = str(config.get(f"{key}_token_param") or "api_token").strip()
            token_header = str(config.get(f"{key}_token_header") or "X-API-Key").strip()
            site_id = self._optional_int(config.get(f"{key}_site_id"))
            proxy = bool(config.get(f"{key}_proxy"))
            if not rss_url:
                logger.warning(f"[{self.plugin_name}] {name} 已启用但未填写 RSS 地址")
                continue
            self._sites.append(SiteConfig(
                key=key,
                name=name,
                rss_url=rss_url,
                base_url=base_url,
                api_token=token,
                endpoint=endpoint,
                auth_mode=auth_mode,
                token_param=token_param,
                token_header=token_header,
                mp_site_id=site_id,
                proxy=proxy,
            ))

        with self._cache_lock:
            self._cache.clear()
        with self._normal_candidates_lock:
            self._normal_candidates.clear()

        if self._enabled and self._onlyonce:
            timer = threading.Timer(3, self.refresh)
            timer.daemon = True
            timer.start()
            self._onlyonce = False
            config["onlyonce"] = False
            self.update_config(config)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [{
            "id": "Unit3dChineseSubFallback",
            "name": "UNIT3D 简中 RSS 后备刷新",
            "trigger": "interval",
            "func": self.refresh,
            "kwargs": {"minutes": self._safety_interval},
        }]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        common = [
            {
                "component": "VRow",
                "content": [
                    self._col("VSwitch", "enabled", "启用插件", 3),
                    self._col("VSwitch", "onlyonce", "立即运行一次", 3),
                    self._col("VSwitch", "verify_ssl", "验证 HTTPS 证书", 3),
                    self._col("VTextField", "fallback_delay", "正常 RSS 后等待（秒）", 3, type="number",
                              hint="系统正常 RSS 候选处理后再等待，确保普通站点优先下载。"),
                ],
            },
            {
                "component": "VRow",
                "content": [
                    self._col("VTextField", "timeout", "API 超时（秒）", 4, type="number"),
                    self._col("VTextField", "cache_ttl", "API 结果缓存（分钟）", 4, type="number"),
                    self._col("VTextField", "safety_interval", "兜底刷新周期（分钟）", 4, type="number",
                              hint="若系统 RSS 本轮没有触发资源选择事件，仍会按此周期检查。"),
                ],
            },
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": "仅检查 API MediaInfo 的 Text/Subtitle 轨，并且只接受 CHS、zh-CN、zh-Hans、Simplified Chinese、简体中文、简中等明确简体标记。不会检查 BDInfo、外挂字幕、标题或描述。",
                },
            },
        ]
        tabs = {
            "component": "VTabs",
            "props": {"model": "site_tab", "grow": True},
            "content": [
                {"component": "VTab", "props": {"value": "blu"}, "text": "BLU"},
                {"component": "VTab", "props": {"value": "atr"}, "text": "ATR"},
            ],
        }
        windows = {
            "component": "VWindow",
            "props": {"model": "site_tab"},
            "content": [
                {"component": "VWindowItem", "props": {"value": "blu"}, "content": self._site_form("blu", "BLU")},
                {"component": "VWindowItem", "props": {"value": "atr"}, "content": self._site_form("atr", "ATR")},
            ],
        }
        defaults = {
            "enabled": False,
            "onlyonce": False,
            "verify_ssl": True,
            "timeout": 10,
            "cache_ttl": 1440,
            "fallback_delay": 120,
            "safety_interval": 30,
            "site_tab": "blu",
            "blu_enabled": False,
            "blu_rss_url": "",
            "blu_base_url": "https://blutopia.cc",
            "blu_api_token": "",
            "blu_endpoint": "/api/torrents/{id}",
            "blu_auth_mode": "query",
            "blu_token_param": "api_token",
            "blu_token_header": "X-API-Key",
            "blu_site_id": "",
            "blu_proxy": False,
            "atr_enabled": False,
            "atr_rss_url": "",
            "atr_base_url": "https://aither.cc",
            "atr_api_token": "",
            "atr_endpoint": "/api/torrents/{id}",
            "atr_auth_mode": "query",
            "atr_token_param": "api_token",
            "atr_token_header": "X-API-Key",
            "atr_site_id": "",
            "atr_proxy": False,
        }
        return [{"component": "VForm", "content": [*common, tabs, windows]}], defaults

    def get_page(self) -> List[dict]:
        with self._stats_lock:
            stats = dict(self._stats)
        site_names = "、".join(site.name for site in self._sites) or "未配置"
        return [{
            "component": "VAlert",
            "props": {
                "type": "info",
                "variant": "tonal",
                "text": (
                    f"站点：{site_names}；RSS 刷新 {stats['rss_runs']} 次，读取 {stats['rss_items']} 条，"
                    f"命中订阅 {stats['subscription_matches']} 条，规则拒绝 {stats['rule_rejected']} 条，"
                    f"API 检查 {stats['api_checked']} 条，无简中拒绝 {stats['subtitle_rejected']} 条，"
                    f"已下载 {stats['downloaded']} 条，错误 {stats['errors']} 条。"
                ),
            },
        }]

    def stop_service(self):
        timer = self._timer
        self._timer = None
        if timer:
            try:
                timer.cancel()
            except Exception:
                pass
        with self._cache_lock:
            self._cache.clear()
        with self._normal_candidates_lock:
            self._normal_candidates.clear()

    @eventmanager.register(ChainEventType.ResourceSelection, priority=99)
    def on_system_resource_selection(self, event: Event):
        """正常 RSS 进入资源选择后，延迟触发本插件后备刷新。

        MoviePilot 当前没有公开的“系统 RSS 刷新完成”事件，因此使用正常 RSS 的
        ResourceSelection 作为同步信号，并保留 interval 服务作为空结果轮次的兜底。
        """
        if not self._enabled or self._refreshing or not event:
            return
        data = event.event_data
        if self._read(data, "source") == self.plugin_name:
            return
        contexts = self._read(data, "updated_contexts") if self._read(data, "updated") else None
        if contexts is None:
            contexts = self._read(data, "contexts")
        if not contexts:
            return
        normal_rss_found = False
        subscriptions = None
        for context in contexts:
            source = str(self._read(context, "resource_source", "") or "").lower()
            site_name = str(self._read(self._read(context, "torrent_info"), "site_name", "") or "").upper()
            if source != "rss" or site_name in {"BLU", "ATR"}:
                continue
            normal_rss_found = True
            mediainfo = self._read(context, "media_info")
            meta = self._read(context, "meta_info")
            if mediainfo and meta:
                if subscriptions is None:
                    subscriptions = SubscribeOper().list(state="R") or []
                subscribe = self._find_subscription(mediainfo, meta, subscriptions)
                if subscribe:
                    self._mark_normal_candidate(subscribe, meta)
        if normal_rss_found:
            self._schedule_refresh()


    def _mark_normal_candidate(self, subscribe: Any, meta: Any):
        expires_at = time.time() + max(600, self._fallback_delay + 300)
        keys = self._normal_candidate_keys(subscribe, meta, for_lookup=False)
        with self._normal_candidates_lock:
            self._purge_normal_candidates_locked()
            for key in keys:
                self._normal_candidates[key] = expires_at

    def _has_normal_candidate(self, subscribe: Any, meta: Any) -> bool:
        keys = self._normal_candidate_keys(subscribe, meta, for_lookup=True)
        with self._normal_candidates_lock:
            self._purge_normal_candidates_locked()
            return any(key in self._normal_candidates for key in keys)

    def _purge_normal_candidates_locked(self):
        now = time.time()
        expired = [key for key, expires_at in self._normal_candidates.items() if expires_at <= now]
        for key in expired:
            self._normal_candidates.pop(key, None)

    @classmethod
    def _normal_candidate_keys(cls, subscribe: Any, meta: Any, for_lookup: bool = False) -> set[str]:
        subscription_key = cls._subscription_key(subscribe)
        season = cls._read(meta, "begin_season", None)
        season_key = str(season if season is not None else getattr(subscribe, "season", "*") or "*")
        episodes = cls._read(meta, "episode_list", None) or []
        normalized_episodes = []
        for episode in episodes:
            try:
                normalized_episodes.append(int(episode))
            except (TypeError, ValueError):
                continue
        wildcard = f"{subscription_key}:s{season_key}:e*"
        if not normalized_episodes:
            return {wildcard}
        keys = {f"{subscription_key}:s{season_key}:e{episode}" for episode in normalized_episodes}
        if for_lookup:
            # 正常站点的季包占位可以压制后备单集；正常单集不会误伤其他集。
            keys.add(wildcard)
        return keys

    @staticmethod
    def _subscription_key(subscribe: Any) -> str:
        subscribe_id = getattr(subscribe, "id", None)
        if subscribe_id is not None:
            return f"sub:{subscribe_id}"
        for field in ("tmdbid", "doubanid", "bangumiid"):
            value = getattr(subscribe, field, None)
            if value:
                return f"{field}:{value}"
        return f"name:{getattr(subscribe, 'name', '')}"

    def _schedule_refresh(self):
        now = time.time()
        with self._refresh_lock:
            if now - self._last_schedule < max(30, self._fallback_delay // 2):
                return
            self._last_schedule = now
            if self._timer:
                try:
                    self._timer.cancel()
                except Exception:
                    pass
            self._timer = threading.Timer(self._fallback_delay, self.refresh)
            self._timer.daemon = True
            self._timer.start()
            logger.info(f"[{self.plugin_name}] 已收到系统 RSS 资源选择信号，将在 {self._fallback_delay} 秒后刷新后备 RSS")

    def refresh(self):
        if not self._enabled or not self._sites:
            return
        with self._refresh_lock:
            if self._refreshing:
                return
            self._refreshing = True
        try:
            self._inc("rss_runs")
            subscriptions = SubscribeOper().list(state="R") or []
            if not subscriptions:
                logger.debug(f"[{self.plugin_name}] 没有进行中的订阅，跳过后备 RSS")
                return
            downloaded_keys = set(self.get_data("downloaded_keys") or [])
            for site in self._sites:
                self._refresh_site(site, subscriptions, downloaded_keys)
            self.save_data("downloaded_keys", list(downloaded_keys)[-2000:])
        except Exception as exc:
            self._inc("errors")
            logger.error(f"[{self.plugin_name}] RSS 刷新失败：{self._safe_error_text(exc)}")
        finally:
            with self._refresh_lock:
                self._refreshing = False

    def _refresh_site(self, site: SiteConfig, subscriptions: List[Any], downloaded_keys: set):
        logger.info(f"[{self.plugin_name}] 开始刷新 {site.name} 后备 RSS")
        results = RssHelper().parse(site.rss_url, proxy=site.proxy, timeout=self._timeout)
        if not results:
            logger.warning(f"[{self.plugin_name}] {site.name} 未获取到 RSS 数据")
            return
        for item in results:
            self._inc("rss_items")
            title = str(item.get("title") or "").strip()
            description = str(item.get("description") or "").strip()
            enclosure = str(item.get("enclosure") or "").strip()
            link = str(item.get("link") or "").strip()
            if not title or not enclosure:
                continue
            item_key = f"{site.key}:{extract_torrent_id(link, enclosure) or enclosure}"
            if item_key in downloaded_keys:
                continue
            try:
                candidate = self._match_subscription(site, item, subscriptions)
                if not candidate:
                    continue
                subscribe, meta, mediainfo, torrentinfo = candidate
                self._inc("subscription_matches")

                # 正常 RSS 只要已经产生同一订阅/同一集候选，本轮就禁止后备源参与。
                if self._has_normal_candidate(subscribe, meta):
                    logger.info(f"[{self.plugin_name}] 正常站点已有同集候选，跳过后备资源：{title}")
                    continue

                # 第一次缺集检查：正常站点本轮若已下载，会把按集事实写入 note/episode_priority。
                if not subscription_has_pending_episode(subscribe, meta.episode_list):
                    continue

                torrent_id = extract_torrent_id(link, enclosure)
                if not torrent_id:
                    logger.info(f"[{self.plugin_name}] {site.name} 无法提取 torrent id：{title}")
                    continue
                decision = self._check_api(site, torrent_id)
                if not decision.allowed:
                    self._inc("subtitle_rejected")
                    logger.info(f"[{self.plugin_name}] 拒绝 {title}：{decision.reason}")
                    continue

                # 下载前重新从数据库获取订阅状态，避免正常站点在 API 查询期间刚好完成下载。
                latest = self._find_subscription(mediainfo, meta, SubscribeOper().list(state="R") or [])
                if (not latest
                        or self._has_normal_candidate(latest, meta)
                        or not subscription_has_pending_episode(latest, meta.episode_list)):
                    logger.info(f"[{self.plugin_name}] {title} 已被正常站点候选或下载满足，取消后备下载")
                    continue

                context = Context(
                    meta_info=meta,
                    media_info=mediainfo,
                    torrent_info=torrentinfo,
                    resource_source="rss",
                    match_source="plugin",
                    candidate_recognized=True,
                )
                wanted_episodes = set(meta.episode_list or []) or None
                result = DownloadChain().download_single(
                    context=context,
                    episodes=wanted_episodes,
                    source="Subscribe",
                    downloader=latest.downloader,
                    save_path=latest.save_path,
                    username=self.plugin_name,
                    custom_words=latest.custom_words,
                )
                if result:
                    downloaded_keys.add(item_key)
                    self._inc("downloaded")
                    logger.info(f"[{self.plugin_name}] 已作为最低优先级后备下载：{title}")
            except Exception as exc:
                self._inc("errors")
                logger.error(f"[{self.plugin_name}] 处理 {site.name} RSS 条目失败：{title}；{self._safe_error_text(exc, site)}")
        logger.info(f"[{self.plugin_name}] {site.name} 后备 RSS 刷新完成")

    def _match_subscription(self, site: SiteConfig, item: dict, subscriptions: List[Any]):
        original_title = str(item.get("title") or "")
        description = str(item.get("description") or "")
        global_title, _ = WordsMatcher().prepare(original_title)
        meta = MetaInfo(title=global_title, subtitle=description)
        if not meta.name:
            return None
        mediainfo: MediaInfo = self.chain.recognize_media(meta=meta)
        if not mediainfo or mediainfo.type != MediaType.TV:
            return None
        subscribe = self._find_subscription(mediainfo, meta, subscriptions)
        if not subscribe:
            return None
        if subscribe.sites:
            allowed_site_ids = {str(value) for value in subscribe.sites}
            if site.mp_site_id is None or str(site.mp_site_id) not in allowed_site_ids:
                return None

        custom_title, _ = WordsMatcher().prepare(original_title, split_custom_words(subscribe.custom_words))
        meta = MetaInfo(title=custom_title, subtitle=description)
        if not meta.name:
            return None
        recognize_kwargs = {"meta": meta}
        if subscribe.tmdbid:
            recognize_kwargs["tmdbid"] = subscribe.tmdbid
        elif subscribe.doubanid:
            recognize_kwargs["doubanid"] = subscribe.doubanid
        if subscribe.episode_group:
            recognize_kwargs["episode_group"] = subscribe.episode_group
        mediainfo = self.chain.recognize_media(**recognize_kwargs)
        if not mediainfo:
            return None
        subscribe = self._find_subscription(mediainfo, meta, subscriptions)
        if not subscribe:
            return None

        text = f"{original_title} {description}"
        if subscribe.include and not safe_regex_match(subscribe.include, text):
            self._inc("rule_rejected")
            return None
        if subscribe.exclude and safe_regex_match(subscribe.exclude, text):
            self._inc("rule_rejected")
            return None

        pubdate = item.get("pubdate")
        if isinstance(pubdate, datetime.datetime):
            pubdate_text = pubdate.strftime("%Y-%m-%d %H:%M:%S")
        else:
            pubdate_text = str(pubdate or "") or None
        torrentinfo = TorrentInfo(
            site=site.mp_site_id,
            site_name=site.name,
            site_proxy=site.proxy,
            site_order=999999,
            title=original_title,
            description=description,
            enclosure=item.get("enclosure"),
            page_url=item.get("link"),
            size=item.get("size") or 0,
            seeders=item.get("seeders") or 0,
            peers=item.get("peers") or 0,
            grabs=item.get("grabs") or 0,
            pubdate=pubdate_text,
        )
        groups = subscribe.filter_groups or SystemConfigOper().get(SystemConfigKey.SubscribeFilterRuleGroups)
        filtered = self.chain.filter_torrents(
            rule_groups=groups,
            torrent_list=[torrentinfo],
            mediainfo=mediainfo,
        )
        if not filtered:
            self._inc("rule_rejected")
            return None
        torrentinfo = filtered[0]
        # 该数字只是显式标记；真正的“最低优先级”由延迟执行和两次缺集检查保证。
        torrentinfo.site_order = 999999
        return subscribe, meta, mediainfo, torrentinfo

    @staticmethod
    def _find_subscription(mediainfo: MediaInfo, meta: Any, subscriptions: List[Any]):
        season = meta.begin_season
        for subscribe in subscriptions:
            if subscribe.type != MediaType.TV.value:
                continue
            if subscribe.season is not None and season is not None and int(subscribe.season) != int(season):
                continue
            if subscribe.tmdbid and mediainfo.tmdb_id and int(subscribe.tmdbid) == int(mediainfo.tmdb_id):
                return subscribe
            if subscribe.doubanid and mediainfo.douban_id and str(subscribe.doubanid) == str(mediainfo.douban_id):
                return subscribe
            if subscribe.bangumiid and getattr(mediainfo, "bangumi_id", None) and int(subscribe.bangumiid) == int(mediainfo.bangumi_id):
                return subscribe
        return None

    def _check_api(self, site: SiteConfig, torrent_id: str) -> SubtitleDecision:
        cache_key = f"{site.key}:{torrent_id}"
        with self._cache_lock:
            entry = self._cache.get(cache_key)
            if entry and entry.valid():
                return entry.decision
            if entry:
                self._cache.pop(cache_key, None)
        self._inc("api_checked")
        try:
            payload = self._request_torrent(site, torrent_id)
            attrs = normalize_torrent_payload(payload)
            if not attrs:
                decision = SubtitleDecision(False, f"{site.name} API 未返回种子属性", api_error=True)
            else:
                decision = evaluate_mediainfo(attrs)
                if decision.allowed:
                    decision = SubtitleDecision(True, f"{site.name} API 已确认简体中文字幕", decision.evidence)
        except Exception as exc:
            decision = SubtitleDecision(False, f"{site.name} API 检查失败：{self._safe_error_text(exc, site)}", api_error=True)
        with self._cache_lock:
            self._cache[cache_key] = CacheEntry(decision, time.time() + self._cache_ttl * 60)
        return decision

    def _request_torrent(self, site: SiteConfig, torrent_id: str) -> dict:
        if not site.api_token:
            raise ValueError("API Token 未填写")
        url = site.detail_url(torrent_id)
        params: Dict[str, str] = {}
        headers = {
            "Accept": "application/json",
            "User-Agent": f"MoviePilot-{self.__class__.__name__}/{self.plugin_version}",
        }
        if site.auth_mode == "query":
            params[site.token_param or "api_token"] = site.api_token
        elif site.auth_mode == "bearer":
            headers["Authorization"] = f"Bearer {site.api_token}"
        elif site.auth_mode == "header":
            headers[site.token_header or "X-API-Key"] = site.api_token
        else:
            raise ValueError(f"不支持的认证方式：{site.auth_mode}")
        try:
            response = requests.get(url, params=params, headers=headers, timeout=self._timeout, verify=self._verify_ssl)
        except requests.RequestException as exc:
            raise RuntimeError(f"网络请求失败（{exc.__class__.__name__}）") from exc
        if response.status_code == 401:
            raise RuntimeError("401 未授权，请检查 API Token")
        if response.status_code == 403:
            raise RuntimeError("403 无权限，站点可能未开放该 API")
        if response.status_code == 404:
            raise RuntimeError("404 未找到种子或 API 路径不兼容")
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"HTTP {response.status_code}，UNIT3D API 请求失败")
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("API 未返回 JSON，可能被登录页或 Cloudflare 拦截") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("API JSON 顶层不是对象")
        return payload

    @staticmethod
    def _site_form(key: str, name: str) -> List[dict]:
        return [{
            "component": "VCard",
            "props": {"variant": "flat", "class": "mt-3"},
            "content": [{
                "component": "VCardText",
                "content": [
                    {"component": "VRow", "content": [
                        Unit3dChineseSubFilter._col("VSwitch", f"{key}_enabled", f"启用 {name}", 4),
                        Unit3dChineseSubFilter._col("VSwitch", f"{key}_proxy", "RSS 使用代理", 4),
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_site_id", "MoviePilot 站点 ID（可选）", 4, type="number",
                                                    hint="订阅限制了站点范围时必须填写；可在 MP 站点管理中查看。"),
                    ]},
                    {"component": "VRow", "content": [
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_rss_url", f"{name} RSS 地址", 12,
                                                    placeholder="包含个人密钥的完整 RSS URL"),
                    ]},
                    {"component": "VRow", "content": [
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_base_url", "站点地址", 6),
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_api_token", "UNIT3D API Token", 6,
                                                    type="password"),
                    ]},
                    {"component": "VRow", "content": [
                        {
                            "component": "VCol", "props": {"cols": 12, "md": 4},
                            "content": [{
                                "component": "VSelect",
                                "props": {
                                    "model": f"{key}_auth_mode",
                                    "label": "API 认证方式",
                                    "items": [
                                        {"title": "URL 参数", "value": "query"},
                                        {"title": "Bearer", "value": "bearer"},
                                        {"title": "请求头", "value": "header"},
                                    ],
                                },
                            }],
                        },
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_token_param", "Token 参数名", 4,
                                                    hint="URL 参数模式通常为 api_token。"),
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_token_header", "Token 请求头名", 4,
                                                    hint="请求头模式使用。"),
                    ]},
                    {"component": "VRow", "content": [
                        Unit3dChineseSubFilter._col("VTextField", f"{key}_endpoint", "种子详情 API 路径", 12,
                                                    hint="{id} 会替换为 RSS 中的种子 ID。"),
                    ]},
                ],
            }],
        }]

    @staticmethod
    def _col(component: str, model: str, label: str, md: int, **props) -> dict:
        component_props = {"model": model, "label": label, **props}
        if "hint" in component_props:
            component_props["persistent-hint"] = True
        return {
            "component": "VCol",
            "props": {"cols": 12, "md": md},
            "content": [{"component": component, "props": component_props}],
        }

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

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _safe_error_text(exc: Exception, site: SiteConfig = None) -> str:
        text = str(exc) or exc.__class__.__name__
        if site and site.api_token and len(site.api_token) >= 6:
            text = text.replace(site.api_token, "***")
        text = re.sub(r"(?i)([?&](?:api[_-]?token|token|apikey|api[_-]?key|key)=)[^&\s]+", r"\1***", text)
        return text[:300]
