"""
SpaceCleaner: 空间清理 + 智能RSS下载 + tmdbid标签，共用播放进度缓存。
"""
import os, re, time, threading, shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app import schemas
from app.chain.download import DownloadChain
from app.core.config import settings
from app.core.context import Context, TorrentInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfo
from app.core.meta.metavideo import MetaVideo
from app.log import logger
from app.plugins import _PluginBase
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.models.transferhistory import TransferHistory
from app.utils.system import SystemUtils
from app.chain import ChainBase
from app.helper.rss import RssHelper
from app.schemas import NotificationType
from app.schemas.types import EventType, MediaImageType, MediaType
from app.utils.http import RequestUtils


class spacecleaner(_PluginBase):
    plugin_name = "空间清理器"
    plugin_desc = "剩余空间不足时自动删除已观看资源；智能RSS下载自动跳过已看完剧集；tmdbid标签识别。"
    plugin_icon = "delete.png"
    plugin_version = "4.1.3"
    plugin_label = "系统工具"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin/MoviePilot-Plugins"
    plugin_config_prefix = "spacecleaner_"
    plugin_order = 10
    auth_level = 1

    # === 空间清理配置 ===
    _enabled = False
    _min_free_percent = 10
    _delete_by_target = False
    _target_free_percent = 20
    _delete_count = 1
    _check_interval = 6
    _dry_run = False
    _delete_same_size = True
    _delete_seed_replicas = False  # 删除同 tmdbid 标签的种子
    _delete_same_name_torrents = False  # 删种时删除下载器中所有同名的种子
    _notify = True
    _media_cache_disabled = False  # 关闭媒体缓存（默认开启）
    _pb_page = 1
    _pb_sort_by = "time"  # 播放缓存排序：time / title / status
    _pb_sort_desc = True  # 播放缓存排序方向：True 降序 / False 升序
    _pb_filter_watched = True  # 播放缓存默认只显示已看完
    _watched_threshold = 85  # 标记已看播放进度阈值（%）
    _clean_downloader = []  # 空间清理扫描的下载器，空列表扫描全部

    # === RSS 下载配置 ===
    _rss_on = False
    _rss_cron = ""
    _rss_urls = ""
    _rss_dl = ""
    _rss_sz = ""
    _rss_inc = ""
    _rss_exc = ""
    _rss_once = False
    _rss_ntf = False
    _rss_th = 85
    _rss_wash_mode = False  # 洗版模式：播放进度低于阈值时触发洗版，只下载最早版本
    _rss_save_path = ""  # RSS 下载自定义保存路径

    # === tmdbid标签配置 ===
    _tag_enabled = False
    _tag_interval = 24  # 标签扫描间隔（小时）
    _tag_downloaders: List[str] = []  # 标签扫描的下载器，空列表扫描全部
    _tag_run_now = False
    _tag_scheduler_thread: Optional[threading.Thread] = None
    _tag_scheduler_running = False
    _tag_scheduler_event: Optional[threading.Event] = None

    # === 内部状态 ===
    _scheduler_thread = None
    _scheduler_running = False
    _scheduler_event = None
    _chain = None
    _running = False
    _cached_space_info = None
    _cached_space_time = 0
    _space_cache_ttl = 10
    _all_torrents_cache = None
    _all_torrents_cache_time = 0
    _torrents_cache_ttl = 120
    _pb_cache = None
    _pb_cache_time = 0
    _pb_cache_ttl = 30
    _pb: List[dict] = []
    _pb_max = 5000
    _pb_lock = threading.Lock()
    _rss_s: Optional[BackgroundScheduler] = None
    _rss_busy = False
    _rss_seen: set = set()
    _rss_lk = threading.Lock()

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._enabled = self._rss_on = self._tag_enabled = False
        self._min_free_percent = 10
        self._delete_by_target = self._dry_run = self._notify = False
        self._delete_same_size = True
        self._delete_seed_replicas = False
        self._delete_same_name_torrents = False
        self._media_cache_disabled = False
        self._pb_page = 1
        self._pb_sort_by = "time"
        self._pb_sort_desc = True
        self._pb_filter_watched = True
        self._watched_threshold = 85
        self._delete_count = 1
        self._check_interval = 6
        self._clean_downloader = []
        self._rss_cron = self._rss_urls = self._rss_sz = self._rss_inc = self._rss_exc = ""
        self._rss_dl = ""
        self._rss_once = self._rss_ntf = False
        self._rss_th = 85
        self._rss_wash_mode = False
        self._rss_save_path = ""
        self._tag_interval = 24
        self._tag_downloaders = []
        self._tag_run_now = False
        self._pb = list(self.get_data("pb") or [])
        self._rss_seen = set()
        self._stop_rss_scheduler()

        if not config:
            return

        # 空间清理配置
        self._enabled = bool(config.get("enabled"))
        self._min_free_percent = int(config.get("min_free_percent") or 10)
        self._delete_by_target = bool(config.get("delete_by_target"))
        self._target_free_percent = int(config.get("target_free_percent") or 20)
        self._delete_count = int(config.get("delete_count") or 1)
        self._check_interval = int(config.get("check_interval") or 6)
        self._dry_run = bool(config.get("dry_run"))
        self._delete_same_size = bool(config.get("delete_same_size"))
        self._delete_seed_replicas = bool(config.get("delete_seed_replicas"))
        self._delete_same_name_torrents = bool(config.get("delete_same_name_torrents"))
        self._notify = bool(config.get("notify", True))
        self._media_cache_disabled = bool(config.get("media_cache_disabled", False))
        try:
            self._pb_page = max(1, int(config.get("pb_page") or 1))
        except (ValueError, TypeError):
            self._pb_page = 1
        self._pb_sort_by = str(config.get("pb_sort_by") or "time")
        if self._pb_sort_by not in ("time", "title", "status"):
            self._pb_sort_by = "time"
        self._pb_sort_desc = bool(config.get("pb_sort_desc", True))
        self._pb_filter_watched = bool(config.get("pb_filter_watched", True))
        try:
            self._watched_threshold = int(config.get("watched_threshold") or 85)
        except (ValueError, TypeError):
            self._watched_threshold = 85
        raw = config.get("clean_downloader") or []
        if isinstance(raw, list):
            self._clean_downloader = [str(d) for d in raw if d]
        elif isinstance(raw, str):
            self._clean_downloader = [raw] if raw else []
        else:
            self._clean_downloader = []
        run_now = bool(config.get("run_now"))

        # RSS 下载配置
        self._rss_on = bool(config.get("rss_on"))
        self._rss_cron = str(config.get("rss_cron") or "")
        self._rss_urls = str(config.get("rss_urls") or "")
        self._rss_dl = str(config.get("rss_dl") or "")
        self._rss_sz = str(config.get("rss_sz") or "")
        self._rss_inc = str(config.get("rss_inc") or "")
        self._rss_exc = str(config.get("rss_exc") or "")
        self._rss_once = bool(config.get("rss_once"))
        self._rss_ntf = bool(config.get("rss_ntf"))
        try:
            self._rss_th = int(config.get("rss_th") or 85)
        except (ValueError, TypeError):
            self._rss_th = 85
        self._rss_seen = set(self.get_data("rss_seen") or [])
        self._rss_wash_mode = bool(config.get("rss_wash_mode"))
        self._rss_save_path = str(config.get("rss_save_path") or "")

        # tmdbid标签配置
        self._tag_enabled = bool(config.get("tag_enabled"))
        self._tag_interval = max(int(config.get("tag_interval") or 24), 1)
        raw_tag_dl = config.get("tag_downloaders") or []
        if isinstance(raw_tag_dl, list):
            self._tag_downloaders = [str(d) for d in raw_tag_dl if d]
        elif isinstance(raw_tag_dl, str):
            self._tag_downloaders = [raw_tag_dl] if raw_tag_dl else []
        else:
            self._tag_downloaders = []
        self._tag_run_now = bool(config.get("tag_run_now"))

        if self._enabled:
            self._start_scheduler()
        if run_now:
            config["run_now"] = False
            self.update_config(config)
            threading.Thread(target=self._run_now_task, daemon=True, name="SC-RunNow").start()

        if self._tag_enabled:
            self._start_tag_scheduler()
        if self._tag_run_now:
            self._tag_run_now = False
            self._update_config()
            threading.Thread(target=self._scan_and_tag, daemon=True, name="SC-TmdbRunNow").start()

        if self._rss_once:
            self._rss_once = False
            self._update_config()
            if self._rss_on and self._rss_urls:
                s = BackgroundScheduler(timezone=settings.TZ)
                s.add_job(self._rss_run, "date", run_date=datetime.now())
                s.start()
                self._rss_s = s
            return
        if self._rss_on and self._rss_cron and self._rss_urls:
            s = BackgroundScheduler(timezone=settings.TZ)
            s.add_job(self._rss_run, CronTrigger.from_crontab(self._rss_cron))
            s.start()
            self._rss_s = s

    def _update_config(self):
        self.update_config({
            "enabled": self._enabled, "min_free_percent": self._min_free_percent,
            "delete_by_target": self._delete_by_target, "target_free_percent": self._target_free_percent,
            "delete_count": self._delete_count, "check_interval": self._check_interval,
            "dry_run": self._dry_run, "delete_same_size": self._delete_same_size,
            "delete_seed_replicas": self._delete_seed_replicas, "delete_same_name_torrents": self._delete_same_name_torrents, "notify": self._notify,
            "media_cache_disabled": self._media_cache_disabled, "pb_page": self._pb_page, "run_now": False,
            "pb_sort_by": self._pb_sort_by, "pb_sort_desc": self._pb_sort_desc,
            "pb_filter_watched": self._pb_filter_watched, "watched_threshold": self._watched_threshold,
            "rss_on": self._rss_on, "rss_cron": self._rss_cron, "rss_urls": self._rss_urls,
            "rss_dl": self._rss_dl, "rss_sz": self._rss_sz, "rss_inc": self._rss_inc,
            "rss_exc": self._rss_exc, "rss_once": self._rss_once, "rss_ntf": self._rss_ntf,
            "rss_th": self._rss_th, "rss_wash_mode": self._rss_wash_mode,
            "rss_save_path": self._rss_save_path,
            "clean_downloader": self._clean_downloader,
            "tag_enabled": self._tag_enabled,
            "tag_interval": self._tag_interval, "tag_downloaders": self._tag_downloaders,
            "tag_run_now": self._tag_run_now,
        })

    def get_state(self) -> bool:
        return self._enabled or self._rss_on or self._tag_enabled

    # ==================== Webhook 共用播放缓存 ====================

    @eventmanager.register(EventType.WebhookMessage)
    def on_webhook(self, event: Event) -> None:
        if self._media_cache_disabled:
            logger.info("SC on_webhook skipped: media_cache_disabled=True")
            return
        if not self._enabled and not self._rss_on and not self._tag_enabled:
            logger.info(f"SC on_webhook skipped: enabled={self._enabled} rss_on={self._rss_on} tag_enabled={self._tag_enabled}")
            return
        try:
            from app.schemas.mediaserver import WebhookEventInfo
            ev: WebhookEventInfo = event.event_data
        except Exception:
            return
        if not ev:
            return
        if ev.event not in ("playback.stop", "PlaybackStopped", "playback.pause", "PlaybackPaused", "media.stop"):
            return
        pct = ev.percentage or 0
        logger.info(f"SC webhook: {ev.item_name} event={ev.event} media_type={ev.media_type} tmdb={ev.tmdb_id} s={ev.season_id} e={ev.episode_id} {pct:.1f}%")
        if ev.media_type not in ("TV", "电视剧", "SHOW", "SERIES", "Episode", "episode", "Movie", "movie"):
            return
        if not ev.item_name:
            return
        if ev.media_type in ("TV", "电视剧", "SHOW", "SERIES", "Episode", "episode"):
            if not ev.season_id or not ev.episode_id:
                return
        tid = ev.tmdb_id
        if not tid and ev.item_path:
            m = re.search(r'tmdbid[=_](\d+)', ev.item_path)
            if m:
                tid = m.group(1)
        # 如果还没有 tmdb_id，尝试从已有缓存中按 (season, episode) 反查
        if not tid and ev.season_id and ev.episode_id:
            try:
                ssn = int(ev.season_id)
                een = int(ev.episode_id)
                with self._pb_lock:
                    for r in self._pb:
                        if r.get("s") == ssn and r.get("e") == een:
                            km = re.match(r'(\d+):', r.get("k", ""))
                            if km:
                                tid = km.group(1)
                                break
            except (ValueError, TypeError):
                pass
        if not tid:
            return
        try:
            tmdb = int(tid)
            pct = float(pct)
        except (ValueError, TypeError):
            return
        try:
            sn = int(ev.season_id) if ev.season_id else 0
            en = int(ev.episode_id) if ev.episode_id else 0
        except (ValueError, TypeError):
            sn = en = 0
        if sn > 0 and en > 0:
            k = f"{tmdb}:S{sn:02d}E{en:02d}"
            se_display = f"S{sn:02d}E{en:02d}"
            n = self._normalize_episode_display(ev.item_name, sn, en)
        else:
            k = f"{tmdb}:M"
            n = ev.item_name
            se_display = ""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    if pct > (r.get("p", 0) or 0):
                        r["p"] = pct
                        r["t"] = ts
                    self.save_data("pb", self._pb)
                    logger.info(f"SC cached: {n} {se_display} {pct:.1f}%")
                    return
            self._pb.append({"k": k, "n": n, "s": sn, "e": en, "p": pct, "t": ts})
            if len(self._pb) > self._pb_max:
                self._pb = self._pb[-self._pb_max:]
        self.save_data("pb", self._pb)
        logger.info(f"SC cached: {n} {se_display} {pct:.1f}%")

    # ==================== API ====================

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/dry_run", "endpoint": self.api_dry_run, "methods": ["GET"], "summary": "试运行"},
            {"path": "/run_now", "endpoint": self.api_run_now, "methods": ["GET"], "summary": "立即清理"},
            {"path": "/delete_history", "endpoint": self.api_delete_history, "methods": ["GET"], "summary": "删除历史"},
            {"path": "/space_info", "endpoint": self.api_space_info, "methods": ["GET"], "summary": "空间信息"},
            {"path": "/del_pb_item", "endpoint": self.del_pb_item, "methods": ["GET"], "summary": "删除单条播放缓存"},
            {"path": "/clear_pb", "endpoint": self.clear_pb, "methods": ["GET"], "summary": "清除所有播放缓存"},
            {"path": "/pb_page", "endpoint": self.set_pb_page, "methods": ["GET"], "summary": "设置播放缓存页码"},
            {"path": "/pb_sort", "endpoint": self.set_pb_sort, "methods": ["GET"], "summary": "设置播放缓存排序"},
            {"path": "/pb_filter_toggle", "endpoint": self.toggle_pb_filter, "methods": ["GET"], "summary": "切换已看完筛选"},
            {"path": "/rss_dh", "endpoint": self.rss_dh, "methods": ["GET"], "summary": "删除RSS历史"},
            {"path": "/rss_ca", "endpoint": self.rss_ca, "methods": ["GET"], "summary": "清除RSS数据"},
            {"path": "/tag_scan_now", "endpoint": self.api_tag_scan_now, "methods": ["GET"], "summary": "立即扫描tmdbid标签"},
        ]

    def rss_dh(self, k: str, apikey: str):
        return schemas.Response(success=True)

    def rss_ca(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self.save_data("rss_seen", [])
        self._rss_seen = set()
        return schemas.Response(success=True)

    def del_pb_item(self, k: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        with self._pb_lock:
            before = len(self._pb)
            self._pb = [r for r in self._pb if r.get("k") != k]
        if len(self._pb) != before:
            self.save_data("pb", self._pb)
            self._pb_cache = None
            logger.info(f"SC 删除单条缓存: {k}")
        return schemas.Response(success=True)

    def clear_pb(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        with self._pb_lock:
            self._pb.clear()
        self.save_data("pb", [])
        self._invalidate_caches()
        logger.info("SC 播放缓存已清除")
        return schemas.Response(success=True)

    def set_pb_page(self, page: int, apikey: str):
        """设置播放缓存当前页码。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        try:
            self._pb_page = max(1, int(page or 1))
        except (ValueError, TypeError):
            self._pb_page = 1
        self._update_config()
        return schemas.Response(success=True)

    def set_pb_sort(self, sort_by: str, apikey: str):
        """设置播放缓存排序方式。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if sort_by not in ("time", "title", "status"):
            return schemas.Response(success=False, message="无效排序字段")
        if self._pb_sort_by == sort_by:
            # 同字段切换排序方向
            self._pb_sort_desc = not self._pb_sort_desc
        else:
            self._pb_sort_by = sort_by
            self._pb_sort_desc = True  # 默认降序
        self._pb_page = 1
        self._update_config()
        return schemas.Response(success=True)

    def toggle_pb_filter(self, apikey: str):
        """切换播放缓存已看完筛选。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_filter_watched = not self._pb_filter_watched
        self._pb_page = 1
        self._update_config()
        return schemas.Response(success=True)

    @staticmethod
    def _normalize_episode_display(name: str, season: int, episode: int) -> str:
        """统一播放记录名称中的季集格式为 S01E06。"""
        if not name:
            return name
        se_display = f"S{season:02d}E{episode:02d}"
        normalized = re.sub(rf"S0*{season}E0*{episode}", se_display, name, count=1, flags=re.IGNORECASE)
        if normalized != name:
            return normalized
        return f"{name} {se_display}"

    @staticmethod
    def _normalize_cached_name(name: str, season: int = 0, episode: int = 0) -> str:
        """统一缓存记录名称中的季集格式为 S01E06。"""
        if not name:
            return name
        if season > 0 and episode > 0:
            se_display = f"S{season:02d}E{episode:02d}"
            normalized = re.sub(rf"S0*{season}E0*{episode}", se_display, name, count=1, flags=re.IGNORECASE)
            return normalized if normalized != name else f"{name} {se_display}"
        return re.sub(r"S(\d{1,2})E(\d{1,2})", lambda m: f"S{int(m.group(1)):02d}E{int(m.group(2)):02d}", name, flags=re.IGNORECASE)

    # ==================== 表单（两个标签页） ====================

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        dls = []
        try:
            from app.helper.downloader import DownloaderHelper
            svcs = DownloaderHelper().get_services()
            dls = [{"title": n, "value": n} for n, s in svcs.items() if s.config and s.config.enabled]
        except Exception:
            pass
        return [
            {
                "component": "VExpansionPanels",
                "props": {"multiple": False, "modelValue": 0},
                "content": [
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {"component": "VExpansionPanelTitle", "text": "空间清理"},
                            {"component": "VExpansionPanelText", "content": [
                                {
                                    "component": "VForm",
                                    "content": [
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "删除时发送通知"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "media_cache_disabled", "label": "关闭媒体缓存", "hint": "开启后不再接收媒体服务器播放进度，也不新增播放记录", "persistent-hint": True}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "delete_by_target", "label": "按目标百分比删除", "hint": "将持续删除资源直到磁盘剩余空间达到目标百分比", "persistent-hint": True}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "dry_run", "label": "试运行模式", "hint": "仅在日志中显示将要删除的资源，不执行实际删除操作", "persistent-hint": True}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "delete_seed_replicas", "label": "删除同 tmdbid 标签的种子"}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "delete_same_name_torrents", "label": "删种同名种子", "hint": "触发删种时检索下载器，删除所有与被删种子同名的种子", "persistent-hint": True}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSwitch", "props": {"model": "run_now", "label": "立即运行一次"}}]},
                                        ]},
                                        {"component": "VDivider", "props": {"class": "my-2"}},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "min_free_percent", "label": "删种触发阈值（%）", "type": "number", "min": 1, "max": 99}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "target_free_percent", "label": "目标剩余百分比", "type": "number", "min": 1, "max": 99}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "check_interval", "label": "检查间隔（小时）", "type": "number", "min": 1}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "delete_count", "label": "单次删除资源数", "type": "number", "min": 1}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "watched_threshold", "label": "标记已看播放进度阈值（%）", "type": "number", "min": 1, "max": 100, "hint": "播放进度达到此百分比时标记为已观看", "persistent-hint": True}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSelect", "props": {"model": "clean_downloader", "label": "扫描下载器", "items": dls, "multiple": True, "chips": True, "clearable": True, "hint": "删种时扫描的下载器，留空扫描全部", "persistent-hint": True}}]},
                                        ]},
                                    ],
                                },
                            ]},
                        ],
                    },
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {"component": "VExpansionPanelTitle", "text": "BT动漫RSS下载/洗版"},
                            {"component": "VExpansionPanelText", "content": [
                                {
                                    "component": "VForm",
                                    "content": [
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_on", "label": "启用"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_ntf", "label": "通知"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_once", "label": "立即运行"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_wash_mode", "label": "洗版模式", "hint": "播放进度低于阈值或无播放缓存时触发洗版，洗版只下载最早发布的版本", "persistent-hint": True}}]},
                                        ]},
                                        {"component": "VDivider", "props": {"class": "my-2"}},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "rss_th", "label": "洗版播放进度阈值(%)", "type": "number"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "rss_sz", "label": "种子大小过滤(GB)", "placeholder": "1-10"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VCronField", "props": {"model": "rss_cron", "label": "执行周期"}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "rss_save_path", "label": "自定义保存路径", "placeholder": "留空使用默认路径", "hint": "支持 <storage>:<path> 格式", "persistent-hint": True}}]},
                                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextarea", "props": {"model": "rss_urls", "label": "RSS链接", "rows": 4}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "rss_inc", "label": "包含(正则)"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "rss_exc", "label": "排除(正则)"}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSelect", "props": {"model": "rss_dl", "label": "下载器", "items": dls}}]},
                                        ]},
                                    ],
                                },
                            ]},
                        ],
                    },
                    {
                        "component": "VExpansionPanel",
                        "content": [
                            {"component": "VExpansionPanelTitle", "text": "tmdbid标签"},
                            {"component": "VExpansionPanelText", "content": [
                                {
                                    "component": "VForm",
                                    "content": [
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "tag_enabled", "label": "启用tmdbid标签"}}]},
                                            {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "tag_run_now", "label": "立即运行"}}]},
                                        ]},
                                        {"component": "VDivider", "props": {"class": "my-2"}},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "tag_interval", "label": "扫描间隔（小时）", "type": "number", "min": 1}}]},
                                        ]},
                                        {"component": "VRow", "content": [
                                            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSelect", "props": {"model": "tag_downloaders", "label": "扫描下载器", "items": dls, "multiple": True, "chips": True, "clearable": True, "hint": "勾选要标记tmdbid标签的下载器，不选则不标记", "persistent-hint": True}}]},
                                        ]},
                                    ],
                                },
                            ]},
                        ],
                    },
                ],
            },
        ], {
            "enabled": False, "min_free_percent": 10,
            "delete_by_target": False, "target_free_percent": 20,
            "delete_count": 1, "check_interval": 6,
            "dry_run": False, "delete_same_size": False, "delete_seed_replicas": False, "delete_same_name_torrents": False, "notify": True,
            "media_cache_disabled": False, "pb_page": 1, "clean_downloader": [], "run_now": False,
            "pb_sort_by": "time", "pb_sort_desc": True, "pb_filter_watched": True, "watched_threshold": 85,
            "rss_on": False, "rss_cron": "*/30 * * * *", "rss_urls": "",
            "rss_dl": "", "rss_sz": "", "rss_inc": "", "rss_exc": "",
            "rss_once": False, "rss_ntf": True, "rss_th": 85, "rss_wash_mode": False, "rss_save_path": "",
            "tag_enabled": False, "tag_interval": 24, "tag_downloaders": [], "tag_run_now": False,
        }

    # ==================== 详情页（三区块平铺） ====================

    def get_page(self) -> Optional[List[dict]]:
        space_info = self._get_space_info()
        delete_history = self._get_delete_history()
        pb = self._get_playback_pb()
        cards = []

        # 使用提示
        cards.append({
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1 font-weight-bold"}, "text": "使用提示"},
                {"component": "VCardText", "content": [
                    {"component": "div", "text": "使用前需配置Webhooks，详见https://github.com/cudamin/MoviePilot-Plugins"},
                ]},
            ],
        })

        # 区块1：磁盘空间
        if space_info:
            total_gb = space_info["total_gb"]
            free_gb = space_info["free_gb"]
            used_gb = space_info["used_gb"]
            free_pct = space_info["free_percent"]
            bar_color = "error" if free_pct < self._min_free_percent else "warning" if free_pct < self._target_free_percent else "success"
            cards.append({
                "component": "VCard", "props": {"variant": "flat"},
                "content": [
                    {"component": "VCardTitle", "props": {"class": "d-flex align-center justify-space-between pa-3"}, "content": [
                        {"component": "div", "content": [
                            {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold"}, "text": "磁盘空间"},
                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": f"剩余 {free_gb:.1f} GB / 总计 {total_gb:.1f} GB"},
                        ]},
                        {"component": "VChip", "props": {"color": bar_color, "variant": "tonal", "size": "small"}, "text": f"剩余 {free_pct:.1f}%"},
                    ]},
                    {"component": "VCardText", "props": {"class": "pt-0"}, "content": [
                        {"component": "VProgressLinear", "props": {"modelValue": 100 - free_pct, "color": bar_color, "height": 12, "rounded": True, "class": "mb-4"}},
                        {"component": "VRow", "props": {"class": "ga-0"}, "content": [
                            {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 h-100"}, "content": [
                                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "总空间"},
                                {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": f"{total_gb:.1f}"},
                                {"component": "div", "props": {"class": "text-caption"}, "text": "GB"},
                            ]}]},
                            {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 h-100"}, "content": [
                                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "已用空间"},
                                {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": f"{used_gb:.1f}"},
                                {"component": "div", "props": {"class": "text-caption"}, "text": "GB"},
                            ]}]},
                            {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 h-100"}, "content": [
                                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "剩余空间"},
                                {"component": "div", "props": {"class": "text-h6 font-weight-bold text-success"}, "text": f"{free_gb:.1f}"},
                                {"component": "div", "props": {"class": "text-caption"}, "text": "GB"},
                            ]}]},
                            {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "pa-3 h-100"}, "content": [
                                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "剩余百分比"},
                                {"component": "div", "props": {"class": "text-h6 font-weight-bold"}, "text": f"{free_pct:.1f}%"},
                                {"component": "div", "props": {"class": "text-caption"}, "text": f"已用 {100-free_pct:.1f}%"},
                            ]}]},
                        ]},
                    ]},
                ],
            })

        # 区块2：删除历史
        hist_rows = []
        if delete_history:
            for h in reversed(delete_history[-50:]):
                hist_rows.append({"tr": [{"td": [{"text": h.get("time", "")}]}, {"td": [{"text": h.get("title", "")}]}, {"td": [{"text": h.get("action", "")}]}]})
        cards.append({
            "component": "VCard", "props": {"variant": "flat", "class": "mt-4"},
            "content": [
                {"component": "VCardTitle", "props": {}, "content": "删除历史记录（最近 50 条）"},
                {"component": "VCardText", "content": [{"component": "VTable", "props": {"density": "compact", "hover": True}, "content": {
                    "thead": [{"th": [{"text": "时间"}, {"text": "资源"}, {"text": "操作"}]}],
                    "tbody": hist_rows if hist_rows else [{"tr": [{"td": {"attrs": {"colspan": 3}, "content": [{"text": "暂无删除记录"}]}}]}],
                }}]},
            ],
        })

        # 区块3：播放记录 + 缓存管理
        all_items = []
        for r in pb:
            progress = r.get("p", 0) or 0
            watched = progress >= self._watched_threshold
            sn, en = r.get("s", 0) or 0, r.get("e", 0) or 0
            se_display = f"S{sn:02d}E{en:02d}" if sn > 0 and en > 0 else "电影"
            name = self._normalize_cached_name(r.get("n", ""), sn, en)
            title_clean = re.sub(r'\s+S\d{2}E\d{2}\s*.*$', '', name).strip() or name
            all_items.append({
                "k": r.get("k", ""),
                "title": title_clean,
                "se": se_display,
                "progress": progress,
                "watched": watched,
                "time": r.get("t", ""),
            })
        # 默认筛选已看完
        if self._pb_filter_watched:
            filtered_items = [x for x in all_items if x.get("watched")]
        else:
            filtered_items = list(all_items)
        # 排序
        if self._pb_sort_by == "title":
            filtered_items.sort(key=lambda x: x.get("title", ""), reverse=not self._pb_sort_desc)
        elif self._pb_sort_by == "status":
            filtered_items.sort(key=lambda x: (not x.get("watched"), x.get("title", "")), reverse=not self._pb_sort_desc)
        else:  # time
            filtered_items.sort(key=lambda x: x.get("time", ""), reverse=self._pb_sort_desc)
        watched_count = sum(1 for r in all_items if r.get("watched"))
        page_size = 10
        total_pages = max(1, (len(filtered_items) + page_size - 1) // page_size)
        page = min(max(1, self._pb_page), total_pages)
        if page != self._pb_page:
            self._pb_page = page
            self._update_config()
        page_items = filtered_items[(page - 1) * page_size: page * page_size]
        # 表头排序箭头
        def sort_arrow(field):
            if self._pb_sort_by == field:
                return " ↓" if self._pb_sort_desc else " ↑"
            return ""
        table_rows = [
            {"component": "div", "props": {"class": "d-flex align-center px-3 py-2 text-caption font-weight-bold bg-grey-lighten-4"}, "content": [
                {"component": "VBtn", "props": {"variant": "text", "size": "x-small", "color": "default", "class": "px-1 font-weight-bold", "style": "flex: 1 1 auto; min-width: 0; justify-content: flex-start; text-transform: none; letter-spacing: 0; font-size: inherit;"}, "text": "标题" + sort_arrow("title"),
                 "events": {"click": {"api": "plugin/SpaceCleaner/pb_sort", "method": "get", "params": {"sort_by": "title", "apikey": settings.API_TOKEN}}}},
                {"component": "div", "props": {"style": "width: 92px;"}, "text": "季集"},
                {"component": "div", "props": {"style": "width: 76px;"}, "text": "进度"},
                {"component": "VBtn", "props": {"variant": "text", "size": "x-small", "color": "default", "class": "px-1 font-weight-bold", "style": "width: 76px; justify-content: flex-start; text-transform: none; letter-spacing: 0; font-size: inherit;"}, "text": "状态" + sort_arrow("status"),
                 "events": {"click": {"api": "plugin/SpaceCleaner/pb_sort", "method": "get", "params": {"sort_by": "status", "apikey": settings.API_TOKEN}}}},
                {"component": "VBtn", "props": {"variant": "text", "size": "x-small", "color": "default", "class": "px-1 font-weight-bold", "style": "width: 150px; justify-content: flex-start; text-transform: none; letter-spacing: 0; font-size: inherit;"}, "text": "时间" + sort_arrow("time"),
                 "events": {"click": {"api": "plugin/SpaceCleaner/pb_sort", "method": "get", "params": {"sort_by": "time", "apikey": settings.API_TOKEN}}}},
                {"component": "div", "props": {"style": "width: 64px;"}, "text": "操作"},
            ]}
        ]
        if page_items:
            for item in page_items:
                table_rows.append({"component": "div", "props": {"class": "d-flex align-center px-3 py-2 border-t text-caption"}, "content": [
                    {"component": "div", "props": {"style": "flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"}, "text": item["title"]},
                    {"component": "div", "props": {"style": "width: 92px;"}, "text": item["se"]},
                    {"component": "div", "props": {"style": "width: 76px;"}, "text": f"{item['progress']:.1f}%"},
                    {"component": "div", "props": {"style": "width: 76px;"}, "text": "已看完" if item["watched"] else "未看完"},
                    {"component": "div", "props": {"style": "width: 150px;"}, "text": item["time"]},
                    {"component": "div", "props": {"style": "width: 64px;"}, "content": [
                        {"component": "VBtn", "props": {"color": "error", "variant": "text", "size": "small"},
                         "text": "删除",
                         "events": {"click": {"api": "plugin/SpaceCleaner/del_pb_item", "method": "get",
                                              "params": {"k": item["k"], "apikey": settings.API_TOKEN}}}},
                    ]},
                ]})
        else:
            table_rows.append({"component": "div", "props": {"class": "pa-4 text-center text-body-2 text-medium-emphasis"}, "text": "暂无播放缓存"})
        page_controls = [
            {"component": "VBtn", "props": {"variant": "tonal", "size": "small", "disabled": page <= 1}, "text": "上一页",
             "events": {"click": {"api": "plugin/SpaceCleaner/pb_page", "method": "get",
                                  "params": {"page": page - 1, "apikey": settings.API_TOKEN}}}},
            {"component": "VChip", "props": {"variant": "tonal", "size": "small", "class": "mx-2"}, "text": f"第 {page}/{total_pages} 页"},
            {"component": "VBtn", "props": {"variant": "tonal", "size": "small", "disabled": page >= total_pages}, "text": "下一页",
             "events": {"click": {"api": "plugin/SpaceCleaner/pb_page", "method": "get",
                                  "params": {"page": page + 1, "apikey": settings.API_TOKEN}}}},
        ]
        cards.append({
            "component": "VCard", "props": {"variant": "flat", "class": "mt-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "d-flex align-center justify-space-between pa-3"}, "content": [
                    {"component": "div", "content": [
                        {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold"}, "text": "播放缓存"},
                        {"component": "div", "props": {"class": "text-caption text-medium-emphasis"},
                         "text": f"共 {len(all_items)} 条，已看完 {watched_count} 条，未看完 {len(all_items)-watched_count} 条（当前显示 {len(filtered_items)} 条）"},
                    ]},
                    {"component": "div", "props": {"class": "d-flex align-center"}, "content": [
                        {"component": "VBtn", "props": {"variant": self._pb_filter_watched and "flat" or "text", "size": "x-small", "color": self._pb_filter_watched and "primary" or "default", "class": "mr-2"},
                         "text": "仅已看完",
                         "events": {"click": {"api": "plugin/SpaceCleaner/pb_filter_toggle", "method": "get", "params": {"apikey": settings.API_TOKEN}}}},
                        {"component": "VBtn", "props": {"color": "error", "variant": "tonal", "size": "small", "disabled": not bool(pb)},
                         "text": "清除全部",
                         "events": {"click": {"api": "plugin/SpaceCleaner/clear_pb", "method": "get",
                                              "params": {"apikey": settings.API_TOKEN}}}},
                    ]},
                ]},
                {"component": "VCardText", "props": {"class": "pa-0"}, "content": [
                    {"component": "div", "props": {"class": "overflow-x-auto"}, "content": table_rows},
                    {"component": "div", "props": {"class": "d-flex align-center justify-end pa-3 pr-10", "style": "padding-right: 56px !important;"}, "content": page_controls},
                ]},
            ],
        })
        return cards

    # ==================== 缓存管理 ====================

    def _invalidate_caches(self) -> None:
        self._cached_space_info = None
        self._cached_space_time = 0
        self._all_torrents_cache = None
        self._all_torrents_cache_time = 0
        self._pb_cache = None
        self._pb_cache_time = 0

    def _get_cached_space_info(self) -> Optional[Dict[str, float]]:
        now = time.time()
        if self._cached_space_info is not None and now - self._cached_space_time < self._space_cache_ttl:
            return self._cached_space_info
        info = self._get_space_info()
        if info:
            self._cached_space_info = info
            self._cached_space_time = now
        return info

    def _get_cached_torrents(self, chain: ChainBase) -> List[Any]:
        now = time.time()
        if self._all_torrents_cache is not None and now - self._all_torrents_cache_time < self._torrents_cache_ttl:
            return self._all_torrents_cache
        downloaders = self._clean_downloader or [None]
        all_t = []
        for dl in downloaders:
            t = chain.list_torrents(downloader=dl or None, include_all_tags=True) or []
            all_t.extend(t)
        self._all_torrents_cache = all_t
        self._all_torrents_cache_time = now
        return all_t

    def stop_service(self) -> None:
        self._scheduler_running = False
        if self._scheduler_event:
            self._scheduler_event.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=3)
        self._scheduler_thread = None
        self._scheduler_event = None
        self._tag_scheduler_running = False
        if self._tag_scheduler_event:
            self._tag_scheduler_event.set()
        if self._tag_scheduler_thread and self._tag_scheduler_thread.is_alive():
            self._tag_scheduler_thread.join(timeout=3)
        self._tag_scheduler_thread = None
        self._tag_scheduler_event = None
        self._stop_rss_scheduler()
        self._invalidate_caches()

    def _stop_rss_scheduler(self):
        if self._rss_s:
            try:
                self._rss_s.shutdown(wait=False)
            except Exception:
                pass
            self._rss_s = None

    # ==================== 空间清理调度 ====================

    def _start_scheduler(self) -> None:
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._scheduler_running = True
        self._scheduler_event = threading.Event()
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True, name="SC-Scheduler")
        self._scheduler_thread.start()

    def _scheduler_loop(self) -> None:
        interval_seconds = self._check_interval * 3600
        self._scheduler_event.wait(interval_seconds)
        while self._scheduler_running:
            try:
                self._check_and_clean()
            except Exception as e:
                logger.error(f"SC检查异常: {str(e)}")
            self._scheduler_event.clear()
            if self._scheduler_event.wait(interval_seconds):
                break

    def _run_now_task(self) -> None:
        logger.info("SC 开始立即清理...")
        try:
            self._check_and_clean()
            logger.info("SC 立即清理完成")
        except Exception as e:
            logger.error(f"SC 立即清理失败: {str(e)}")

    # ==================== 空间清理核心 ====================

    def _get_chain(self) -> ChainBase:
        if not self._chain:
            self._chain = ChainBase()
        return self._chain

    def _get_playback_pb(self) -> List[Dict[str, Any]]:
        if self._media_cache_disabled:
            return []
        now = time.time()
        if self._pb_cache is not None and now - self._pb_cache_time < self._pb_cache_ttl:
            return self._pb_cache
        with self._pb_lock:
            pb_copy = list(self._pb)
        self._pb_cache = pb_copy
        self._pb_cache_time = now
        return pb_copy

    def _is_watched_pb(self, tmdbid: int, season: Optional[int] = None, episode: Optional[int] = None) -> bool:
        """判断指定剧集是否已看完（播放进度 >= 标记已看阈值）。"""
        pb = self._get_playback_pb()
        if not pb:
            return False
        if season and episode:
            k = f"{tmdbid}:S{season:02d}E{episode:02d}"
            for r in pb:
                if r.get("k") == k:
                    return (r.get("p", 0) or 0) >= self._watched_threshold
        return False

    def _is_watched_pb_by_record(self, record: TransferHistory) -> bool:
        """根据转移记录判断是否已看完（播放进度 >= 标记已看阈值）。"""
        if not record.tmdbid:
            return False
        tmdb = record.tmdbid
        pb = self._get_playback_pb()
        # 电影：查 {tmdbid}:M
        if record.type != "电视剧":
            k = f"{tmdb}:M"
            for r in pb:
                if r.get("k") == k:
                    return (r.get("p", 0) or 0) >= self._watched_threshold
            return False
        season_num = None
        if record.seasons:
            s = record.seasons.strip().upper().replace("S", "")
            if s.isdigit():
                season_num = int(s)
        episodes_str = (record.episodes or "").strip().upper().replace("E", "")
        if not season_num:
            return False

        # 单集
        if episodes_str.isdigit():
            return self._is_watched_pb(tmdb, season_num, int(episodes_str))

        # 多集范围
        eps = []
        if "-" in episodes_str or "~" in episodes_str:
            sep = "-" if "-" in episodes_str else "~"
            parts = episodes_str.split(sep)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                eps = list(range(int(parts[0]), int(parts[1]) + 1))
        elif "," in episodes_str:
            eps = [int(p) for p in episodes_str.split(",") if p.strip().isdigit()]
        if eps:
            return self._is_watched_pb(tmdb, season_num, max(eps))

        # 整季包（无具体集号）：查该季最大缓存集号
        pb = self._get_playback_pb()
        max_ep = 0
        prefix = f"{tmdb}:S{season_num:02d}E"
        for r in pb:
            if r.get("k", "").startswith(prefix):
                ep = r.get("e", 0) or 0
                if ep > max_ep:
                    max_ep = ep
        if max_ep > 0:
            return self._is_watched_pb(tmdb, season_num, max_ep)
        return False

    def _get_space_info(self) -> Optional[Dict[str, float]]:
        try:
            download_dirs, library_dirs = [], []
            for d in (self.systemconfig.get("DownloadDirectories") or []):
                if isinstance(d, dict) and d.get("path"):
                    download_dirs.append(Path(d["path"]))
            for d in (self.systemconfig.get("LibraryDirectories") or []):
                if isinstance(d, dict) and d.get("path"):
                    library_dirs.append(Path(d["path"]))
            all_dirs = download_dirs + library_dirs or [settings.CONFIG_PATH]
            total_space, free_space = SystemUtils.space_usage(all_dirs)
            if total_space == 0:
                return None
            free_percent = (free_space / total_space) * 100
            return {"total_gb": total_space / (1024 ** 3), "free_gb": free_space / (1024 ** 3),
                    "used_gb": (total_space - free_space) / (1024 ** 3), "free_percent": free_percent}
        except Exception as e:
            logger.error(f"获取磁盘空间失败: {str(e)}")
            return None

    def _check_and_clean(self) -> None:
        if self._running:
            return
        self._running = True
        try:
            si = self._get_cached_space_info()
            if not si:
                return
            free_pct = si["free_percent"]
            if free_pct >= self._min_free_percent:
                logger.info(f"空间充足 {free_pct:.1f}% >= {self._min_free_percent}%，跳过")
                return
            self._clean_resources(si)
        finally:
            self._running = False
            self._invalidate_caches()

    def _clean_resources(self, space_info: Dict[str, float]) -> None:
        chain = self._get_chain()
        dc = 0
        md = self._delete_count if not self._delete_by_target else 0
        # 试运行模式不需要种子列表
        all_torrents = None if self._dry_run else self._get_cached_torrents(chain)
        from app.db import ScopedSession
        from sqlalchemy import asc
        sess = ScopedSession()
        fr = ""
        try:
            offset = 0
            # 按 download_hash 分组：收集每组所有记录的(tmdbid, season, episodes)
            hash_groups: Dict[str, List[TransferHistory]] = {}
            no_hash_records = []
            while True:
                recs = sess.query(TransferHistory).filter(TransferHistory.status == True).order_by(asc(TransferHistory.id)).offset(offset).limit(25).all()
                if not recs:
                    break
                for r in recs:
                    if r.download_hash:
                        hash_groups.setdefault(r.download_hash, []).append(r)
                    elif r.type == "电视剧":
                        no_hash_records.append(r)
                    elif r.type != "电视剧" and r.tmdbid:
                        # 电影无 hash：直接检查 pb 中的 {tmdbid}:M
                        no_hash_records.append(r)
                offset += len(recs)

            pb = self._get_playback_pb()

            # 检查每个下载任务（含电视剧和电影）：同 hash 取最后一集判断是否已看完
            all_ready: List[TransferHistory] = []
            for hash_id, recs in hash_groups.items():
                # 取该 hash 下最后一条记录作为代表
                check_rec = recs[-1]
                if check_rec.type == "电视剧":
                    # 电视剧：按集号排序取最后一集检查 pb
                    recs.sort(key=lambda x: max(int(ep) for ep in re.findall(r'\d+', x.episodes or "0")) if re.findall(r'\d+', x.episodes or "0") else 0, reverse=True)
                    check_rec = recs[0]
                    season_num = None
                    if check_rec.seasons:
                        s = check_rec.seasons.strip().upper().replace("S", "")
                        if s.isdigit():
                            season_num = int(s)
                    episodes_str = (check_rec.episodes or "").strip().upper().replace("E", "")
                    if season_num and episodes_str:
                        eps = re.findall(r'\d+', episodes_str)
                        if eps:
                            check_ep = max(int(e) for e in eps)
                            k = f"{check_rec.tmdbid}:S{season_num:02d}E{check_ep:02d}"
                        watched = False
                        for p in pb:
                            if p.get("k") == k:
                                pct = p.get("p", 0) or 0
                                if pct >= self._watched_threshold:
                                    watched = True
                                break
                        if watched:
                            all_ready.append(check_rec)
                else:
                    # 电影：检查 pb 中的 {tmdbid}:M
                    k = f"{check_rec.tmdbid}:M"
                    watched = False
                    for p in pb:
                        if p.get("k") == k:
                            pct = p.get("p", 0) or 0
                            if pct >= self._watched_threshold:
                                watched = True
                            break
                    if watched:
                        all_ready.append(check_rec)

            # 无 download_hash 的电视剧：按 tmdbid+season 分组
            no_hash_groups: Dict[str, List[TransferHistory]] = {}
            for r in no_hash_records:
                if r.type != "电视剧" or not r.tmdbid or not r.seasons:
                    continue
                s = r.seasons.strip().upper().replace("S", "")
                if s.isdigit():
                    key = f"{r.tmdbid}:S{int(s):02d}"
                    no_hash_groups.setdefault(key, []).append(r)
            for key, recs in no_hash_groups.items():
                recs.sort(key=lambda x: max(int(ep) for ep in re.findall(r'\d+', x.episodes or "0")) if re.findall(r'\d+', x.episodes or "0") else 0, reverse=True)
                check_rec = recs[0]
                season_num = None
                if check_rec.seasons:
                    s = check_rec.seasons.strip().upper().replace("S", "")
                    if s.isdigit():
                        season_num = int(s)
                episodes_str = (check_rec.episodes or "").strip().upper().replace("E", "")
                if season_num and episodes_str:
                    eps = re.findall(r'\d+', episodes_str)
                    if eps:
                        check_ep = max(int(e) for e in eps)
                        k = f"{check_rec.tmdbid}:S{season_num:02d}E{check_ep:02d}"
                    watched = False
                    for p in pb:
                        if p.get("k") == k:
                            pct = p.get("p", 0) or 0
                            if pct >= self._watched_threshold:
                                watched = True
                            break
                    if watched:
                        all_ready.append(check_rec)

            # 电影无 hash：检查 pb 中的 {tmdbid}:M
            for r in no_hash_records:
                if r.type == "电视剧":
                    continue
                if not r.tmdbid:
                    continue
                k = f"{r.tmdbid}:M"
                for p in pb:
                    if p.get("k") == k:
                        pct = p.get("p", 0) or 0
                        if pct >= self._watched_threshold:
                            all_ready.append(r)
                        break

            # 按播放缓存时间升序处理（最早缓存的优先删除）
            pb_times = {p.get("k"): p.get("t", "9999") for p in pb}
            def get_sort_time(r):
                if r.type != "电视剧":
                    return pb_times.get(f"{r.tmdbid}:M", "9999")
                # 电视剧：尝试获取其最后一集的缓存时间
                # 逻辑与 _is_watched_pb 一致
                s = r.seasons.strip().upper().replace("S", "") if r.seasons else ""
                e_str = (r.episodes or "").strip().upper().replace("E", "")
                if s.isdigit() and e_str:
                    eps = re.findall(r'\d+', e_str)
                    if eps:
                        k = f"{r.tmdbid}:S{int(s):02d}E{max(int(e) for e in eps):02d}"
                        return pb_times.get(k, "9999")
                return "9999"

            if not all_ready:
                logger.info("SC 未发现满足删除条件的资源（已看完且在转移历史中）")
                return

            all_ready.sort(key=get_sort_time)
            for r in all_ready:
                if not self._delete_by_target and dc >= md:
                    fr = "limit"
                    break
                cs = self._get_cached_space_info()
                if cs:
                    if self._delete_by_target and cs["free_percent"] >= self._target_free_percent:
                        logger.info(f"SC 空间已达到目标阈值 {self._target_free_percent}% (当前 {cs['free_percent']:.1f}%)，停止清理")
                        fr = "space_ok"
                        break
                    if not self._delete_by_target and cs["free_percent"] >= self._min_free_percent:
                        logger.info(f"SC 空间已恢复至触发阈值 {self._min_free_percent}% (当前 {cs['free_percent']:.1f}%)，停止清理")
                        fr = "space_ok"
                        break
                self._delete_resource(r, chain, cs or space_info, all_torrents)
                dc += 1
        finally:
            sess.close()
        if fr:
            return
        logger.info(f"SC 清理完成，删除 {dc} 个资源")

    def _delete_resource(self, record, chain, space_info, all_torrents=None):
        """删除单个转移记录对应的资源，避免跨 Session 访问 ORM 懒加载属性。"""
        record_id = record.id
        title = record.title or "未知"
        media_type = record.type or ""
        seasons = record.seasons or ""
        episodes = record.episodes or ""
        src = record.src or ""
        dest = record.dest or ""
        tmdbid = record.tmdbid
        download_hash = record.download_hash or ""
        display_name = title if media_type != "电视剧" else f"{title} {seasons} {episodes}".strip()
        if self._dry_run:
            logger.info(f"【试运行】将删除: {display_name}")
            self._add_delete_history(display_name, "试运行 - 将删除")
            return
        try:
            if src:
                p = Path(src)
                if p.exists():
                    self._safe_delete_path(p)
                # 删除下载目录
                pp = p.parent
                if pp.exists():
                    self._safe_delete_path(pp)
            if dest:
                p = Path(dest)
                if p.exists():
                    self._safe_delete_path(p)
                # 删除媒体库目录
                pp = p.parent
                if pp.exists():
                    self._safe_delete_path(pp)
            # 从下载器中删除种子（同时删除文件）
            self._delete_downloader_torrents(chain, download_hash, display_name, all_torrents)
            # 删除同 tmdbid 标签的种子：下载器中同 tmdbid 标签的所有种子
            if self._delete_seed_replicas and tmdbid:
                self._delete_linked_torrents(chain, tmdbid, all_torrents=all_torrents)
            # 用独立 session 删除转移记录，避免跨 session 操作
            from app.db import ScopedSession
            ds = ScopedSession()
            try:
                r = ds.query(TransferHistory).filter(TransferHistory.id == record_id).first()
                if r:
                    ds.delete(r)
                    ds.commit()
            finally:
                ds.close()
            # 从 pb 缓存中删除该 tmdbid 对应的条目
            self._delete_pb_by_tmdbid(tmdbid)
            self._add_delete_history(display_name, "已删除")
            if self._notify:
                self.post_message(title="空间清理器 - 资源已删除",
                                  text=f"资源: {display_name}\n当前剩余空间: {space_info['free_gb']:.2f} GB ({space_info['free_percent']:.1f}%)")
        except Exception as e:
            logger.error(f"删除 {display_name} 失败: {str(e)}")
            self._add_delete_history(display_name, f"删除失败: {str(e)}")

    def _clean_tmdbid_records(self, tmdbid: int):
        """删除指定 tmdbid 对应的所有转移记录、源文件和媒体库文件。"""
        try:
            from app.db import ScopedSession
            sess = ScopedSession()
            try:
                recs = sess.query(TransferHistory).filter(
                    TransferHistory.tmdbid == tmdbid,
                    TransferHistory.status == True
                ).all()
                deleted_count = 0
                for r in recs:
                    if r.src:
                        p = Path(r.src)
                        if p.exists():
                            self._safe_delete_path(p)
                        pp = p.parent
                        if pp.exists():
                            self._safe_delete_path(pp)
                    if r.dest:
                        p = Path(r.dest)
                        if p.exists():
                            self._safe_delete_path(p)
                        pp = p.parent
                        if pp.exists():
                            self._safe_delete_path(pp)
                    sess.delete(r)
                    deleted_count += 1
                sess.commit()
                if deleted_count:
                    logger.info(f"联动清理 tmdbid={tmdbid} 共 {deleted_count} 条记录")
                    self._add_delete_history(f"tmdbid={tmdbid}", f"联动删除 {deleted_count} 条记录")
            finally:
                sess.close()
        except Exception as e:
            logger.error(f"联动清理 tmdbid={tmdbid} 失败: {str(e)}")

    def _delete_pb_by_tmdbid(self, tmdbid: Optional[int]):
        """从 pb 缓存中删除指定 tmdbid 的所有条目。"""
        if not tmdbid:
            return
        with self._pb_lock:
            before = len(self._pb)
            prefix = f"{tmdbid}:"
            self._pb = [r for r in self._pb if not r.get("k", "").startswith(prefix)]
            after = len(self._pb)
        if before != after:
            self.save_data("pb", self._pb)
            logger.info(f"从 pb 缓存删除 tmdbid={tmdbid} 共 {before - after} 条")

    def _delete_downloader_torrents(self, chain, download_hash, display_name, all_torrents=None):
        """根据 download_hash 从下载器中删除种子及文件。"""
        if not download_hash:
            logger.warning(f"无 download_hash，跳过删种: {display_name}")
            return
        try:
            chain.remove_torrents(hashs=download_hash, delete_file=True)
            logger.info(f"已从下载器删除种子: {display_name} ({download_hash})")
        except Exception as e:
            logger.error(f"从下载器删除种子失败 {display_name}: {str(e)}")
        # 删种同名种子：检索下载器中所有与被删种子同名的种子并删除
        if self._delete_same_name_torrents:
            self._delete_same_name_torrents_in_downloader(chain, download_hash, display_name)

    def _delete_linked_torrents(self, chain, tmdbid, all_torrents=None):
        """删除下载器中带有指定 tmdbid 标签的所有种子及文件。"""
        if not tmdbid or not all_torrents:
            return
        tag_pattern = re.compile(rf'tmdbid\s*=\s*{tmdbid}\b')
        for t in all_torrents:
            tags = getattr(t, "tags", "") or ""
            if not tag_pattern.search(tags):
                continue
            t_name = (getattr(t, "title", None) or getattr(t, "name", None) or "").strip()
            try:
                chain.remove_torrents(hashs=t.hash, delete_file=True)
                logger.info(f"联动删除种子及文件 (tmdbid={tmdbid}): {t_name}")
            except Exception:
                pass

    def _delete_same_name_torrents_in_downloader(self, chain, download_hash, display_name):
        """使用 include_all_tags=True 检索下载器，删除所有与被删种子同名的种子。"""
        try:
            downloaders = self._clean_downloader or [None]
            for dl in downloaders:
                all_t = chain.list_torrents(downloader=dl or None, include_all_tags=True) or []
                for t in all_t:
                    t_name = (getattr(t, "title", None) or getattr(t, "name", None) or "").strip()
                    if not t_name or t.hash == download_hash:
                        continue
                    # 比较显示名称（忽略大小写）
                    if t_name.lower() == display_name.lower():
                        try:
                            chain.remove_torrents(hashs=t.hash, delete_file=True)
                            logger.info(f"已删除同名种子: {t_name} ({t.hash})")
                        except Exception as e:
                            logger.error(f"删除同名种子失败 {t_name}: {str(e)}")
        except Exception as e:
            logger.error(f"检索同名种子失败: {str(e)}")

    def _safe_delete_path(self, path: Path):
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass

    @staticmethod
    def _is_dir_empty(path: Path) -> bool:
        try:
            return not any(path.iterdir())
        except Exception:
            return False

    @staticmethod
    def _parse_episode_info(record: TransferHistory) -> Tuple[Optional[bool], Optional[int], List[int]]:
        """解析转移记录中的剧集信息，返回 (is_single_episode, season_num, episode_numbers)。"""
        season_num = None
        if record.seasons:
            s = record.seasons.strip().upper().replace("S", "")
            if s.isdigit():
                season_num = int(s)
        episodes_str = (record.episodes or "").strip().upper().replace("E", "")
        if not episodes_str:
            return False, season_num, []
        if "-" in episodes_str or "~" in episodes_str:
            sep = "-" if "-" in episodes_str else "~"
            parts = episodes_str.split(sep)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                return False, season_num, list(range(int(parts[0]), int(parts[1]) + 1))
            return False, season_num, []
        if "," in episodes_str:
            parts = [p.strip() for p in episodes_str.split(",") if p.strip().isdigit()]
            if parts:
                return False, season_num, [int(p) for p in parts]
            return False, season_num, []
        if episodes_str.isdigit():
            return True, season_num, [int(episodes_str)]
        return None, season_num, []

    def _add_delete_history(self, title: str, action: str):
        h = self.get_data("delete_history") or []
        h.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "title": title, "action": action})
        if len(h) > 200:
            h = h[-200:]
        self.save_data("delete_history", h)

    def _get_delete_history(self) -> List[Dict[str, str]]:
        return self.get_data("delete_history") or []

    # ==================== API ====================

    def api_dry_run(self):
        si = self._get_space_info()
        if not si:
            return {"success": False, "message": "无法获取磁盘空间"}
        if si["free_percent"] >= self._min_free_percent:
            return {"success": True, "space_info": {**si, "threshold_percent": self._min_free_percent, "needs_cleanup": False}, "would_delete": [], "message": "空间充足"}
        from app.db import ScopedSession
        from sqlalchemy import asc
        sess = ScopedSession()
        try:
            recs = sess.query(TransferHistory).filter(TransferHistory.status == True).order_by(asc(TransferHistory.id)).limit(200).all()
            wd = []
            for r in recs:
                if len(wd) >= self._delete_count:
                    break
                if r.type == "电视剧" and self._is_watched_pb_by_record(r):
                    wd.append({"id": r.id, "title": r.title or "", "type": r.type or "", "seasons": r.seasons or "", "episodes": r.episodes or "", "date": r.date or ""})
        finally:
            sess.close()
        return {"success": True, "space_info": {**si, "threshold_percent": self._min_free_percent, "needs_cleanup": True}, "would_delete": wd, "message": f"试运行完成，将删除 {len(wd)} 个资源"}

    def api_run_now(self):
        try:
            self._check_and_clean()
            return {"success": True, "message": "清理完成"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def api_delete_history(self):
        return {"success": True, "history": self._get_delete_history()}

    def api_space_info(self):
        si = self._get_space_info()
        if not si:
            return {"success": False, "message": "无法获取磁盘空间"}
        return {"success": True, "space_info": {**si, "threshold_percent": self._min_free_percent, "needs_cleanup": si["free_percent"] < self._min_free_percent}}

    # ==================== RSS 下载 ====================

    def _rss_run(self):
        if self._rss_busy or not self._rss_urls:
            return
        urls = [u.strip() for u in self._rss_urls.split("\n") if u.strip()]
        if not urls:
            return
        logger.info("SC-RSS 开始运行...")
        self._rss_busy = True
        try:
            if self._rss_wash_mode:
                # 洗版模式：先收集所有 URL 的条目，统一去重后再下载
                self._rss_run_dedup(urls)
            else:
                # 普通模式：逐个 URL 处理
                for url in urls:
                    try:
                        self._rss_proc(url)
                    except Exception as e:
                        logger.error(f"RSS url err {url} {e}")
        finally:
            self._rss_busy = False
        logger.info("SC-RSS 运行完成")

    def _rss_run_dedup(self, urls: List[str]):
        """洗版模式：收集所有 URL 的 RSS 条目，播放进度低于阈值时触发洗版，只下载最早版本。"""
        from collections import OrderedDict
        all_candidates = OrderedDict()  # dedup_key -> (item, m, meta, s_season, se_fmt, url)
        total_items = 0
        for url in urls:
            items = RssHelper().parse(url)
            if not items:
                logger.info(f"SC-RSS 未获取到新报文: {url}")
                continue
            total_items += len(items)
            for item in items:
                t = item.get("title", "")
                e = item.get("enclosure", "") or item.get("link", "")
                if not t or not e:
                    continue
                with self._rss_lk:
                    if e in self._rss_seen:
                        continue
                    self._rss_seen.add(e)
                if self._rss_inc and not re.search(self._rss_inc, t, re.IGNORECASE):
                    continue
                if self._rss_exc and re.search(self._rss_exc, t, re.IGNORECASE):
                    continue
                if self._rss_sz:
                    sz = item.get("size", 0) or 0
                    if sz > 0:
                        lo, hi = 0, float("inf")
                        p = self._rss_sz.split("-")
                        try:
                            if len(p) >= 1 and p[0]:
                                lo = float(p[0])
                            if len(p) >= 2 and p[1]:
                                hi = float(p[1])
                        except ValueError:
                            pass
                        gb = sz / (1024 ** 3)
                        if gb < lo or gb > hi:
                            continue
                m, meta = self._rss_id(item, t)
                if not m or not meta:
                    self._rss_log("识别失败", t)
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS识别失败",
                                          text=f"资源无法识别: {t}")
                    continue
                cr = self._rss_ck(m, meta)
                s_season = m.season or meta.begin_season or 1
                se_fmt = f"S{int(s_season):02d}E{int(meta.begin_episode or 0):02d}" if meta.begin_episode else ""
                # 洗版模式：播放进度低于阈值才触发洗版
                if cr["s"]:
                    # 已看完（进度 >= 阈值），跳过
                    self._rss_log("跳过", m.title, cr["r"])
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS跳过",
                                          text=f"{m.title} {se_fmt} {cr['r']}")
                    continue
                # 构造去重 key：优先用 tmdb_id+season+episode，缺字段时用 enclosure
                if m.tmdb_id and meta.begin_episode:
                    dedup_key = (m.tmdb_id, int(s_season), int(meta.begin_episode))
                elif m.tmdb_id:
                    dedup_key = ("tmdb", m.tmdb_id, int(s_season))
                else:
                    dedup_key = ("enclosure", item.get("enclosure", "") or item.get("link", ""))
                if dedup_key in all_candidates:
                    self._rss_log("洗版去重跳过", m.title, f"{se_fmt} 已有更早版本")
                    continue
                all_candidates[dedup_key] = (item, m, meta, s_season, se_fmt)
        logger.info(f"SC-RSS 报文处理完成：获取 {total_items} 条，过滤后剩余 {len(all_candidates)} 条待处理")
        # 统一下载去重后的条目
        dc = 0
        for dedup_key, payload in all_candidates.items():
            item, m, meta, s_season, se_fmt = payload
            if self._rss_dl_add(item, m, meta):
                dc += 1
                self._rss_log("下载", m.title)
                if self._rss_ntf:
                    self.post_message(title="SC-RSS",
                                      text=f"已添加下载: {m.title} {se_fmt}")
            else:
                self._rss_log("下载失败", item.get("title", ""), "推送下载器失败")
                if self._rss_ntf:
                    self.post_message(title="SC-RSS",
                                      text=f"添加种子任务失败: {m.title} {se_fmt}")
        s = list(self._rss_seen)
        if len(s) > 2000:
            s = s[-2000:]
            self._rss_seen = set(s)
        self.save_data("rss_seen", s)

    def _rss_proc(self, url: str):
        items = RssHelper().parse(url)
        if not items:
            logger.info(f"SC-RSS 未获取到新报文: {url}")
            return
        url_new = 0
        url_filtered = 0
        # 第一遍：识别所有条目，按 (tmdb_id, season, episode) 分组，每组只保留最早出现的条目
        seen_dedup = {}  # dedup_key -> (item, m, meta, s_season, se_fmt)
        for item in items:
            t = item.get("title", "")
            e = item.get("enclosure", "") or item.get("link", "")
            if not t or not e:
                continue
            with self._rss_lk:
                if e in self._rss_seen:
                    continue
                self._rss_seen.add(e)
            url_new += 1
            if self._rss_inc and not re.search(self._rss_inc, t, re.IGNORECASE):
                continue
            if self._rss_exc and re.search(self._rss_exc, t, re.IGNORECASE):
                continue
            if self._rss_sz:
                sz = item.get("size", 0) or 0
                if sz > 0:
                    lo, hi = 0, float("inf")
                    p = self._rss_sz.split("-")
                    try:
                        if len(p) >= 1 and p[0]:
                            lo = float(p[0])
                        if len(p) >= 2 and p[1]:
                            hi = float(p[1])
                    except ValueError:
                        pass
                    gb = sz / (1024 ** 3)
                    if gb < lo or gb > hi:
                        continue
            url_filtered += 1
            m, meta = self._rss_id(item, t)
            if not m or not meta:
                self._rss_log("识别失败", t)
                if self._rss_ntf:
                    self.post_message(title="SC-RSS识别失败",
                                      text=f"资源无法识别: {t}")
                continue
            cr = self._rss_ck(m, meta)
            s_season = m.season or meta.begin_season or 1
            se_fmt = f"S{int(s_season):02d}E{int(meta.begin_episode or 0):02d}" if meta.begin_episode else ""
            if cr["s"]:
                sc = 1
                self._rss_log("跳过", m.title, cr["r"])
                if self._rss_ntf:
                    self.post_message(title="SC-RSS跳过",
                                      text=f"{m.title} {se_fmt} {cr['r']}")
                continue
            # 洗版模式：播放进度低于阈值时触发洗版，同一 (tmdb_id, season, episode) 只保留 RSS feed 中最早出现的条目
            if self._rss_wash_mode:
                # 构造去重 key：优先用 tmdb_id+season+episode，缺字段时用 enclosure
                if m.tmdb_id and meta.begin_episode:
                    dedup_key = (m.tmdb_id, int(s_season), int(meta.begin_episode))
                elif m.tmdb_id:
                    dedup_key = ("tmdb", m.tmdb_id, int(s_season))
                else:
                    dedup_key = ("enclosure", item.get("enclosure", "") or item.get("link", ""))
                if dedup_key in seen_dedup:
                    self._rss_log("洗版去重跳过", m.title, f"{se_fmt} 已有更早版本")
                    continue
                seen_dedup[dedup_key] = (item, m, meta, s_season, se_fmt)
            else:
                # 开关关闭或无法去重的条目直接下载
                if self._rss_dl_add(item, m, meta):
                    self._rss_log("下载", m.title)
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS",
                                          text=f"已添加下载: {m.title} {se_fmt}")
                else:
                    self._rss_log("下载失败", t, "推送下载器失败")
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS",
                                          text=f"添加种子任务失败: {m.title} {se_fmt}")
        if self._rss_wash_mode:
            for dedup_key, payload in seen_dedup.items():
                item, m, meta, s_season, se_fmt = payload
                if self._rss_dl_add(item, m, meta):
                    self._rss_log("下载", m.title)
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS",
                                          text=f"已添加下载: {m.title} {se_fmt}")
                else:
                    self._rss_log("下载失败", item.get("title", ""), "推送下载器失败")
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS",
                                          text=f"添加种子任务失败: {m.title} {se_fmt}")
        logger.info(f"SC-RSS [{url}] 获取到 {url_new} 个新报文，过滤后剩余 {url_filtered} 个")
        s = list(self._rss_seen)
        if len(s) > 2000:
            s = s[-2000:]
            self._rss_seen = set(s)
        self.save_data("rss_seen", s)

    def _rss_ck(self, m, meta: MetaInfo) -> dict:
        if self._media_cache_disabled:
            return {"s": False, "r": "媒体缓存关闭"}
        if not m.tmdb_id:
            return {"s": False, "r": "no tmdb"}
        # 优先使用 MediaInfo 的 season（TMDB 识别结果），fallback 到 MetaInfo 解析结果
        s = m.season or meta.begin_season
        e = meta.begin_episode
        if not e:
            return {"s": False, "r": "no ep"}
        if not s:
            s = 1
        k = f"{m.tmdb_id}:S{int(s):02d}E{int(e):02d}"
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    p = r.get("p", 0) or 0
                    if self._rss_wash_mode:
                        # 洗版模式：进度 >= 阈值视为已看完（跳过），低于阈值触发洗版
                        return {"s": p >= self._rss_th, "r": f"{'≥' if p>=self._rss_th else '<'}{self._rss_th}%({p:.1f}%)"}
                    else:
                        # 非洗版模式：不跳过
                        return {"s": False, "r": f"{p:.1f}%(洗版关闭)"}
            return {"s": False, "r": "无记录(触发洗版)"}

    def _rss_id(self, item: dict, rt: str):
        enc = item.get("enclosure", "") or item.get("link", "")
        fns = self._rss_fnames(enc)
        if fns:
            for fn in fns:
                try:
                    meta = MetaInfo(title=fn)
                    if meta.name and (media := self.chain.recognize_media(meta=meta, cache=True)):
                        return media, meta
                except Exception:
                    continue
        ct = re.sub(r'\[.*?\]', "", rt).strip()
        for c in [x.strip() for x in ct.split("/") if x.strip()] or [ct]:
            meta = MetaInfo(title=c, subtitle=item.get("description", ""))
            if meta.name and (media := self.chain.recognize_media(meta=meta, cache=True)):
                return media, meta
        return None, None

    @staticmethod
    def _rss_fnames(enc: str) -> List[str]:
        if not enc:
            return []
        try:
            import bencode

            r = RequestUtils(timeout=30).get_res(enc)
            if not r or r.status_code != 200:
                return []
            t = bencode.bdecode(r.content)
            info = t.get("info", {})
            files = info.get("files", [])
            if files:
                res = []
                for f in files:
                    parts = [p.decode("utf-8", errors="replace") if isinstance(p, bytes) else str(p) for p in f.get("path", [])]
                    if parts:
                        res.append("/".join(parts))
                return res
            name = info.get("name", "")
            if isinstance(name, bytes):
                name = name.decode("utf-8", errors="replace")
            return [name] if name else []
        except Exception:
            return []

    def _rss_dl_add(self, item: dict, m, meta: MetaInfo) -> bool:
        try:
            enc = item.get("enclosure", "") or item.get("link", "")
            if not enc:
                return False
            ti = TorrentInfo(title=item.get("title", ""), description="",
                             enclosure=enc, page_url=item.get("link", ""), size=item.get("size", 0))
            ctx = Context(meta_info=meta, media_info=m, torrent_info=ti)
            result = DownloadChain().download_single(
                context=ctx, downloader=self._rss_dl or None,
                save_path=self._rss_save_path or None,
                username="SC-RSS", return_detail=True)
            if isinstance(result, tuple):
                h, err = result
                if h:
                    return True
                if err:
                    logger.warn(f"SC-RSS 下载失败: {m.title} {err}")
                return False
            return bool(result)
        except Exception as e:
            logger.error(f"RSS dl err {e}")
            return False

    def _rss_log(self, a, title, r=""):
        # RSS下载历史不持久化，仅用于本次运行期间的日志记录
        pass

    # ==================== tmdbid标签 ====================

    def _start_tag_scheduler(self) -> None:
        """启动tmdbid标签后台扫描线程。"""
        if self._tag_scheduler_thread and self._tag_scheduler_thread.is_alive():
            return
        self._tag_scheduler_running = True
        self._tag_scheduler_event = threading.Event()
        self._tag_scheduler_thread = threading.Thread(
            target=self._tag_scheduler_loop,
            daemon=True,
            name="SC-TmdbScheduler",
        )
        self._tag_scheduler_thread.start()

    def _tag_scheduler_loop(self) -> None:
        """tmdbid标签后台扫描循环。"""
        if not self._tag_scheduler_event:
            return
        interval_seconds = max(self._tag_interval, 1) * 3600
        self._tag_scheduler_event.wait(interval_seconds)
        while self._tag_scheduler_running:
            try:
                self._scan_and_tag()
            except Exception as err:
                logger.error(f"tmdbid标签扫描异常: {err}")
            self._tag_scheduler_event.clear()
            if self._tag_scheduler_event.wait(interval_seconds):
                break

    def _scan_and_tag(self) -> None:
        """扫描下载器种子：识别并添加 tmdbid 标签。"""
        chain = self._get_chain()
        torrents = self._tag_list_torrents(chain)
        if not torrents:
            logger.info("SC-Tmdb 未找到任何种子")
            return

        # 识别未标记 tmdbid 的种子并打标签
        title_groups = self._tag_group_untagged(torrents)
        if title_groups:
            logger.info(f"SC-Tmdb 待识别标题数: {len(title_groups)}，待处理种子数: {sum(len(v) for v in title_groups.values())}")
            tagged, skipped = self._tag_title_groups(chain, title_groups)
            logger.info(f"SC-Tmdb tmdbid 标记完成: 已标记 {tagged} 个种子，跳过 {skipped} 个")
        else:
            logger.info("SC-Tmdb 所有种子已有 tmdbid 标签")

    def _tag_list_torrents(self, chain: ChainBase) -> List[Any]:
        """列出需要标记tmdbid标签的种子。"""
        downloaders = self._tag_downloaders or []
        if not downloaders:
            logger.info("SC-Tmdb 未选择下载器，跳过")
            return []
        logger.info(f"SC-Tmdb 开始扫描下载器种子，下载器: {downloaders}")
        all_torrents: List[Any] = []
        for downloader in downloaders:
            torrents = chain.list_torrents(downloader=downloader, include_all_tags=True)
            if torrents:
                all_torrents.extend(torrents)
        return all_torrents

    @staticmethod
    def _tag_group_untagged(torrents: List[Any]) -> Dict[str, List[Tuple[str, Optional[str]]]]:
        """按标题归并尚未添加 tmdbid 标签的种子。返回 {title: [(hash, downloader), ...]}。"""
        title_groups: Dict[str, List[Tuple[str, Optional[str]]]] = {}
        for torrent in torrents:
            title = (getattr(torrent, "title", None) or getattr(torrent, "name", None) or "").strip()
            if not title:
                continue
            if "tmdbid=" in (getattr(torrent, "tags", None) or ""):
                continue
            torrent_hash = getattr(torrent, "hash", None)
            if not torrent_hash:
                continue
            downloader = getattr(torrent, "downloader", None)
            title_groups.setdefault(title, []).append((torrent_hash, downloader))
        return title_groups

    def _tag_title_groups(self, chain: ChainBase, title_groups: Dict[str, List[Tuple[str, Optional[str]]]]) -> Tuple[int, int]:
        """识别标题分组并写入 tmdbid 标签。返回 (tagged, skipped)。"""
        tagged = 0
        skipped = 0
        for title, hash_list in title_groups.items():
            tmdb_id = self._tag_recognize_tmdb_id(chain, title)
            if not tmdb_id:
                skipped += len(hash_list)
                continue
            tmdb_tag = f"tmdbid={tmdb_id}"
            for torrent_hash, downloader in hash_list:
                try:
                    chain.set_torrents_tag(hashs=torrent_hash, tags=[tmdb_tag], downloader=downloader)
                    tagged += 1
                except Exception as err:
                    logger.error(f"SC-Tmdb 为种子 {title} (hash={torrent_hash}) 打标签失败: {err}")
            logger.info(f"SC-Tmdb 已为 {len(hash_list)} 个同名种子添加标签: {title} -> {tmdb_tag}")
        return tagged, skipped

    @staticmethod
    def _tag_recognize_tmdb_id(chain: ChainBase, title: str) -> Optional[int]:
        """识别标题对应的 TMDB ID。"""
        try:
            meta = MetaVideo(title=title)
            if not meta.name:
                logger.debug(f"SC-Tmdb 未能从标题解析出媒体名称: {title}")
                return None
            mediainfo = chain.recognize_media(meta=meta, cache=True)
            if not mediainfo or not mediainfo.tmdb_id:
                logger.debug(f"SC-Tmdb 未能识别种子: {title}")
                return None
            return int(mediainfo.tmdb_id)
        except Exception as err:
            logger.error(f"SC-Tmdb 处理种子失败: {title}, 错误: {err}")
            return None

    # ==================== tmdbid标签结束 ====================

    def api_tag_scan_now(self, apikey: str = None) -> Dict[str, Any]:
        """API：立即扫描tmdbid标签。"""
        try:
            threading.Thread(target=self._scan_and_tag, daemon=True, name="SC-TmdbScanNow").start()
            return {"success": True, "message": "tmdbid标签扫描已启动"}
        except Exception as err:
            return {"success": False, "message": str(err)}
