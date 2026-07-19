"""
SpaceCleaner: 空间清理 + 智能RSS下载，共用播放进度缓存。
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


class SpaceCleaner(_PluginBase):
    plugin_name = "空间清理器"
    plugin_desc = "剩余空间不足时自动删除已观看资源（优先删除最早看完/标记的资源，电视剧按整理记录中该季最后一集看完即删整季，含辅种及同集/同片的不同版本，删种后一并删除媒体库文件及其所在目录）；智能RSS下载自动跳过已看完剧集。"
    plugin_icon = "delete.png"
    plugin_version = "4.6.0"
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
    _delete_cross_seeds = True  # 删种时同时删除辅种（内容相同、tracker 不同的种子），含非 MP 管理的种子
    _delete_other_versions = True  # 删种时检索整理记录，删除同一集/同一部电影的其他版本
    _notify = True
    _media_cache_disabled = False  # 关闭媒体缓存（默认开启）
    _pb_page = 1
    _pb_sort_by = "time"  # 播放缓存排序：time / title / status
    _pb_sort_desc = True  # 播放缓存排序方向：True 降序 / False 升序
    _pb_filter_watched = True  # 播放缓存默认只显示已看完
    _pb_search = ""  # 播放缓存搜索关键字
    _pb_interacted = False  # 本次数据页会话是否发生过页内交互（用于判断是否为首次打开）
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
    _rss_ctmdba = False  # 洗版分辨季时查询 CTMDbA 插件（番剧季分离），修正 TMDB 合并季
    _rss_ctmdba_port = 8632  # CTMDbA 本地代理端口
    _ctmdba_cache: dict = {}  # tmdb_id -> {(tmdb_season, tmdb_ep): (logical_season, logical_ep)}

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
    _pb_max = 0  # 播放缓存最大条数，0 表示无上限
    _pb_lock = threading.Lock()
    _rss_s: Optional[BackgroundScheduler] = None
    _rss_busy = False
    _rss_seen: set = set()
    _rss_washed: set = set()  # 已洗版下载过的集(tmdbid:SxxExx)，一集一个槽位
    _rss_lk = threading.Lock()

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._enabled = self._rss_on = False
        self._min_free_percent = 10
        self._delete_by_target = self._dry_run = self._notify = False
        self._delete_same_size = True
        self._delete_cross_seeds = True
        self._delete_other_versions = True
        self._media_cache_disabled = False
        self._pb_page = 1
        self._pb_sort_by = "time"
        self._pb_sort_desc = True
        self._pb_filter_watched = True
        self._pb_search = ""
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
        self._rss_ctmdba = False
        self._rss_ctmdba_port = 8632
        self._ctmdba_cache = {}
        self._pb = list(self.get_data("pb") or [])
        self._rss_seen = set()
        self._rss_washed = set()
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
        self._delete_cross_seeds = bool(config.get("delete_cross_seeds", True))
        self._delete_other_versions = bool(config.get("delete_other_versions", True))
        self._notify = bool(config.get("notify", True))
        self._media_cache_disabled = bool(config.get("media_cache_disabled", False))
        # 播放缓存视图状态不持久化：每次加载插件默认按时间从近到远排序，并清空搜索
        self._pb_page = 1
        self._pb_sort_by = "time"
        self._pb_sort_desc = True  # 时间降序（从近到远）
        self._pb_filter_watched = bool(config.get("pb_filter_watched", True))
        self._pb_search = ""
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
        self._rss_washed = set(self.get_data("rss_washed") or [])
        self._rss_wash_mode = bool(config.get("rss_wash_mode"))
        self._rss_save_path = str(config.get("rss_save_path") or "")
        self._rss_ctmdba = bool(config.get("rss_ctmdba"))
        try:
            self._rss_ctmdba_port = int(config.get("rss_ctmdba_port") or 8632)
        except (ValueError, TypeError):
            self._rss_ctmdba_port = 8632

        if self._enabled:
            self._start_scheduler()
        if run_now:
            config["run_now"] = False
            self.update_config(config)
            threading.Thread(target=self._run_now_task, daemon=True, name="SC-RunNow").start()

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
            "delete_cross_seeds": self._delete_cross_seeds, "delete_other_versions": self._delete_other_versions, "notify": self._notify,
            "media_cache_disabled": self._media_cache_disabled, "run_now": False,
            "pb_filter_watched": self._pb_filter_watched, "watched_threshold": self._watched_threshold,
            "rss_on": self._rss_on, "rss_cron": self._rss_cron, "rss_urls": self._rss_urls,
            "rss_dl": self._rss_dl, "rss_sz": self._rss_sz, "rss_inc": self._rss_inc,
            "rss_exc": self._rss_exc, "rss_once": self._rss_once, "rss_ntf": self._rss_ntf,
            "rss_th": self._rss_th, "rss_wash_mode": self._rss_wash_mode,
            "rss_save_path": self._rss_save_path,
            "rss_ctmdba": self._rss_ctmdba, "rss_ctmdba_port": self._rss_ctmdba_port,
            "clean_downloader": self._clean_downloader,
        })

    def get_state(self) -> bool:
        return self._enabled or self._rss_on

    # ==================== Webhook 共用播放缓存 ====================

    @eventmanager.register(EventType.WebhookMessage)
    def on_webhook(self, event: Event) -> None:
        if self._media_cache_disabled:
            logger.info("SC on_webhook skipped: media_cache_disabled=True")
            return
        if not self._enabled and not self._rss_on:
            logger.info(f"SC on_webhook skipped: enabled={self._enabled} rss_on={self._rss_on}")
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
            if self._pb_max > 0 and len(self._pb) > self._pb_max:
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
            {"path": "/pb_search", "endpoint": self.set_pb_search, "methods": ["GET"], "summary": "设置播放缓存搜索关键字"},
            {"path": "/pb_mark_watched", "endpoint": self.pb_mark_watched, "methods": ["GET"], "summary": "将单条播放记录标记为已看"},
            {"path": "/pb_mark_all_watched", "endpoint": self.pb_mark_all_watched, "methods": ["GET"], "summary": "将所有未看完记录标记为已看"},
            {"path": "/pb_toggle_prio", "endpoint": self.pb_toggle_prio, "methods": ["GET"], "summary": "切换播放记录优先删除标记"},
            {"path": "/rss_dh", "endpoint": self.rss_dh, "methods": ["GET"], "summary": "删除RSS历史"},
            {"path": "/rss_ca", "endpoint": self.rss_ca, "methods": ["GET"], "summary": "清除RSS数据"},
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
        self._pb_interacted = True
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
        self._pb_interacted = True
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
        self._pb_interacted = True
        try:
            self._pb_page = max(1, int(page or 1))
        except (ValueError, TypeError):
            self._pb_page = 1
        return schemas.Response(success=True)

    def set_pb_sort(self, sort_by: str, apikey: str):
        """设置播放缓存排序方式。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if sort_by not in ("time", "title", "status"):
            return schemas.Response(success=False, message="无效排序字段")
        self._pb_interacted = True
        if self._pb_sort_by == sort_by:
            # 同字段切换排序方向
            self._pb_sort_desc = not self._pb_sort_desc
        else:
            self._pb_sort_by = sort_by
            self._pb_sort_desc = True  # 默认降序
        self._pb_page = 1
        return schemas.Response(success=True)

    def toggle_pb_filter(self, apikey: str):
        """切换播放缓存已看完筛选。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_interacted = True
        self._pb_filter_watched = not self._pb_filter_watched
        self._pb_page = 1
        self._update_config()
        return schemas.Response(success=True)

    def set_pb_search(self, q: str = "", apikey: str = ""):
        """设置播放缓存搜索关键字（空串表示清除搜索）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_interacted = True
        self._pb_search = (q or "").strip()
        self._pb_page = 1
        return schemas.Response(success=True)

    def pb_mark_watched(self, k: str, apikey: str):
        """将单条未看完的播放记录标记为已看（进度置为100%）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_interacted = True
        marked = False
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    r["p"] = 100.0
                    r["t"] = ts
                    marked = True
                    break
        if marked:
            self.save_data("pb", self._pb)
            self._pb_cache = None
            logger.info(f"SC 标记已看: {k}")
        return schemas.Response(success=True)

    def pb_toggle_prio(self, k: str, apikey: str):
        """切换单条播放记录的优先删除标记。被标记的资源在空间清理时优先删除。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_interacted = True
        new_state = None
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    new_state = not bool(r.get("prio"))
                    r["prio"] = new_state
                    break
        if new_state is not None:
            self.save_data("pb", self._pb)
            self._pb_cache = None
            logger.info(f"SC 优先删除标记 {'开启' if new_state else '取消'}: {k}")
        return schemas.Response(success=True)

    def pb_mark_all_watched(self, apikey: str):
        """将所有未看完的播放记录批量标记为已看（进度置为100%）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._pb_interacted = True
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cnt = 0
        with self._pb_lock:
            for r in self._pb:
                if (r.get("p", 0) or 0) < self._watched_threshold:
                    r["p"] = 100.0
                    r["t"] = ts
                    cnt += 1
        if cnt:
            self.save_data("pb", self._pb)
            self._pb_cache = None
        logger.info(f"SC 批量标记已看 {cnt} 条")
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

    # ==================== 表单（顶部 Tab 页签切换） ====================

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        dls = []
        try:
            from app.helper.downloader import DownloaderHelper
            svcs = DownloaderHelper().get_services()
            dls = [{"title": n, "value": n} for n, s in svcs.items() if s.config and s.config.enabled]
        except Exception:
            pass

        # ---------- 空间清理 ----------
        clean_form = {
            "component": "VForm",
            "content": [
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1"}, "text": "基本设置"},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "删除时发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "dry_run", "label": "试运行模式", "hint": "仅在日志中显示将要删除的资源，不实际删除", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "run_now", "label": "立即运行一次"}}]},
                ]},
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1 mt-3"}, "text": "删除策略"},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "delete_by_target", "label": "按目标百分比删除", "hint": "持续删除资源直到剩余空间达到目标百分比", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "delete_cross_seeds", "label": "删除辅种", "hint": "删种时同时删除内容相同、tracker 不同的辅种（含非 MP 管理的种子）", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "delete_other_versions", "label": "删除不同版本", "hint": "删种时检索整理记录，删除同一集/同一部电影的不同版本（不同分辨率、字幕组等）", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "media_cache_disabled", "label": "关闭媒体缓存", "hint": "开启后不再接收播放进度，也不新增播放记录", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mb-0"},
                         "content": [{"component": "div", "props": {"class": "text-caption"}, "text": "「删除不同版本」：删种时会检索媒体整理记录，把同一集电视剧或同一部电影的其他版本（不同分辨率、编码、字幕组、发布组等）一并删除，包括它们对应的源文件、媒体库文件、下载器种子（含辅种）及整理记录。电视剧按 tmdbid + 季 + 集号匹配，电影按 tmdbid 匹配。"}]}
                    ]},
                ]},
                {"component": "VDivider", "props": {"class": "my-2"}},
                {"component": "div", "props": {"class": "text-caption text-medium-emphasis mb-1 mt-2"}, "text": "清理参数"},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "min_free_percent", "label": "删种触发阈值（%）", "type": "number", "min": 1, "max": 99}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "target_free_percent", "label": "目标剩余百分比", "type": "number", "min": 1, "max": 99}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "check_interval", "label": "检查间隔（小时）", "type": "number", "min": 1}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "delete_count", "label": "单次删除资源数", "type": "number", "min": 1}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "watched_threshold", "label": "标记已看播放进度阈值（%）", "type": "number", "min": 1, "max": 100, "hint": "播放进度达到此百分比时标记为已观看", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [{"component": "VSelect", "props": {"model": "clean_downloader", "label": "扫描下载器", "items": dls, "multiple": True, "chips": True, "clearable": True, "hint": "删种时扫描的下载器，留空扫描全部", "persistent-hint": True}}]},
                ]},
            ],
        }

        # ---------- BT动漫RSS下载/洗版 ----------
        rss_form = {
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
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "rss_ctmdba", "label": "使用 CTMDbA 分季", "hint": "洗版辨别番剧季号时查询 CureTMDbA 插件的季分离映射（需已安装并启用该插件）", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": "rss_ctmdba_port", "label": "CTMDbA 端口", "type": "number", "placeholder": "8632", "hint": "与 CureTMDbA 插件运行端口一致", "persistent-hint": True}}]},
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
        }

        return [
            {
                "component": "VTabs",
                "props": {"model": "_active_tab", "color": "primary", "grow": True, "class": "mb-4"},
                "content": [
                    {"component": "VTab", "props": {"value": "clean"}, "text": "空间清理"},
                    {"component": "VTab", "props": {"value": "rss"}, "text": "BT动漫RSS下载/洗版"},
                ],
            },
            {
                "component": "VWindow",
                "props": {"model": "_active_tab"},
                "content": [
                    {"component": "VWindowItem", "props": {"value": "clean"}, "content": [clean_form]},
                    {"component": "VWindowItem", "props": {"value": "rss"}, "content": [rss_form]},
                ],
            },
        ], {
            "_active_tab": "clean",
            "enabled": False, "min_free_percent": 10,
            "delete_by_target": False, "target_free_percent": 20,
            "delete_count": 1, "check_interval": 6,
            "dry_run": False, "delete_same_size": False, "delete_cross_seeds": True, "delete_other_versions": True, "notify": True,
            "media_cache_disabled": False, "clean_downloader": [], "run_now": False,
            "pb_filter_watched": True, "watched_threshold": 85,
            "rss_on": False, "rss_cron": "*/30 * * * *", "rss_urls": "",
            "rss_dl": "", "rss_sz": "", "rss_inc": "", "rss_exc": "",
            "rss_once": False, "rss_ntf": True, "rss_th": 85, "rss_wash_mode": False, "rss_save_path": "",
            "rss_ctmdba": False, "rss_ctmdba_port": 8632,
        }

    # ==================== 详情页（三区块平铺） ====================

    def get_page(self) -> Optional[List[dict]]:
        # 首次打开数据页（非页内交互触发的刷新）时自动清除搜索栏并回到首页
        if not self._pb_interacted:
            self._pb_search = ""
            self._pb_page = 1
        # 重置交互标记：下一次渲染若无交互即视为重新打开数据页
        self._pb_interacted = False
        space_info = self._get_space_info()
        delete_history = self._get_delete_history()
        pb = self._get_playback_pb()
        cards = []

        # 使用提示（紧凑模式）
        cards.append({
            "component": "VAlert",
            "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mb-2"},
            "text": "使用前需配置Webhooks，详见 https://github.com/cudamin/MoviePilot-Plugins",
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
            "component": "VCard", "props": {"variant": "flat", "class": "mt-2"},
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
                "prio": bool(r.get("prio")),
            })
        # 默认筛选已看完
        if self._pb_filter_watched:
            filtered_items = [x for x in all_items if x.get("watched")]
        else:
            filtered_items = list(all_items)
        # 搜索过滤（匹配标题 / 季集）
        if self._pb_search:
            qs = self._pb_search.lower()
            filtered_items = [x for x in filtered_items
                              if qs in (x.get("title", "") or "").lower()
                              or qs in (x.get("se", "") or "").lower()]
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
                {"component": "div", "props": {"style": "width: 200px;"}, "text": "操作"},
            ]}
        ]
        if page_items:
            for item in page_items:
                op_buttons = [
                    {"component": "VBtn", "props": {"color": "error", "variant": "text", "size": "x-small", "class": "px-1"},
                     "text": "删除",
                     "events": {"click": {"api": "plugin/SpaceCleaner/del_pb_item", "method": "get",
                                          "params": {"k": item["k"], "apikey": settings.API_TOKEN}}}},
                    {"component": "VBtn", "props": {"color": "success", "variant": "text", "size": "x-small", "class": "px-1"},
                     "text": "已看",
                     "events": {"click": {"api": "plugin/SpaceCleaner/pb_mark_watched", "method": "get",
                                          "params": {"k": item["k"], "apikey": settings.API_TOKEN}}}},
                    {"component": "VBtn", "props": {"color": "warning" if item["prio"] else "default", "variant": "text", "size": "x-small", "class": "px-1"},
                     "text": "取消优先" if item["prio"] else "优先删除",
                     "events": {"click": {"api": "plugin/SpaceCleaner/pb_toggle_prio", "method": "get",
                                          "params": {"k": item["k"], "apikey": settings.API_TOKEN}}}},
                ]
                title_cell = ("⭐ " + item["title"]) if item["prio"] else item["title"]
                table_rows.append({"component": "div", "props": {"class": "d-flex align-center px-3 py-2 border-t text-body-2"}, "content": [
                    {"component": "div", "props": {"style": "flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"}, "text": title_cell},
                    {"component": "div", "props": {"style": "width: 92px;"}, "text": item["se"]},
                    {"component": "div", "props": {"style": "width: 76px;"}, "text": f"{item['progress']:.1f}%"},
                    {"component": "div", "props": {"style": "width: 76px;"}, "text": "已看完" if item["watched"] else "未看完"},
                    {"component": "div", "props": {"style": "width: 150px;"}, "text": item["time"]},
                    {"component": "div", "props": {"style": "width: 200px; display: flex; flex-direction: row; align-items: center; gap: 4px;"}, "content": op_buttons},
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
        # 搜索框（数据页事件仅支持静态参数，输入值通过内嵌HTML提交到搜索API，再触发隐藏按钮刷新页面）
        q_escaped = (self._pb_search or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        _token = settings.API_TOKEN
        _search_js = (
            "var i=document.getElementById('sc-pb-q');"
            "fetch('/api/v1/plugin/SpaceCleaner/pb_search?q='+encodeURIComponent(i?i.value:'')+'&apikey=" + _token + "')"
            ".finally(function(){var b=document.getElementById('sc-pb-refresh');if(b){b.click();}});"
        )
        _clear_js = (
            "var i=document.getElementById('sc-pb-q');if(i){i.value='';}"
            "fetch('/api/v1/plugin/SpaceCleaner/pb_search?q=&apikey=" + _token + "')"
            ".finally(function(){var b=document.getElementById('sc-pb-refresh');if(b){b.click();}});"
        )
        search_html = (
            '<div style="display:flex;align-items:center;gap:8px;width:100%;">'
            f'<input id="sc-pb-q" type="text" placeholder="搜索标题关键字，回车确认" value="{q_escaped}" '
            'style="flex:1 1 auto;min-width:100px;padding:5px 10px;border:1px solid rgba(128,128,128,.45);'
            'border-radius:6px;font-size:12px;background:transparent;color:inherit;outline:none;" '
            f'onkeydown="if(event.key===\'Enter\'){{event.preventDefault();{_search_js}}}">'
            '<button type="button" style="padding:5px 14px;border-radius:6px;font-size:12px;border:none;cursor:pointer;'
            'background:rgb(var(--v-theme-primary));color:#fff;white-space:nowrap;" '
            f'onclick="{_search_js}">搜索</button>'
            '<button type="button" style="padding:5px 14px;border-radius:6px;font-size:12px;cursor:pointer;'
            'border:1px solid rgba(128,128,128,.45);background:transparent;color:inherit;white-space:nowrap;" '
            f'onclick="{_clear_js}">清除</button>'
            '</div>'
        )
        # 统计副标题
        pb_subtitle = f"共 {len(all_items)} 条，已看完 {watched_count} 条，未看完 {len(all_items)-watched_count} 条（当前显示 {len(filtered_items)} 条）"
        if self._pb_search:
            pb_subtitle += f"，搜索：{self._pb_search}"
        cards.append({
            "component": "VCard", "props": {"variant": "flat", "class": "mt-2"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "d-flex align-center justify-space-between pa-3"}, "content": [
                    {"component": "div", "content": [
                        {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold"}, "text": "播放缓存"},
                        {"component": "div", "props": {"class": "text-caption text-medium-emphasis"},
                         "text": pb_subtitle},
                    ]},
                    {"component": "div", "props": {"class": "d-flex align-center"}, "content": [
                        {"component": "VBtn", "props": {"variant": self._pb_filter_watched and "flat" or "text", "size": "x-small", "color": self._pb_filter_watched and "primary" or "default", "class": "mr-2"},
                         "text": "仅已看完",
                         "events": {"click": {"api": "plugin/SpaceCleaner/pb_filter_toggle", "method": "get", "params": {"apikey": settings.API_TOKEN}}}},
                        {"component": "VBtn", "props": {"color": "success", "variant": "tonal", "size": "small", "class": "mr-2",
                                                        "disabled": (len(all_items) - watched_count) <= 0},
                         "text": "全部标记已看",
                         "events": {"click": {"api": "plugin/SpaceCleaner/pb_mark_all_watched", "method": "get",
                                              "params": {"apikey": settings.API_TOKEN}}}},
                        {"component": "VBtn", "props": {"color": "error", "variant": "tonal", "size": "small", "disabled": not bool(pb)},
                         "text": "清除全部",
                         "events": {"click": {"api": "plugin/SpaceCleaner/clear_pb", "method": "get",
                                              "params": {"apikey": settings.API_TOKEN}}}},
                    ]},
                ]},
                {"component": "VCardText", "props": {"class": "pa-0"}, "content": [
                    {"component": "div", "props": {"class": "d-flex align-center px-3 pt-3 pb-1"}, "content": [
                        {"component": "div", "props": {"style": "flex: 1 1 auto; min-width: 0;"}, "html": search_html},
                        {"component": "VBtn", "props": {"id": "sc-pb-refresh", "style": "display:none;", "variant": "text", "size": "x-small"},
                         "text": "刷新",
                         "events": {"click": {"api": "plugin/SpaceCleaner/pb_page", "method": "get",
                                              "params": {"page": page, "apikey": settings.API_TOKEN}}}},
                    ]},
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
        # 清理无对应整理记录的失效播放缓存，避免其干扰后续判断
        self._prune_orphan_pb()
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
            pb = self._get_playback_pb()
            # 有播放缓存记录的 tmdbid 集合：没有播放缓存的整理记录无需检查（不可能满足"已看完"条件）
            pb_tmdbids = set()
            for p in pb:
                km = re.match(r'(\d+):', p.get("k", "") or "")
                if km:
                    pb_tmdbids.add(int(km.group(1)))

            offset = 0
            # 按 download_hash 分组：收集每组所有记录的(tmdbid, season, episodes)
            hash_groups: Dict[str, List[TransferHistory]] = {}
            no_hash_records = []
            while True:
                recs = sess.query(TransferHistory).filter(TransferHistory.status == True).order_by(asc(TransferHistory.id)).offset(offset).limit(25).all()
                if not recs:
                    break
                for r in recs:
                    # 无播放缓存记录的资源直接跳过，节省后续处理
                    if not r.tmdbid or r.tmdbid not in pb_tmdbids:
                        continue
                    if r.download_hash:
                        hash_groups.setdefault(r.download_hash, []).append(r)
                    elif r.type == "电视剧":
                        no_hash_records.append(r)
                    elif r.type != "电视剧" and r.tmdbid:
                        # 电影无 hash：直接检查 pb 中的 {tmdbid}:M
                        no_hash_records.append(r)
                offset += len(recs)

            # 优先删除标记：pb 中被标记 prio 的资源对应的 tmdbid 集合
            prio_tmdbids = set()
            for p in pb:
                if p.get("prio"):
                    km = re.match(r'(\d+):', p.get("k", "") or "")
                    if km:
                        prio_tmdbids.add(km.group(1))

            def _snap(r):
                return {"id": r.id, "title": r.title or "未知", "type": r.type or "",
                        "seasons": r.seasons or "", "episodes": r.episodes or "",
                        "src": r.src or "", "dest": r.dest or "", "tmdbid": r.tmdbid,
                        "download_hash": r.download_hash or ""}

            def _ep_max(rec):
                mm = re.findall(r'\d+', rec.episodes or "0")
                return max(int(e) for e in mm) if mm else 0

            def _movie_watched_time(rec):
                """电影：pb 中 {tmdbid}:M 已看完才返回其缓存时间，否则 None。"""
                k = f"{rec.tmdbid}:M"
                for p in pb:
                    if p.get("k") == k:
                        if (p.get("p", 0) or 0) >= self._watched_threshold:
                            return p.get("t", "9999")
                        return None
                return None

            def _unit_earliest_mark_time(tmdbid, season=None):
                """取该单元在播放缓存中最早的标记时间（排序用）。

                电影取 {tmdbid}:M 的缓存时间；电视剧取该季各集缓存时间的最小值。
                以"最早标记"作为删除/跳过顺序依据：越早标记的越先处理。
                无匹配缓存时返回 "9999"（排到最后）。
                """
                if season is None:
                    prefix = f"{tmdbid}:M"
                    def _m(k):
                        return k == prefix
                else:
                    sp = f"{tmdbid}:S{season:02d}E"
                    def _m(k):
                        return k.startswith(sp)
                times = [p.get("t", "9999") for p in pb if _m(p.get("k", "") or "")]
                return min(times) if times else "9999"

            def _season_last_watched_time(recs):
                """电视剧整季判断：以整理记录中该季出现的最后一集为准，
                只有最后一集已看完才返回 (缓存时间, None)，否则返回 (None, 跳过原因)。
                例：记录含 S01E01~S01E13，则需 S01E13 看完才删除整季。"""
                tmdbid = recs[0].tmdbid
                title = recs[0].title or "未知"
                season = None
                max_ep = 0
                for r in recs:
                    s = self._norm_season(r.seasons or "")
                    if s is not None:
                        season = s
                    eps = self._episode_set(r.episodes or "")
                    if eps:
                        max_ep = max(max_ep, max(eps))
                if season is None or max_ep <= 0:
                    return None, f"{title}: 无法解析季/集号，跳过"
                se = f"S{season:02d}E{max_ep:02d}"
                k = f"{tmdbid}:{se}"
                for p in pb:
                    if p.get("k") == k:
                        prog = p.get("p", 0) or 0
                        if prog >= self._watched_threshold:
                            return p.get("t", "9999"), None
                        return None, f"{title} S{season:02d}: 最后一集 {se} 未看完（进度 {prog:.0f}% < {self._watched_threshold}%），整季跳过"
                return None, f"{title} S{season:02d}: 最后一集 {se} 无播放记录，整季跳过"

            # 删除单元：电视剧整季一起删除、电影单条删除
            delete_units = []
            skip_logs = []

            def _add_tv_season_unit(recs):
                """一个 (tmdbid, season) 的所有整理记录构成一个删除单元。"""
                t, skip_reason = _season_last_watched_time(recs)
                tmdbid = recs[0].tmdbid
                season = self._norm_season(recs[0].seasons or "")
                # 排序键：该季在播放缓存中最早的标记时间（越早越先处理）
                mark_time = _unit_earliest_mark_time(tmdbid, season)
                if t is None:
                    if skip_reason:
                        skip_logs.append((mark_time, skip_reason))
                    return
                rep = max(recs, key=_ep_max)
                tmdbid = rep.tmdbid
                season = self._norm_season(rep.seasons or "")
                dh = ""
                for rr in recs:
                    if rr.download_hash:
                        dh = rr.download_hash
                        break
                ep_count = len({e for r in recs for e in self._episode_set(r.episodes or "")})
                display = f"{rep.title or ''} S{season:02d}".strip() if season is not None else (rep.title or "未知")
                display = f"{display}（整季 {ep_count} 集）"
                delete_units.append({
                    "records": [_snap(r) for r in recs],
                    "hash": dh,
                    "tmdbid": tmdbid,
                    "season": season,
                    "is_tv": True,
                    "display": display,
                    "sort_time": mark_time,
                    "prio": str(tmdbid) in prio_tmdbids,
                })

            def _add_movie_unit(recs):
                t = _movie_watched_time(recs[0])
                if t is None:
                    return
                rep = recs[0]
                tmdbid = rep.tmdbid
                dh = ""
                for rr in recs:
                    if rr.download_hash:
                        dh = rr.download_hash
                        break
                delete_units.append({
                    "records": [_snap(r) for r in recs],
                    "hash": dh,
                    "tmdbid": tmdbid,
                    "season": None,
                    "is_tv": False,
                    "display": rep.title or "未知",
                    "sort_time": _unit_earliest_mark_time(tmdbid, None),
                    "prio": str(tmdbid) in prio_tmdbids,
                })

            # 汇总所有记录（有/无 download_hash）后重新分组
            all_records: List[TransferHistory] = []
            for recs in hash_groups.values():
                all_records.extend(recs)
            all_records.extend(no_hash_records)

            # 电视剧：按 tmdbid+season 归并（跨种子/跨版本），整季作为一个删除单元
            tv_season_groups: Dict[str, List[TransferHistory]] = {}
            # 电影：有 hash 按 hash 归并，无 hash 每条一个单元
            movie_hash_groups: Dict[str, List[TransferHistory]] = {}
            movie_no_hash: List[TransferHistory] = []
            for r in all_records:
                if not r.tmdbid:
                    continue
                if (r.type or "") == "电视剧":
                    season = self._norm_season(r.seasons or "")
                    if season is None:
                        continue  # 无法判定季，跳过
                    key = f"{r.tmdbid}:S{season:02d}"
                    tv_season_groups.setdefault(key, []).append(r)
                else:
                    if r.download_hash:
                        movie_hash_groups.setdefault(r.download_hash, []).append(r)
                    else:
                        movie_no_hash.append(r)

            for recs in tv_season_groups.values():
                _add_tv_season_unit(recs)
            for recs in movie_hash_groups.values():
                _add_movie_unit(recs)
            for r in movie_no_hash:
                _add_movie_unit([r])

            # 输出因未看完/无播放记录而跳过的电视剧季，按最早标记时间排序，每行 5 个
            if skip_logs:
                skip_logs.sort(key=lambda x: x[0])
                reasons = [s for _, s in skip_logs]
                logger.info(f"SC 以下 {len(reasons)} 个电视剧季不满足删除条件，已跳过（按最早标记时间排序）：")
                for i in range(0, len(reasons), 5):
                    logger.info("SC   - " + " ｜ ".join(reasons[i:i + 5]))

            if not delete_units:
                logger.info("SC 未发现满足删除条件的资源（已看完且在转移历史中）")
                return

            logger.info(f"SC 共 {len(delete_units)} 个删除单元满足条件，按优先标记与最早标记时间排序后开始清理")
            # 排序：优先删除被标记的资源，其次播放缓存中最早标记的（标记时间升序）
            delete_units.sort(key=lambda u: (not u["prio"], u["sort_time"]))
        finally:
            sess.close()

        for unit in delete_units:
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
            self._delete_unit(unit, chain, cs or space_info, all_torrents)
            dc += 1
        if fr:
            return
        logger.info(f"SC 清理完成，删除 {dc} 个资源")

    def _delete_unit(self, unit, chain, space_info, all_torrents=None):
        """删除一个删除单元（合集的所有集一起删除）。

        使用预先快照的记录字典，避免跨 Session 访问 ORM 懒加载属性。
        """
        records = unit["records"]
        display_name = unit["display"]
        download_hash = unit["hash"]
        tmdbid = unit["tmdbid"]
        season = unit.get("season")
        is_tv = unit.get("is_tv", False)
        # 其他版本：同一集/同一部电影的不同发布版本（分辨率、字幕组、编码等）
        other_versions = self._find_other_version_records(records) if self._delete_other_versions else []
        all_recs = records + other_versions
        if self._dry_run:
            # 统计将删除的种子（主种子 + 辅种）
            torrents = self._get_cached_torrents(chain)
            seen = set()
            main_cnt = cross_cnt = 0
            for rec in all_recs:
                dh = rec.get("download_hash", "")
                if dh and dh not in seen:
                    seen.add(dh)
                    tl = self._collect_torrents_to_delete(dh, torrents)
                    main_cnt += 1
                    cross_cnt += sum(1 for _, _, is_cross in tl if is_cross)
            unit_type = "电视剧整季" if is_tv else "电影"
            logger.info(
                f"【试运行】将删除 [{unit_type}] {display_name}："
                f"整理记录 {len(records)} 条"
                + (f"，不同版本 {len(other_versions)} 条" if other_versions else "")
                + f"，种子 {main_cnt} 个"
                + (f"（含辅种 {cross_cnt} 个）" if cross_cnt else "")
            )
            detail = (f"将删除：整理记录 {len(records)} 条"
                      + (f"，不同版本 {len(other_versions)} 条" if other_versions else "")
                      + f"，种子 {main_cnt} 个"
                      + (f"（含辅种 {cross_cnt} 个）" if cross_cnt else ""))
            self._add_delete_history(display_name, "试运行 - " + detail)
            return
        try:
            unit_type = "电视剧整季" if is_tv else "电影"
            logger.info(f"SC 开始删除 [{unit_type}] {display_name}："
                        f"整理记录 {len(records)} 条"
                        + (f"，不同版本 {len(other_versions)} 条" if other_versions else ""))
            # 1) 从下载器删除该单元涉及的全部种子及其辅种（整季可能跨多个种子）
            #    删种时 delete_file=True 会一并删除下载目录中的源文件
            torrents_deleted = 0
            seen_hashes = set()
            for rec in records:
                dh = rec.get("download_hash", "")
                if dh and dh not in seen_hashes:
                    seen_hashes.add(dh)
                    torrents_deleted += self._delete_downloader_torrents(chain, dh, display_name, all_torrents)
            # 若单元记录均无 hash 但存在代表 hash，仍尝试删除
            if not seen_hashes and download_hash:
                seen_hashes.add(download_hash)
                torrents_deleted += self._delete_downloader_torrents(chain, download_hash, display_name, all_torrents)
            # 不同版本各自对应的种子也一并删除（含其辅种）
            for rec in other_versions:
                dh = rec.get("download_hash", "")
                if dh and dh not in seen_hashes:
                    seen_hashes.add(dh)
                    torrents_deleted += self._delete_downloader_torrents(chain, dh, rec.get("title", "不同版本"), all_torrents)
            if not seen_hashes:
                logger.info(f"SC [{display_name}] 无关联种子（可能为无 hash 记录），跳过删种")
            # 2) 删种后删除媒体库文件及其所在目录（MP 软链接/硬链接、重命名、刮削生成的目录）
            #    以及仍残留的下载源文件（无 hash 记录不会被删种流程清理）
            media_dirs = set()
            for rec in all_recs:
                # 下载源文件：有种子的已随删种删除，此处兜底处理无 hash 记录
                src = rec.get("src", "")
                if src:
                    sp = Path(src)
                    if sp.exists():
                        self._safe_delete_path(sp)
                # 媒体库文件（链接+重命名后的成品）
                dest = rec.get("dest", "")
                if dest:
                    dp = Path(dest)
                    if dp.exists():
                        self._safe_delete_path(dp)
                    media_dirs.add(dp.parent)
            # 删除媒体库目录整体（含 nfo、海报等刮削文件）
            for d in media_dirs:
                self._delete_media_dir(d)
            # 用独立 session 删除所有相关转移记录
            from app.db import ScopedSession
            ds = ScopedSession()
            try:
                for rec in all_recs:
                    r = ds.query(TransferHistory).filter(TransferHistory.id == rec["id"]).first()
                    if r:
                        ds.delete(r)
                ds.commit()
            finally:
                ds.close()
            # 从 pb 缓存中删除对应条目：电视剧仅删除该季，电影删除整个 tmdbid
            if is_tv and season is not None:
                self._delete_pb_by_tmdbid(tmdbid, season)
            else:
                self._delete_pb_by_tmdbid(tmdbid)
            logger.info(f"SC 删除完成 [{unit_type}] {display_name}："
                        f"整理记录 {len(records)} 条"
                        + (f"，不同版本 {len(other_versions)} 条" if other_versions else "")
                        + f"，种子 {torrents_deleted} 个")
            extra = f"，含不同版本 {len(other_versions)} 条" if other_versions else ""
            self._add_delete_history(
                display_name,
                f"已删除（记录 {len(records)} 条{extra}，种子 {torrents_deleted} 个）")
            if self._notify:
                ver_line = f"\n不同版本: {len(other_versions)} 条" if other_versions else ""
                self.post_message(title="空间清理器 - 资源已删除",
                                  text=f"资源: {display_name}{ver_line}\n删除种子: {torrents_deleted} 个\n当前剩余空间: {space_info['free_gb']:.2f} GB ({space_info['free_percent']:.1f}%)")
        except Exception as e:
            logger.error(f"删除 {display_name} 失败: {str(e)}")
            self._add_delete_history(display_name, f"删除失败: {str(e)}")

    @staticmethod
    def _norm_season(seasons: str) -> Optional[int]:
        """规范化季号：'S01' / '1' -> 1；无效返回 None。"""
        s = (seasons or "").strip().upper().replace("S", "")
        return int(s) if s.isdigit() else None

    @staticmethod
    def _episode_set(episodes: str) -> set:
        """解析集号字符串为集号集合，支持 E01、E01-E12、E01,E03、01~12 等格式。"""
        e_str = (episodes or "").strip().upper().replace("E", "")
        if not e_str:
            return set()
        result = set()
        for part in re.split(r'[,\s]+', e_str):
            if not part:
                continue
            m = re.match(r'^(\d+)\s*[-~]\s*(\d+)$', part)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                if a <= b:
                    result.update(range(a, b + 1))
            elif part.isdigit():
                result.add(int(part))
        return result

    def _find_other_version_records(self, unit_records: List[dict]) -> List[dict]:
        """检索整理记录，找出同一集/同一部电影的其他发布版本。

        判定标准：
        - 电影：同一 tmdbid 的其他电影记录（不同分辨率、编码、字幕组等）。
        - 电视剧：同一 tmdbid + 同一季，且集号有交集的其他记录。
        排除本删除单元已包含的记录（按记录 id 去重）。返回记录快照字典列表。
        """
        unit_ids = {r["id"] for r in unit_records}
        # 从本单元记录中提取待匹配的 (tmdbid, 类型, 季, 集集合)
        tmdbids = {r.get("tmdbid") for r in unit_records if r.get("tmdbid")}
        if not tmdbids:
            return []
        # 汇总本单元每个 tmdbid 涉及的 电影/电视剧 季集
        movie_tmdbids = set()
        tv_seasons: Dict[tuple, set] = {}  # (tmdbid, season) -> 集号集合
        for r in unit_records:
            tid = r.get("tmdbid")
            if not tid:
                continue
            if r.get("type") == "电视剧":
                season = self._norm_season(r.get("seasons", ""))
                if season is None:
                    continue
                eps = self._episode_set(r.get("episodes", ""))
                tv_seasons.setdefault((tid, season), set()).update(eps)
            else:
                movie_tmdbids.add(tid)

        found: Dict[int, dict] = {}
        try:
            from app.db import ScopedSession
            sess = ScopedSession()
            try:
                recs = sess.query(TransferHistory).filter(
                    TransferHistory.tmdbid.in_(list(tmdbids)),
                    TransferHistory.status == True
                ).all()
                for r in recs:
                    if r.id in unit_ids or r.id in found:
                        continue
                    tid = r.tmdbid
                    if (r.type or "") == "电视剧":
                        season = self._norm_season(r.seasons or "")
                        if season is None or (tid, season) not in tv_seasons:
                            continue
                        target_eps = tv_seasons[(tid, season)]
                        rec_eps = self._episode_set(r.episodes or "")
                        # 集号有交集才视为同一集的其他版本
                        if not rec_eps or not (rec_eps & target_eps):
                            continue
                    else:
                        if tid not in movie_tmdbids:
                            continue
                    found[r.id] = {
                        "id": r.id, "title": r.title or "未知", "type": r.type or "",
                        "seasons": r.seasons or "", "episodes": r.episodes or "",
                        "src": r.src or "", "dest": r.dest or "", "tmdbid": tid,
                        "download_hash": r.download_hash or "",
                    }
            finally:
                sess.close()
        except Exception as e:
            logger.error(f"检索其他版本失败: {str(e)}")
            return []
        return list(found.values())

    def _delete_pb_by_tmdbid(self, tmdbid: Optional[int], season: Optional[int] = None):
        """从 pb 缓存中删除指定 tmdbid 的条目。

        指定 season 时只删除该季（键形如 {tmdbid}:S01E..），
        避免删除某一季时误删同剧其他季的播放记录；未指定时删除该 tmdbid 全部条目。
        """
        if not tmdbid:
            return
        if season is not None:
            season_prefix = f"{tmdbid}:S{season:02d}E"
            def _match(k):
                return k.startswith(season_prefix)
            scope = f"tmdbid={tmdbid} S{season:02d}"
        else:
            prefix = f"{tmdbid}:"
            def _match(k):
                return k.startswith(prefix)
            scope = f"tmdbid={tmdbid}"
        with self._pb_lock:
            before = len(self._pb)
            self._pb = [r for r in self._pb if not _match(r.get("k", ""))]
            after = len(self._pb)
        if before != after:
            self.save_data("pb", self._pb)
            logger.info(f"从 pb 缓存删除 {scope} 共 {before - after} 条")

    def _prune_orphan_pb(self) -> int:
        """清理在媒体整理记录中已无对应记录的播放缓存条目。

        判定按「季 / 电影」粒度进行（与删除单元一致），不做逐集比对，避免误删：
        - 电视剧键 {tmdbid}:S{季}E{集}：只要整理记录中存在该 tmdbid + 该季的任意记录即视为有对应记录；
        - 电影键 {tmdbid}:M：只要整理记录中存在该 tmdbid 的记录即视为有对应记录。
        仅当整个 tmdbid（或该季）在转移历史中已完全不存在时（资源被彻底删除、
        整理记录被清理但播放缓存残留），才将其判为失效并删除。返回删除条数。
        """
        pb = list(self._pb)
        if not pb:
            return 0
        # 收集 pb 中出现的所有 tmdbid，一次性查出相关整理记录
        tmdbids = set()
        for r in pb:
            m = re.match(r'(\d+):', r.get("k", "") or "")
            if m:
                tmdbids.add(int(m.group(1)))
        if not tmdbids:
            return 0
        # 构建整理记录覆盖的范围：
        #   covered_movie_tmdbids: 存在电影/其他类型记录的 tmdbid 集合
        #   covered_tv_seasons: 存在电视剧记录的 (tmdbid, season) 集合
        #   covered_tv_tmdbids: 存在电视剧记录但季号无法解析的 tmdbid（整部保留）
        covered_movie_tmdbids = set()
        covered_tv_seasons = set()
        covered_tv_tmdbids = set()
        try:
            from app.db import ScopedSession
            sess = ScopedSession()
            try:
                recs = sess.query(TransferHistory).filter(
                    TransferHistory.tmdbid.in_(list(tmdbids)),
                    TransferHistory.status == True
                ).all()
                for r in recs:
                    tid = r.tmdbid
                    if not tid:
                        continue
                    if (r.type or "") == "电视剧":
                        covered_tv_tmdbids.add(tid)
                        season = self._norm_season(r.seasons or "")
                        if season is not None:
                            covered_tv_seasons.add((tid, season))
                    else:
                        covered_movie_tmdbids.add(tid)
            finally:
                sess.close()
        except Exception as e:
            logger.error(f"SC 清理失效播放缓存时查询整理记录失败: {str(e)}")
            return 0

        def _is_orphan(k: str) -> bool:
            # 电影键 {tmdbid}:M
            m_movie = re.match(r'^(\d+):M$', k)
            if m_movie:
                return int(m_movie.group(1)) not in covered_movie_tmdbids
            # 电视剧键 {tmdbid}:S{季}E{集}
            m_tv = re.match(r'^(\d+):S(\d+)E\d+$', k)
            if m_tv:
                tid = int(m_tv.group(1))
                season = int(m_tv.group(2))
                # 该剧在整理记录中完全不存在 -> 失效
                if tid not in covered_tv_tmdbids:
                    return True
                # 该剧存在，但无任何可解析季号的记录 -> 无法按季判定，保守保留
                if not any(t == tid for (t, s) in covered_tv_seasons):
                    return False
                # 该季在整理记录中不存在 -> 失效
                return (tid, season) not in covered_tv_seasons
            # 未知键格式：保守保留，不删
            return False

        # 找出无对应整理记录的失效键
        orphans = [r for r in pb if _is_orphan(r.get("k", "") or "")]
        if not orphans:
            return 0
        orphan_keys = {r.get("k") for r in orphans}
        with self._pb_lock:
            before = len(self._pb)
            self._pb = [r for r in self._pb if r.get("k") not in orphan_keys]
            removed = before - len(self._pb)
        if removed:
            self.save_data("pb", self._pb)
            self._pb_cache = None
            logger.info(f"SC 已清理无对应整理记录的失效播放缓存 {removed} 条")
            for r in orphans[:20]:
                logger.info(f"SC   - 失效缓存: {r.get('n', '')} [{r.get('k', '')}]")
            if len(orphans) > 20:
                logger.info(f"SC   - 其余 {len(orphans) - 20} 条略")
        return removed

    def _collect_torrents_to_delete(self, download_hash, torrents):
        """收集该主种子及其辅种，返回 [(hash, name, is_cross), ...]（含主种子本身）。

        辅种定义：与主种子内容相同（体积一致且名称一致）但 tracker 不同的私有种子。
        辅种共享同一份磁盘文件，通常由不同站点重复做种；删除时必须一并处理，
        否则残留的辅种会重新占用/锁定文件。用于删种与试运行统计（不执行删除）。
        """
        result = []
        if not download_hash:
            return result
        main_t = None
        for t in (torrents or []):
            if getattr(t, "hash", None) == download_hash:
                main_t = t
                break
        result.append((download_hash, self._torrent_name(main_t) if main_t else download_hash, False))
        if not self._delete_cross_seeds:
            return result
        main_size = self._torrent_size(main_t) if main_t else 0
        main_name = self._torrent_name(main_t) if main_t else ""
        if not main_size or not main_name:
            return result
        for t in (torrents or []):
            h = getattr(t, "hash", None)
            if not h or h == download_hash:
                continue
            if self._torrent_size(t) != main_size:
                continue
            if self._torrent_name(t) != main_name:
                continue
            result.append((h, self._torrent_name(t) or h, True))
        return result

    def _delete_downloader_torrents(self, chain, download_hash, display_name, all_torrents=None):
        """删除主种子及其辅种（cross-seed），返回实际删除的种子数量。

        此处扫描整个下载器，包含非 MP 管理的种子，因此非 MP 管理的辅种也会被删除。
        """
        if not download_hash:
            logger.warning(f"SC 无 download_hash，跳过删种: {display_name}")
            return 0
        torrents = all_torrents if all_torrents is not None else self._get_cached_torrents(chain)
        to_delete = self._collect_torrents_to_delete(download_hash, torrents)
        cross_cnt = sum(1 for _, _, is_cross in to_delete if is_cross)
        logger.info(f"SC 准备删种 [{display_name}]: 主种子 1 个" +
                    (f"，辅种 {cross_cnt} 个" if cross_cnt else "，无辅种"))
        deleted = 0
        for h, name, is_cross in to_delete:
            role = "辅种" if is_cross else "主种子"
            try:
                chain.remove_torrents(hashs=h, delete_file=True)
                logger.info(f"SC   已删除{role}: {name} ({h})")
                deleted += 1
            except Exception as e:
                logger.error(f"SC   删除{role}失败 {name} ({h}): {str(e)}")
        return deleted

    @staticmethod
    def _torrent_size(t) -> int:
        """取种子总体积（字节），无法获取时返回 0。"""
        if t is None:
            return 0
        for attr in ("size", "total_size", "totalSize"):
            v = getattr(t, attr, None)
            if v:
                try:
                    return int(v)
                except (ValueError, TypeError):
                    continue
        return 0

    @staticmethod
    def _torrent_name(t) -> str:
        """取种子名称（用于辅种匹配），优先内容名 name，其次 title。"""
        if t is None:
            return ""
        return (getattr(t, "name", None) or getattr(t, "title", None) or "").strip()

    def _safe_delete_path(self, path: Path):
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
        except Exception:
            pass

    # 视频文件扩展名：目录内若含此类文件则不视为可清理的残留目录
    _VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".wmv", ".flv",
                   ".iso", ".rmvb", ".rm", ".mpg", ".mpeg", ".m4v", ".webm", ".vob", ".strm"}

    def _dir_is_leftover_metadata(self, path: Path) -> bool:
        """目录是否为「资源删除后残留的元数据目录」——可安全删除。

        判定为 True 的条件：目录内不含任何子目录，且不含任何视频文件；
        即只剩 nfo、海报/背景图、字幕、系统垃圾等刮削/元数据文件。
        这类目录（如某剧删除全部季后仅剩 tvshow.nfo、poster.jpg 的剧名根目录）
        应连同删除。若目录内仍有子目录（如同类目录下的其他剧集、其他季）
        或仍存在视频文件，则返回 False 以停止上溯，避免误删。
        """
        try:
            for e in path.iterdir():
                if e.is_dir():
                    return False  # 存在子目录（其他剧集/其他季），保留
                if e.is_file() and e.suffix.lower() in self._VIDEO_EXTS:
                    return False  # 仍有视频文件，保留
            return True
        except Exception:
            return False

    def _delete_media_dir(self, media_dir: Path, max_levels: int = 3):
        """删除媒体库中该资源所在目录（MP 软链接/硬链接、重命名、刮削生成的成品目录）。

        MP 通常为每部电影/每季电视剧建立独立目录，目录内除媒体文件外还含
        nfo、海报、fanart 等刮削文件；删除资源时应连同整个目录一并删除。
        电视剧的季目录（如 .../剧名 (2026) {tmdbid=x}/Season 1）删除后，
        若上层剧名目录随之变空，也一并向上清理（最多 max_levels 层），
        遇到仍有内容的目录（如同类目录下的其他剧集）、挂载点或根目录即停止。
        """
        try:
            if not media_dir or not media_dir.exists() or not media_dir.is_dir():
                return
            # 避免误删挂载点/根目录
            if media_dir.parent == media_dir or os.path.ismount(str(media_dir)):
                logger.warning(f"SC 跳过删除媒体库目录（疑似挂载点/根目录）: {media_dir}")
                return
            self._safe_delete_path(media_dir)
            logger.info(f"SC 已删除媒体库目录: {media_dir}")
            # 向上清理因删除季目录而残留的空目录（如剧名根目录）
            cur = media_dir.parent
            for _ in range(max_levels):
                if not cur or not cur.exists() or not cur.is_dir():
                    break
                if cur.parent == cur or os.path.ismount(str(cur)):
                    break
                if not self._dir_is_leftover_metadata(cur):
                    break  # 仍有子目录或视频文件（如同目录下别的剧集），停止上溯
                parent = cur.parent
                self._safe_delete_path(cur)
                logger.info(f"SC 已删除残留空目录: {cur}")
                cur = parent
        except Exception as e:
            logger.error(f"SC 删除媒体库目录失败 {media_dir}: {e}")

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

    # 删除记录缓存上限：保留最近 50 条，避免占用过多存储空间与读取开销
    _DELETE_HISTORY_MAX = 50

    def _add_delete_history(self, title: str, action: str):
        h = self.get_data("delete_history") or []
        h.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "title": title, "action": action})
        if len(h) > self._DELETE_HISTORY_MAX:
            h = h[-self._DELETE_HISTORY_MAX:]
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
        """洗版模式（一集一个槽位）：收集所有 URL 的 RSS 条目，播放进度低于阈值时触发洗版；
        同一集有多个版本时只下载最早发布的版本，已洗版下载过的集在后续刷新中不再重复下载。"""
        from collections import OrderedDict
        all_candidates = OrderedDict()  # dedup_key -> (item, m, meta, s_season, se_fmt, ts)
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
                m, meta, video_name = self._rss_id(item, t)
                if not m or not meta:
                    self._rss_log("识别失败", t)
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS识别失败",
                                          text=f"资源无法识别: {t}")
                    continue
                # 判断电视剧 / 电影，电视剧用 MP 剧集解析引擎重新提取季/集
                is_tv = (getattr(m, "type", None) == MediaType.TV) or (m.season is not None) or (meta.begin_episode is not None)
                if is_tv:
                    s_season, s_episode = self._rss_tv_season_episode(m, meta, video_name)
                else:
                    s_season, s_episode = None, None
                cr = self._rss_ck(m, meta, s_season, s_episode)
                if is_tv:
                    se_fmt = f"S{int(s_season):02d}E{int(s_episode):02d}" if s_episode else f"S{int(s_season):02d}"
                else:
                    se_fmt = "电影"
                # 洗版模式：播放进度低于阈值才触发洗版
                if cr["s"]:
                    # 已看完（进度 >= 阈值），跳过
                    self._rss_log("跳过", m.title, cr["r"])
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS跳过",
                                          text=f"{m.title} {se_fmt} {cr['r']}")
                    continue
                # 构造去重 key：电视剧按 tmdb+季+集，电影按 tmdb，缺字段时用 enclosure
                if is_tv and m.tmdb_id and s_episode:
                    dedup_key = (m.tmdb_id, int(s_season), int(s_episode))
                elif m.tmdb_id and not is_tv:
                    dedup_key = ("movie", m.tmdb_id)
                elif m.tmdb_id:
                    dedup_key = ("tmdb", m.tmdb_id, int(s_season))
                else:
                    dedup_key = ("enclosure", item.get("enclosure", "") or item.get("link", ""))
                # 一集一个槽位：该集在之前的刷新中已洗版下载过，则不再重复下载
                ep_key = self._rss_wash_key(dedup_key)
                if ep_key and ep_key in self._rss_washed:
                    self._rss_log("洗版跳过", m.title, f"{se_fmt} 已洗版下载过")
                    continue
                ts = self._rss_pubts(item)
                if dedup_key in all_candidates:
                    # 同一集有多个版本时，只保留发布时间最早的版本
                    if self._rss_earlier(ts, all_candidates[dedup_key][5]):
                        all_candidates[dedup_key] = (item, m, meta, s_season, se_fmt, ts)
                        self._rss_log("洗版替换", m.title, f"{se_fmt} 选用更早发布版本")
                    else:
                        self._rss_log("洗版去重跳过", m.title, f"{se_fmt} 已有更早版本")
                    continue
                all_candidates[dedup_key] = (item, m, meta, s_season, se_fmt, ts)
        logger.info(f"SC-RSS 报文处理完成：获取 {total_items} 条，过滤后剩余 {len(all_candidates)} 条待处理")
        # 统一下载去重后的条目
        dc = 0
        for dedup_key, payload in all_candidates.items():
            item, m, meta, s_season, se_fmt, ts = payload
            if self._rss_dl_add(item, m, meta):
                dc += 1
                ep_key = self._rss_wash_key(dedup_key)
                if ep_key:
                    self._rss_washed.add(ep_key)
                self._rss_log("下载", m.title)
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 已添加下载",
                                      text=self._rss_notify_text(item, meta, m, se_fmt))
            else:
                self._rss_log("下载失败", item.get("title", ""), "推送下载器失败")
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 添加失败",
                                      text=f"名称: {m.title} {se_fmt}")
        s = list(self._rss_seen)
        if len(s) > 2000:
            s = s[-2000:]
            self._rss_seen = set(s)
        self.save_data("rss_seen", s)
        w = list(self._rss_washed)
        if len(w) > 3000:
            w = w[-3000:]
            self._rss_washed = set(w)
        self.save_data("rss_washed", w)

    @staticmethod
    def _rss_wash_key(dedup_key) -> Optional[str]:
        """生成持久化洗版槽位键。
        电视剧 (tmdb_id, season, episode) -> "{tmdbid}:SxxExx"；
        电影 ("movie", tmdb_id) -> "{tmdbid}:M"；其余返回 None。"""
        if isinstance(dedup_key, tuple) and len(dedup_key) == 3 and not isinstance(dedup_key[0], str):
            return f"{dedup_key[0]}:S{int(dedup_key[1]):02d}E{int(dedup_key[2]):02d}"
        if isinstance(dedup_key, tuple) and len(dedup_key) == 2 and dedup_key[0] == "movie":
            return f"{dedup_key[1]}:M"
        return None

    @staticmethod
    def _rss_pubts(item: dict) -> float:
        """从 RSS 条目解析发布时间戳，无法解析时返回 +inf（视为最晚）。"""
        for key in ("pubdate", "pub_date", "published", "date", "updated"):
            v = item.get(key)
            if not v:
                continue
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt).timestamp()
                except (ValueError, TypeError):
                    continue
        return float("inf")

    @staticmethod
    def _rss_earlier(ts_a: float, ts_b: float) -> bool:
        """ts_a 是否比 ts_b 更早发布。"""
        return ts_a < ts_b

    def _rss_proc(self, url: str):
        """普通模式（未开启洗版）：不做 TMDB 识别，直接添加种子到下载器。
        去重由 _rss_seen（enclosure 集合，持久化）保证，避免重复添加同一个种子。"""
        items = RssHelper().parse(url)
        if not items:
            logger.info(f"SC-RSS 未获取到新报文: {url}")
            return
        url_new = 0
        url_filtered = 0
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
            # 未开启洗版：跳过 TMDB 识别，仅本地解析标题用于通知的类别/质量（不调用 TMDB）
            meta = MetaInfo(title=t)
            if self._rss_add_direct(item):
                self._rss_log("下载", t)
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 已添加下载",
                                      text=self._rss_notify_text(item, meta))
            else:
                self._rss_log("下载失败", t, "添加下载器失败")
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 添加失败", text=f"名称: {t}")
        logger.info(f"SC-RSS [{url}] 获取到 {url_new} 个新报文，过滤后剩余 {url_filtered} 个")
        s = list(self._rss_seen)
        if len(s) > 2000:
            s = s[-2000:]
            self._rss_seen = set(s)
        self.save_data("rss_seen", s)

    def _rss_ck(self, m, meta: MetaInfo, season: Optional[int] = None, episode: Optional[int] = None) -> dict:
        if self._media_cache_disabled:
            return {"s": False, "r": "媒体缓存关闭"}
        if not m.tmdb_id:
            return {"s": False, "r": "no tmdb"}
        # 电影：查 {tmdbid}:M 播放记录
        if getattr(m, "type", None) != MediaType.TV and season is None and episode is None:
            k = f"{m.tmdb_id}:M"
            with self._pb_lock:
                for r in self._pb:
                    if r.get("k") == k:
                        p = r.get("p", 0) or 0
                        return {"s": p >= self._rss_th, "r": f"{'≥' if p>=self._rss_th else '<'}{self._rss_th}%({p:.1f}%)"}
            return {"s": False, "r": "无记录(触发洗版)"}
        # 电视剧：优先用传入的季/集（MP 剧集解析结果），fallback 到 MediaInfo/MetaInfo
        s = season if season is not None else (m.season or meta.begin_season)
        e = episode if episode is not None else meta.begin_episode
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
        """洗版模式：下载种子文件，取其中的视频文件名做 TMDB 识别。
        recognize_media(cache=True) 会优先命中 MoviePilot 的 TMDB 识别缓存。
        返回 (media, meta, video_name)，video_name 为用于识别的视频文件名（basename），
        供后续电视剧用正则从原视频文件名提取季/集使用。"""
        enc = item.get("enclosure", "") or item.get("link", "")
        fns = self._rss_fnames(enc)
        if fns:
            for fn in fns:
                try:
                    # 用视频文件名（去掉目录）识别，命中率更高
                    base = fn.rsplit("/", 1)[-1]
                    meta = MetaInfo(title=base)
                    if meta.name and (media := self.chain.recognize_media(meta=meta, cache=True)):
                        return media, meta, base
                except Exception:
                    continue
        ct = re.sub(r'\[.*?\]', "", rt).strip()
        for c in [x.strip() for x in ct.split("/") if x.strip()] or [ct]:
            meta = MetaInfo(title=c, subtitle=item.get("description", ""))
            if meta.name and (media := self.chain.recognize_media(meta=meta, cache=True)):
                return media, meta, c
        return None, None, ""

    def _rss_tv_season_episode(self, m, meta, video_name: str):
        """电视剧洗版：解析季号与集号，仅用于洗版判重与“是否已看完”判断（不做实际文件重命名）。

        番剧在 TMDB 上常被合并为一季，导致季号/集号与实际发布不符。开启“CTMDbA 分季”后，
        会查询已安装的 CTMDbA 插件（CureTMDbAnime）的逻辑季集映射，把 TMDB 合并季/集
        翻译成实际的分季季号/集号，从而正确判断当前季是否已看完。

        优先级：CTMDbA 逻辑映射 > 文件名正则解析 > TMDB 识别结果(m.season) > RSS 标题解析(meta)。
        返回 (season:int, episode:int|None)。"""
        season = None
        episode = None
        # 1) 用正则从原视频文件名（去掉目录）解析季/集
        if video_name:
            season, episode = self._parse_se_from_name(video_name.rsplit("/", 1)[-1])
        # 2) 回退到 TMDB 识别结果与 RSS 标题解析结果
        if season is None:
            season = m.season if m.season is not None else meta.begin_season
        if season is None:
            season = 1
        if episode is None:
            episode = meta.begin_episode
        try:
            season = int(season)
        except (ValueError, TypeError):
            season = 1
        try:
            episode = int(episode) if episode is not None else None
        except (ValueError, TypeError):
            episode = None
        # 3) 开启 CTMDbA 分季：用 TMDB(合并)季集查逻辑(分季)季集
        if self._rss_ctmdba and episode is not None and getattr(m, "tmdb_id", None):
            logical = self._ctmdba_logical_se(int(m.tmdb_id), season, episode)
            if logical:
                ls, le = logical
                self._rss_log("CTMDbA分季", getattr(m, "title", ""),
                              f"S{season:02d}E{episode:02d} -> S{ls:02d}E{le:02d}")
                season, episode = ls, le
        return season, episode

    def _ctmdba_logical_se(self, tmdb_id: int, season: int, episode: int):
        """查询 CTMDbA 插件的逻辑季集映射，把 TMDB 合并季/集翻译为实际分季季/集。

        CTMDbA 启用后会在本地起代理服务（默认端口 8632），并提供映射接口
        /cache/mapping/{tmdb_id}，返回 {tmdb季: {tmdb集: {season, episode}}}。
        命中则返回 (逻辑季, 逻辑集)，否则返回 None。结果按运行缓存，避免重复请求。"""
        if not tmdb_id:
            return None
        cache = getattr(self, "_ctmdba_cache", None)
        if cache is None:
            cache = {}
            self._ctmdba_cache = cache
        if tmdb_id not in cache:
            mapping = {}
            try:
                url = f"http://127.0.0.1:{self._rss_ctmdba_port}/cache/mapping/{tmdb_id}"
                result = RequestUtils(timeout=3).get_json(url)
                if isinstance(result, dict):
                    for s_key, eps in result.items():
                        if not isinstance(eps, dict):
                            continue
                        try:
                            s_num = int(s_key)
                        except (TypeError, ValueError):
                            continue
                        for e_key, itm in eps.items():
                            if not isinstance(itm, dict):
                                continue
                            try:
                                e_num = int(e_key)
                                ls = int(itm.get("season"))
                                le = int(itm.get("episode"))
                            except (TypeError, ValueError):
                                continue
                            mapping[(s_num, e_num)] = (ls, le)
            except Exception as e:
                logger.debug(f"SC-RSS 查询 CTMDbA 映射失败 tmdb={tmdb_id}: {e}")
            cache[tmdb_id] = mapping
        return cache[tmdb_id].get((int(season), int(episode)))

    @staticmethod
    def _parse_se_from_name(name: str) -> Tuple[Optional[int], Optional[int]]:
        """用正则从视频文件名解析 (季, 集)。解析不到的部分返回 None。

        支持常见命名：S01E05 / s1.e5 / 1x05 / 第1季第5集 / 第5话 /
        Season 1 Episode 5 / EP05 / E05 / 动漫式 “- 05 ” 或 “[05]”（仅集号，季默认 1）。"""
        if not name:
            return None, None
        s = name
        # S01E05 / S1E5 / S01.E05 / S01 E05
        mm = re.search(r'[Ss](\d{1,2})[\s._\-]*[Ee](\d{1,3})(?!\d)', s)
        if mm:
            return int(mm.group(1)), int(mm.group(2))
        # 1x05 / 01x05
        mm = re.search(r'(?<!\d)(\d{1,2})x(\d{1,3})(?!\d)', s, re.IGNORECASE)
        if mm:
            return int(mm.group(1)), int(mm.group(2))
        # 中文：第1季 ... 第5集/话
        season = None
        ms = re.search(r'第\s*(\d{1,2})\s*季', s)
        if ms:
            season = int(ms.group(1))
        me = re.search(r'第\s*(\d{1,3})\s*[集话話]', s)
        if me:
            return (season if season is not None else 1), int(me.group(1))
        # Season 1 [Episode 5]
        ms = re.search(r'[Ss]eason\s*(\d{1,2})', s)
        if ms:
            season = int(ms.group(1))
        me = re.search(r'[Ee]pisode\s*(\d{1,3})(?!\d)', s)
        if me:
            return (season if season is not None else 1), int(me.group(1))
        # EP05 / EP.05 / E05（前后不接字母，避免误匹配单词）
        mm = re.search(r'(?<![A-Za-z])[Ee][Pp]?[\s._]*(\d{1,3})(?![A-Za-z0-9])', s)
        if mm:
            return (season if season is not None else 1), int(mm.group(1))
        # 动漫式：分隔符后的独立集号，如 “Title - 05 ” 或 “[05]”“【05】”
        # 排除分辨率/年份：不匹配 4 位数，且集号后不紧跟 p（如 720p）
        mm = re.search(r'(?:[-\[【]\s*)(\d{1,3})(?!\d)(?![Pp])\s*(?:[\]】]|[\s.\-]|$)', s)
        if mm:
            return (season if season is not None else 1), int(mm.group(1))
        return season, None

    # 视频文件扩展名（洗版识别时优先取这些文件的文件名）
    _VIDEO_EXTS = (".mp4", ".mkv", ".avi", ".ts", ".m2ts", ".wmv", ".mov",
                   ".flv", ".rmvb", ".rm", ".mpg", ".mpeg", ".webm", ".iso")

    @classmethod
    def _rss_fnames(cls, enc: str) -> List[str]:
        """解析种子文件的文件列表，仅返回视频文件（按体积从大到小），
        无视频文件时回退到全部文件名。"""
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
                all_files = []  # (path, length)
                for f in files:
                    parts = [p.decode("utf-8", errors="replace") if isinstance(p, bytes) else str(p) for p in f.get("path", [])]
                    if parts:
                        all_files.append(("/".join(parts), f.get("length", 0) or 0))
                # 优先取视频文件，按体积从大到小（正片通常最大）
                videos = [(p, l) for p, l in all_files if p.lower().endswith(cls._VIDEO_EXTS)]
                if videos:
                    videos.sort(key=lambda x: x[1], reverse=True)
                    return [p for p, _ in videos]
                return [p for p, _ in all_files]
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

    def _rss_add_direct(self, item: dict) -> bool:
        """未开启洗版模式：不做 TMDB 识别，直接把种子添加到下载器。
        通过下载器实例添加：磁链直接传 URL，种子文件先下载内容再添加。"""
        try:
            enc = item.get("enclosure", "") or item.get("link", "")
            if not enc:
                return False
            from app.helper.downloader import DownloaderHelper
            helper = DownloaderHelper()
            if self._rss_dl:
                svc = helper.get_service(name=self._rss_dl)
            else:
                svcs = helper.get_services() or {}
                svc = None
                for s in svcs.values():
                    if s.config and getattr(s.config, "enabled", True) and not s.instance.is_inactive():
                        svc = s
                        break
            if not svc or not svc.instance:
                logger.warn("SC-RSS 未找到可用下载器，无法直接添加种子")
                return False
            downloader = svc.instance
            content = enc
            # 非磁链：先下载 .torrent 文件内容
            if not enc.lower().startswith("magnet:"):
                r = RequestUtils(timeout=30).get_res(enc)
                if not r or r.status_code != 200:
                    logger.warn(f"SC-RSS 下载种子文件失败: {item.get('title', '')}")
                    return False
                content = r.content
            r = downloader.add_torrent(content=content, download_dir=self._rss_save_path or None)
            return bool(r)
        except Exception as e:
            logger.error(f"RSS direct add err {e}")
            return False

    @staticmethod
    def _rss_size_str(item: dict) -> str:
        sz = item.get("size", 0) or 0
        try:
            sz = float(sz)
        except (ValueError, TypeError):
            return ""
        if sz <= 0:
            return ""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if sz < 1024 or unit == "TB":
                return f"{sz:.2f} {unit}"
            sz /= 1024
        return ""

    def _rss_notify_text(self, item: dict, meta: MetaInfo, m=None, se_fmt: str = "") -> str:
        """通知正文：仅含类别、质量、大小、名称（不含描述）。"""
        # 类别：优先 TMDB 识别的媒体类型，否则用本地解析
        cat = ""
        if m is not None and getattr(m, "type", None):
            cat = m.type.value if hasattr(m.type, "value") else str(m.type)
        elif getattr(meta, "type", None):
            cat = meta.type.value if hasattr(meta.type, "value") else str(meta.type)
        quality = getattr(meta, "resource_pix", "") or getattr(meta, "edition", "") or ""
        size = self._rss_size_str(item)
        # 名称：TMDB 标题优先，否则用报文标题
        if m is not None and getattr(m, "title", None):
            name = f"{m.title} {se_fmt}".strip()
        else:
            name = item.get("title", "")
        lines = []
        if cat:
            lines.append(f"类别: {cat}")
        if quality:
            lines.append(f"质量: {quality}")
        if size:
            lines.append(f"大小: {size}")
        lines.append(f"名称: {name}")
        return "\n".join(lines)

    def _rss_log(self, a, title, r=""):
        # RSS下载历史不持久化，仅用于本次运行期间的日志记录
        pass

