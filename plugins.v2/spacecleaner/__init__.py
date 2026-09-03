"""
SpaceCleaner: 空间清理 + 智能RSS下载，共用播放进度缓存。
"""
import asyncio, json, os, re, time, threading, shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from app import schemas
from app.chain.download import DownloadChain
from app.chain.storage import StorageChain
from app.core.config import settings
from app.core.context import Context, MediaInfo, TorrentInfo
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.db.models.transferhistory import TransferHistory
from app.utils.system import SystemUtils
from app.chain import ChainBase
from app.helper.rss import RssHelper
from app.helper.rule import RuleHelper
from app.schemas.types import EventType, MediaType
from app.utils.http import RequestUtils


class RawTorrent:
    """从下载器原生接口取到的轻量种子对象，只保留删种与索引需要的字段。

    MoviePilot 统一接口 list_torrents 会对每个种子执行一次 MetaInfo 名称识别
    （要过全部自定义识别词），几百个种子即可占满一个 CPU 核数十秒。删除阶段
    只需要 hash / 名称 / 体积 / 路径 / 所属下载器，因此直接读原始数据即可。
    """
    __slots__ = ("hash", "name", "size", "save_path", "content_path", "path", "downloader")

    def __init__(self, torrent_hash: str, name: str, size: int,
                 save_path: str = "", content_path: str = "", downloader: str = ""):
        self.hash = torrent_hash
        self.name = name
        self.size = size
        self.save_path = save_path
        self.content_path = content_path
        self.path = content_path
        self.downloader = downloader


class SpaceCleaner(_PluginBase):
    plugin_name = "空间清理器"
    plugin_desc = "剩余空间不足时自动删除已观看资源（优先删除最早看完/标记的资源，电视剧按整理记录中该季最后一集看完即删整季，含辅种及同集/同片的不同版本，删种后一并删除媒体库文件及其所在目录）；智能RSS下载自动跳过已看完剧集，识别失败或季号不一致时可由智能助手接管识别并自动写入自定义识别词。"
    plugin_icon = "delete.png"
    plugin_version = "5.0.0"
    plugin_label = "系统工具"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin"
    plugin_config_prefix = "spacecleaner_"
    plugin_order = 10
    auth_level = 1

    # === 空间清理配置 ===
    _enabled = False
    _min_free_percent = 10
    _delete_by_target = False
    _target_free_percent = 20
    _delete_count = 1
    _check_cron = "0 */6 * * *"  # 执行周期（cron），默认每6小时的0分执行
    _dry_run = False
    _delete_other_versions = True  # 删种时检索整理记录，删除同一集/同一部电影的其他版本
    _delete_by_record = False  # 按媒体整理记录删除：优先删除整理记录中最早入库的已看资源
    _notify = True
    _pb_filter_watched = True  # 播放缓存默认筛选（详情页首次打开时生效，之后跟随页面视图状态）
    _watched_threshold = 85  # 标记已看播放进度阈值（%）
    _clean_downloader = []  # 空间清理扫描的下载器，空列表扫描全部

    # === RSS 下载配置 ===
    _rss_on = False
    _rss_cron = ""
    _rss_urls = ""
    _rss_dl = ""
    _rss_rule_group = ""  # RSS 下载使用的优先级规则组
    _rss_sz = ""
    _rss_inc = ""
    _rss_exc = ""
    _rss_once = False
    _rss_ntf = False
    _rss_th = 85
    _rss_wash_mode = False  # 洗版模式：播放进度低于阈值时触发洗版，只下载最早版本
    _rss_fname_identify = False  # 种子文件名兜底识别：报文识别失败/无集号/季号不一致时下载种子用文件名再识别
    _rss_ai_identify = False  # 智能助手识别兜底：识别失败/无集号/季号不一致时交给 LLM 接管识别
    _rss_ai_add_words = True  # 智能助手识别成功后自动写入自定义识别词，避免下次再失败
    _rss_ai_max = 5  # 单轮 RSS 刷新最多调用智能助手的次数
    _rss_proxy_retry = False  # 种子下载失败时使用系统代理服务器重试一次
    _rss_save_path = ""  # RSS 下载自定义保存路径

    # === 内部状态 ===
    _scheduler_thread = None
    _scheduler_running = False
    _scheduler_event = None
    _chain = None
    _running = False
    _run_lock = threading.Lock()  # 保护 _running / _rss_busy 的检查与置位
    _cached_space_info = None
    _cached_space_time = 0
    _space_cache_ttl = 10
    _all_torrents_cache = None
    _all_torrents_cache_time = 0
    _torrents_cache_ttl = 120
    _pb_cache = None
    _pb_cache_time = 0
    _pb_cache_ttl = 30
    # 连续删除多个资源时，单元之间的停顿秒数：删种、删文件、清目录会带来瞬时 CPU/IO 峰值
    _unit_delete_interval = 1.0
    _pb: List[dict] = []
    _pb_lock = threading.Lock()
    _HISTORY_MAX = 50  # 删除记录保留条数（只记录真实删除/失败，不记录试运行）
    _rss_s: Optional[BackgroundScheduler] = None
    _rss_busy = False
    # 去重容器用 dict 充当「有序集合」：保留插入顺序，裁剪时才能真正丢弃最早记录
    _rss_seen: Dict[str, None] = {}
    _rss_washed: Dict[str, None] = {}  # 已洗版下载过的集(tmdbid:SxxExx)，一集一个槽位
    _rss_seen_max = 2000
    _rss_washed_max = 3000
    _rss_lk = threading.Lock()
    _api_recognize_cache: List[dict] = []  # TMDB API 识别失败后的独立负缓存
    _api_recognize_cache_max = 5
    _api_recognize_success_cache: List[dict] = []  # TMDB API 识别成功后的独立正缓存
    _api_recognize_success_cache_max = 100
    _api_recognize_cache_lock = threading.Lock()
    # 智能助手识别兜底的单轮状态：调用计数与本轮已失败标题（避免同一轮重复烧 token）
    _rss_ai_calls = 0
    _rss_ai_failed: Dict[str, str] = {}
    _rss_ai_timeout = 90  # 单次智能助手调用超时（秒）

    @staticmethod
    def _to_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
        """安全地把配置值转为 int：无法解析时用默认值，并按需钳制范围。

        表单数字框可能回传空串、小数或非法字符，直接 int() 会抛异常导致 init_plugin 失败。
        """
        try:
            result = int(float(str(value).strip()))
        except (TypeError, ValueError, AttributeError):
            result = default
        if minimum is not None and result < minimum:
            result = minimum
        if maximum is not None and result > maximum:
            result = maximum
        return result

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._enabled = self._rss_on = False
        self._min_free_percent = 10
        self._target_free_percent = 20
        self._delete_by_target = self._dry_run = self._notify = False
        self._delete_other_versions = True
        self._delete_by_record = False
        self._pb_filter_watched = True
        self._watched_threshold = 85
        self._delete_count = 1
        self._check_cron = "0 */6 * * *"
        self._clean_downloader = []
        self._rss_cron = self._rss_urls = self._rss_sz = self._rss_inc = self._rss_exc = ""
        self._rss_dl = ""
        self._rss_rule_group = ""
        self._rss_once = self._rss_ntf = False
        self._rss_th = 85
        self._rss_wash_mode = False
        self._rss_fname_identify = False
        self._rss_ai_identify = False
        self._rss_ai_add_words = True
        self._rss_ai_max = 5
        self._rss_proxy_retry = False
        self._rss_save_path = ""
        self._pb = self._latest_episode_records(list(self.get_data("pb") or []))
        self.save_data("pb", self._pb)
        # 删除记录只保留真实删除/失败结果，历史遗留的试运行条目在这里一次性清掉
        self._migrate_delete_history()
        self._rss_seen = {}
        self._rss_washed = {}
        self._api_recognize_cache = self._load_api_recognize_cache()
        self._api_recognize_success_cache = self._load_api_recognize_success_cache()
        self._stop_rss_scheduler()

        if not config:
            return

        # 空间清理配置
        self._enabled = bool(config.get("enabled"))
        self._min_free_percent = self._to_int(config.get("min_free_percent"), 10, 1, 99)
        self._delete_by_target = bool(config.get("delete_by_target"))
        self._target_free_percent = self._to_int(config.get("target_free_percent"), 20, 1, 99)
        self._delete_count = self._to_int(config.get("delete_count"), 1, 1)
        self._check_cron = str(config.get("check_cron") or "").strip()
        if not self._check_cron:
            # 兼容旧版「执行周期（小时）」配置，迁移为等价 cron 表达式
            legacy_hours = str(config.get("check_interval") or "").strip()
            if legacy_hours.isdigit() and int(legacy_hours) > 0:
                hours = min(23, int(legacy_hours))
                self._check_cron = "0 * * * *" if hours == 1 else f"0 */{hours} * * *"
            else:
                self._check_cron = "0 */6 * * *"
        self._dry_run = bool(config.get("dry_run"))
        self._delete_other_versions = bool(config.get("delete_other_versions", True))
        self._delete_by_record = bool(config.get("delete_by_record"))
        self._notify = bool(config.get("notify", True))
        # 详情页视图状态（页签/分页/排序/筛选/搜索）持久化在插件数据 pb_view_state 中，这里只取默认筛选
        self._pb_filter_watched = bool(config.get("pb_filter_watched", True))
        self._watched_threshold = self._to_int(config.get("watched_threshold"), 85, 1, 100)
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
        self._rss_rule_group = str(config.get("rss_rule_group") or "")
        self._rss_sz = str(config.get("rss_sz") or "")
        self._rss_inc = str(config.get("rss_inc") or "")
        self._rss_exc = str(config.get("rss_exc") or "")
        self._rss_once = bool(config.get("rss_once"))
        self._rss_ntf = bool(config.get("rss_ntf", True))
        self._rss_th = self._to_int(config.get("rss_th"), 85, 1, 100)
        self._rss_seen = dict.fromkeys(self.get_data("rss_seen") or [])
        self._rss_washed = dict.fromkeys(self.get_data("rss_washed") or [])
        self._rss_wash_mode = bool(config.get("rss_wash_mode"))
        self._rss_fname_identify = bool(config.get("rss_fname_identify"))
        self._rss_ai_identify = bool(config.get("rss_ai_identify"))
        self._rss_ai_add_words = bool(config.get("rss_ai_add_words", True))
        self._rss_ai_max = self._to_int(config.get("rss_ai_max"), 5, 1, 50)
        self._rss_proxy_retry = bool(config.get("rss_proxy_retry"))
        self._rss_save_path = str(config.get("rss_save_path") or "")

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
                threading.Thread(target=self._rss_run, daemon=True, name="SC-RssOnce").start()
        if self._rss_on and self._rss_cron and self._rss_urls:
            # 重新注册前先停掉上一轮调度器，避免重复保存配置后多个调度器并发跑 RSS
            self._stop_rss_scheduler()
            try:
                trigger = CronTrigger.from_crontab(self._rss_cron)
            except Exception as exc:
                logger.error(f"SC-RSS 执行周期表达式无效（{self._rss_cron}）: {exc}，回退为 */30 * * * *")
                trigger = CronTrigger.from_crontab("*/30 * * * *")
            s = BackgroundScheduler(timezone=settings.TZ)
            s.add_job(self._rss_run, trigger)
            s.start()
            self._rss_s = s

    def _update_config(self):
        self.update_config({
            "enabled": self._enabled, "min_free_percent": self._min_free_percent,
            "delete_by_target": self._delete_by_target, "target_free_percent": self._target_free_percent,
            "delete_count": self._delete_count, "check_cron": self._check_cron,
            "dry_run": self._dry_run,
            "delete_other_versions": self._delete_other_versions, "notify": self._notify,
            "delete_by_record": self._delete_by_record,
            "run_now": False,
            "pb_filter_watched": self._pb_filter_watched, "watched_threshold": self._watched_threshold,
            "rss_on": self._rss_on, "rss_cron": self._rss_cron, "rss_urls": self._rss_urls,
            "rss_dl": self._rss_dl, "rss_rule_group": self._rss_rule_group,
            "rss_sz": self._rss_sz, "rss_inc": self._rss_inc,
            "rss_exc": self._rss_exc, "rss_once": self._rss_once, "rss_ntf": self._rss_ntf,
            "rss_th": self._rss_th, "rss_wash_mode": self._rss_wash_mode,
            "rss_fname_identify": self._rss_fname_identify,
            "rss_ai_identify": self._rss_ai_identify,
            "rss_ai_add_words": self._rss_ai_add_words,
            "rss_ai_max": self._rss_ai_max,
            "rss_proxy_retry": self._rss_proxy_retry,
            "rss_save_path": self._rss_save_path,
            "clean_downloader": self._clean_downloader,
        })

    def get_state(self) -> bool:
        return self._enabled or self._rss_on

    # ==================== Webhook 共用播放缓存 ====================

    @eventmanager.register(EventType.WebhookMessage)
    def on_webhook(self, event: Event) -> None:
        if not self._enabled and not self._rss_on:
            logger.debug(f"SC on_webhook skipped: enabled={self._enabled} rss_on={self._rss_on}")
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
            if sn > 0 and en > 0:
                # 同一电视剧只保留季集位置最靠后的播放缓存；旧集事件不覆盖新集。
                prefix = f"{tmdb}:S"
                latest_se = max(
                    ((int(r.get("s") or 0), int(r.get("e") or 0))
                     for r in self._pb if str(r.get("k", "")).startswith(prefix)),
                    default=(0, 0),
                )
                if latest_se > (sn, en):
                    logger.info(f"SC cached skipped old episode: {n} {se_display}")
                    return
                self._pb = [r for r in self._pb if not str(r.get("k", "")).startswith(prefix)]
            self._pb.append({"k": k, "n": n, "s": sn, "e": en, "p": pct, "t": ts})
        self.save_data("pb", self._pb)
        logger.info(f"SC cached: {n} {se_display} {pct:.1f}%")

    @staticmethod
    def _latest_episode_records(records: List[dict]) -> List[dict]:
        """按媒体压缩播放缓存，只保留季集位置最靠后的电视剧记录。"""
        latest = {}
        movies = []
        for record in records:
            match = re.match(r'^(\d+):S(\d+)E(\d+)$', str(record.get("k", "")))
            if not match:
                movies.append(record)
                continue
            tmdbid = int(match.group(1))
            season_episode = (int(match.group(2)), int(match.group(3)))
            current = latest.get(tmdbid)
            if current is None or season_episode > current[0]:
                latest[tmdbid] = (season_episode, record)
        return movies + [item[1] for item in latest.values()]

    # ==================== API ====================

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            # ---- 对外接口 ----
            {"path": "/dry_run", "endpoint": self.api_dry_run, "methods": ["GET"], "summary": "试运行"},
            {"path": "/run_now", "endpoint": self.api_run_now, "methods": ["GET"], "summary": "立即清理"},
            {"path": "/delete_history", "endpoint": self.api_delete_history, "methods": ["GET"], "summary": "删除历史"},
            {"path": "/space_info", "endpoint": self.api_space_info, "methods": ["GET"], "summary": "空间信息"},
            # ---- 详情页视图 ----
            {"path": "/refresh", "endpoint": self.page_refresh, "methods": ["GET"], "summary": "刷新详情页数据"},
            {"path": "/dt_tab", "endpoint": self.set_data_tab, "methods": ["GET"], "summary": "切换详情页页签"},
            {"path": "/notice_clear", "endpoint": self.clear_notice, "methods": ["GET"], "summary": "关闭详情页提示"},
            # ---- 播放缓存 ----
            {"path": "/pb_page", "endpoint": self.set_pb_page, "methods": ["GET"], "summary": "设置播放缓存页码"},
            {"path": "/pb_size", "endpoint": self.set_pb_size, "methods": ["GET"], "summary": "设置播放缓存每页条数"},
            {"path": "/pb_sort", "endpoint": self.set_pb_sort, "methods": ["GET"], "summary": "设置播放缓存排序"},
            {"path": "/pb_filter", "endpoint": self.set_pb_filter, "methods": ["GET"], "summary": "设置播放缓存筛选"},
            {"path": "/pb_filter_toggle", "endpoint": self.toggle_pb_filter, "methods": ["GET"], "summary": "切换已看完筛选"},
            {"path": "/pb_search", "endpoint": self.set_pb_search, "methods": ["GET"], "summary": "设置播放缓存搜索关键字"},
            {"path": "/pb_mark_watched", "endpoint": self.pb_mark_watched, "methods": ["GET"], "summary": "将单条播放记录标记为已看"},
            {"path": "/pb_mark_all_watched", "endpoint": self.pb_mark_all_watched, "methods": ["GET"], "summary": "将所有未看完记录标记为已看"},
            {"path": "/pb_toggle_prio", "endpoint": self.pb_toggle_prio, "methods": ["GET"], "summary": "切换播放记录优先删除标记"},
            {"path": "/del_pb_item", "endpoint": self.del_pb_item, "methods": ["GET"], "summary": "删除单条播放缓存"},
            {"path": "/clear_pb", "endpoint": self.clear_pb, "methods": ["GET"], "summary": "清除所有播放缓存"},
            # ---- 删除记录 ----
            {"path": "/dh_filter", "endpoint": self.set_dh_filter, "methods": ["GET"], "summary": "设置删除记录筛选"},
            {"path": "/dh_clear", "endpoint": self.clear_delete_history, "methods": ["GET"], "summary": "清空删除记录"},
            # ---- 快捷操作 ----
            {"path": "/quick_clean", "endpoint": self.quick_clean, "methods": ["GET"], "summary": "详情页立即清理"},
            # ---- RSS ----
            {"path": "/rss_run_once", "endpoint": self.rss_run_once, "methods": ["GET"], "summary": "立即刷新RSS"},
            {"path": "/rss_ca", "endpoint": self.rss_ca, "methods": ["GET"], "summary": "清除RSS已处理报文"},
            {"path": "/rss_wash_clear", "endpoint": self.rss_wash_clear, "methods": ["GET"], "summary": "清除洗版记录"},
            # ---- 识别缓存 ----
            {"path": "/cache_clear", "endpoint": self.cache_clear, "methods": ["GET"], "summary": "清空识别缓存"},
        ]

    # ---------- 详情页视图状态 ----------

    def page_refresh(self, apikey: str = ""):
        """刷新详情页：丢弃内部缓存，由前端重新拉取页面数据。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._invalidate_caches()
        return schemas.Response(success=True)

    def set_data_tab(self, tab: str = "", apikey: str = ""):
        """切换详情页页签。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if tab not in [t for t, _ in self._DATA_TABS]:
            return schemas.Response(success=False, message="无效页签")
        self._patch_view(tab=tab, arm=None)
        return schemas.Response(success=True)

    def clear_notice(self, apikey: str = ""):
        """关闭详情页顶部提示，同时取消待确认的危险操作。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._patch_view(notice=None, arm=None)
        return schemas.Response(success=True)

    def set_pb_page(self, page: int = 1, apikey: str = ""):
        """设置播放缓存当前页码。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._patch_view(page=self._to_int(page, 1, 1))
        return schemas.Response(success=True)

    def set_pb_size(self, size: int = 20, apikey: str = ""):
        """设置播放缓存每页条数。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        value = self._to_int(size, self._VIEW_DEFAULTS["size"])
        if value not in self._PB_SIZES:
            value = self._VIEW_DEFAULTS["size"]
        self._patch_view(size=value, page=1)
        return schemas.Response(success=True)

    def set_pb_sort(self, sort_by: str = "time", apikey: str = ""):
        """设置播放缓存排序字段；同字段再次点击切换升降序。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if sort_by not in self._PB_SORTS:
            return schemas.Response(success=False, message="无效排序字段")
        view = self._get_view()
        if view["sort_by"] == sort_by:
            self._patch_view(sort_desc=not view["sort_desc"], page=1)
        else:
            self._patch_view(sort_by=sort_by, sort_desc=True, page=1)
        return schemas.Response(success=True)

    def set_pb_filter(self, mode: str = "all", apikey: str = ""):
        """设置播放缓存筛选：全部 / 已看完 / 未看完 / 优先删除。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if mode not in [f for f, _ in self._PB_FILTERS]:
            return schemas.Response(success=False, message="无效筛选条件")
        self._patch_view(filter=mode, page=1)
        return schemas.Response(success=True)

    def toggle_pb_filter(self, apikey: str = ""):
        """兼容旧入口：在「已看完」与「全部」之间切换。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        view = self._get_view()
        self._patch_view(filter="all" if view["filter"] == "watched" else "watched", page=1)
        return schemas.Response(success=True)

    def set_pb_search(self, q: str = "", apikey: str = ""):
        """设置播放缓存搜索关键字（空串表示清除搜索）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._patch_view(search=(q or "").strip(), page=1)
        return schemas.Response(success=True)

    def set_dh_filter(self, mode: str = "all", apikey: str = ""):
        """设置删除记录筛选：全部 / 已删除 / 失败。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if mode not in [f for f, _ in self._HISTORY_FILTERS]:
            return schemas.Response(success=False, message="无效筛选条件")
        self._patch_view(dh_filter=mode)
        return schemas.Response(success=True)

    # ---------- 播放缓存操作 ----------

    def _sync_pb_from_data(self) -> None:
        """把内存播放缓存与插件数据对齐，避免不同插件实例互相覆盖。"""
        stored = self.get_data("pb")
        if not isinstance(stored, list):
            return
        with self._pb_lock:
            if stored != self._pb:
                self._pb = stored
                self._pb_cache = None

    def del_pb_item(self, k: str, apikey: str = ""):
        """删除单条播放缓存记录（不动媒体库文件）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._sync_pb_from_data()
        with self._pb_lock:
            before = len(self._pb)
            self._pb = [r for r in self._pb if r.get("k") != k]
            changed = len(self._pb) != before
            snapshot = list(self._pb)
        if changed:
            self.save_data("pb", snapshot)
            self._pb_cache = None
            logger.info(f"SC 删除单条缓存: {k}")
        return schemas.Response(success=True)

    def clear_pb(self, confirm: Any = None, apikey: str = ""):
        """清除所有播放缓存（需二次确认）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if self._need_confirm("clear_pb", confirm, "再次点击「确认清空缓存」将清除全部播放缓存"):
            return schemas.Response(success=True, message="等待确认")
        with self._pb_lock:
            self._pb = []
        self.save_data("pb", [])
        self._invalidate_caches()
        self._set_notice("已清除全部播放缓存", "success")
        logger.info("SC 播放缓存已清除")
        return schemas.Response(success=True)

    def pb_mark_watched(self, k: str, apikey: str = ""):
        """将单条未看完的播放记录标记为已看（进度置为100%）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._sync_pb_from_data()
        marked = False
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    r["p"] = 100.0
                    r["t"] = ts
                    marked = True
                    break
            snapshot = list(self._pb)
        if marked:
            self.save_data("pb", snapshot)
            self._pb_cache = None
            logger.info(f"SC 标记已看: {k}")
        return schemas.Response(success=True)

    def pb_toggle_prio(self, k: str, apikey: str = ""):
        """切换单条播放记录的优先删除标记。被标记的资源在空间清理时优先删除。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._sync_pb_from_data()
        new_state = None
        with self._pb_lock:
            for r in self._pb:
                if r.get("k") == k:
                    new_state = not bool(r.get("prio"))
                    r["prio"] = new_state
                    break
            snapshot = list(self._pb)
        if new_state is not None:
            self.save_data("pb", snapshot)
            self._pb_cache = None
            logger.info(f"SC 优先删除标记 {'开启' if new_state else '取消'}: {k}")
        return schemas.Response(success=True)

    def pb_mark_all_watched(self, apikey: str = ""):
        """将所有未看完的播放记录批量标记为已看（进度置为100%）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        self._sync_pb_from_data()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cnt = 0
        with self._pb_lock:
            for r in self._pb:
                if (r.get("p", 0) or 0) < self._watched_threshold:
                    r["p"] = 100.0
                    r["t"] = ts
                    cnt += 1
            snapshot = list(self._pb)
        if cnt:
            self.save_data("pb", snapshot)
            self._pb_cache = None
        self._set_notice(f"已把 {cnt} 条未看完记录标记为已看", "success" if cnt else "info")
        logger.info(f"SC 批量标记已看 {cnt} 条")
        return schemas.Response(success=True)

    # ---------- 删除记录 / 快捷操作 ----------

    def clear_delete_history(self, confirm: Any = None, apikey: str = ""):
        """清空删除记录（需二次确认，只删记录不影响已删除的文件）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if self._need_confirm("clear_history", confirm, "再次点击「确认清空记录」将清空全部删除记录"):
            return schemas.Response(success=True, message="等待确认")
        self.save_data("delete_history", [])
        self._set_notice("已清空删除记录", "success")
        return schemas.Response(success=True)

    def quick_clean(self, confirm: Any = None, apikey: str = ""):
        """详情页立即清理：需二次确认，后台线程执行，避免阻塞请求。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if self._need_confirm("clean", confirm,
                              "再次点击「确认立即清理」将按当前策略删除已看资源（含种子与媒体库文件）"):
            return schemas.Response(success=True, message="等待确认")
        with self._run_lock:
            busy = self._running
        if busy:
            self._set_notice("清理任务正在运行中，稍后刷新查看结果", "warning")
            return schemas.Response(success=True)
        threading.Thread(target=self._run_now_task, daemon=True, name="SC-PageClean").start()
        self._set_notice("已触发清理任务，稍后点击「刷新数据」查看删除记录", "success")
        return schemas.Response(success=True)

    # ---------- RSS 操作 ----------

    def rss_run_once(self, apikey: str = ""):
        """立即刷新一次 RSS。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if not (self._rss_on and self._rss_urls):
            self._set_notice("RSS 未启用或未配置链接", "warning")
            return schemas.Response(success=True)
        with self._run_lock:
            busy = self._rss_busy
        if busy:
            self._set_notice("上一轮 RSS 刷新仍在进行，稍后再试", "warning")
            return schemas.Response(success=True)
        threading.Thread(target=self._rss_run, daemon=True, name="SC-PageRss").start()
        self._set_notice("已触发 RSS 刷新，稍后点击「刷新数据」查看结果", "success")
        return schemas.Response(success=True)

    def rss_ca(self, confirm: Any = None, apikey: str = ""):
        """清除 RSS 已处理报文记录（需二次确认，清除后同一批报文会被重新处理）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if self._need_confirm("clear_seen", confirm,
                              "再次点击「确认清除已处理报文」将清空 RSS 去重记录，旧报文可能被重新下载"):
            return schemas.Response(success=True, message="等待确认")
        self.save_data("rss_seen", [])
        with self._rss_lk:
            self._rss_seen = {}
        self._set_notice("已清除 RSS 已处理报文记录", "success")
        return schemas.Response(success=True)

    def rss_wash_clear(self, confirm: Any = None, apikey: str = ""):
        """清除洗版记录（需二次确认，清除后同一集会被重新洗版下载）。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        if self._need_confirm("clear_washed", confirm,
                              "再次点击「确认清除洗版记录」后，已洗版的剧集会被重新下载"):
            return schemas.Response(success=True, message="等待确认")
        self.save_data("rss_washed", [])
        with self._rss_lk:
            self._rss_washed = {}
        self._set_notice("已清除洗版记录", "success")
        return schemas.Response(success=True)

    def cache_clear(self, kind: str = "all", apikey: str = ""):
        """清空识别缓存：success 正缓存 / negative 负缓存 / all 全部。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False)
        kind = (kind or "all").strip().lower()
        labels = {"success": "识别成功缓存", "negative": "识别失败缓存", "all": "识别缓存"}
        if kind not in labels:
            return schemas.Response(success=False, message="无效缓存类型")
        with self._api_recognize_cache_lock:
            if kind in ("success", "all"):
                self._api_recognize_success_cache = []
                self.save_data("api_recognize_success_cache", [])
            if kind in ("negative", "all"):
                self._api_recognize_cache = []
                self.save_data("api_recognize_cache", [])
        self._set_notice(f"已清空{labels[kind]}", "success")
        logger.info(f"SC 已清空{labels[kind]}")
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
        # 优先级规则组：供 RSS 下载过滤使用
        groups = []
        try:
            groups = [{"title": g.name, "value": g.name} for g in RuleHelper().get_rule_groups() or []]
        except Exception:
            pass

        def section(text: str, first: bool = False) -> dict:
            """小节标题，统一间距。"""
            cls = "text-caption text-medium-emphasis mb-1" if first else "text-caption text-medium-emphasis mb-1 mt-2"
            return {"component": "div", "props": {"class": cls}, "text": text}

        divider = {"component": "VDivider", "props": {"class": "my-3"}}

        # ---------- 空间清理 ----------
        clean_form = {
            "component": "VForm",
            "content": [
                section("基本设置", first=True),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "通知"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "dry_run", "label": "试运行模式", "hint": "仅在日志中显示将要删除的资源，不实际删除", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "run_now", "label": "立即运行一次"}}]},
                ]},
                divider,
                section("清理参数"),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "min_free_percent", "label": "删种触发阈值（%）", "type": "number", "min": 1, "max": 99, "hint": "剩余空间低于此值开始删除", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "target_free_percent", "label": "目标剩余百分比（%）", "type": "number", "min": 1, "max": 99, "hint": "配合「按目标百分比删除」使用", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "delete_count", "label": "单次删除资源数", "type": "number", "min": 1, "hint": "每次检查最多删除的资源数", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "watched_threshold", "label": "标记已看进度阈值（%）", "type": "number", "min": 1, "max": 100, "hint": "播放进度达到此值标记为已观看", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VCronField", "props": {"model": "check_cron", "label": "执行周期", "hint": "cron 表达式，如 0 */6 * * * 表示每 6 小时", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [{"component": "VSelect", "props": {"model": "clean_downloader", "label": "扫描下载器", "items": dls, "multiple": True, "chips": True, "clearable": True, "hint": "删种时扫描的下载器，留空扫描全部", "persistent-hint": True}}]},
                ]},
                divider,
                section("删除策略"),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "delete_by_target", "label": "按目标百分比删除", "hint": "持续删除资源直到剩余空间达到目标百分比", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "delete_other_versions", "label": "删除不同版本", "hint": "删种时检索整理记录，删除同一集/同一部电影的不同版本", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "delete_by_record", "label": "按媒体整理记录删除", "hint": "优先删除整理记录中最早入库的已看资源（否则按播放缓存最早看完时间）", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mb-0"},
                         "content": [{"component": "div", "props": {"class": "text-caption"}, "text": "「删除不同版本」：删种时会检索媒体整理记录，把同一集电视剧或同一部电影的其他版本（不同分辨率、编码、字幕组、发布组等）一并删除，包括它们对应的源文件、媒体库文件、下载器种子（含辅种）及整理记录。电视剧按 tmdbid + 季 + 集号匹配，电影按 tmdbid 匹配。"}]}
                    ]},
                ]},
            ],
        }

        # ---------- BT动漫RSS下载/洗版 ----------
        rss_form = {
            "component": "VForm",
            "content": [
                section("基本设置", first=True),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_on", "label": "启用"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_ntf", "label": "通知"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_once", "label": "立即刷新RSS"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_wash_mode", "label": "洗版模式", "hint": "播放进度低于阈值或无播放缓存时触发洗版，只下载最早发布的版本", "persistent-hint": True}}]},
                ]},
                divider,
                section("识别兜底"),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "rss_fname_identify", "label": "种子文件名兜底识别", "hint": "报文识别失败、无集号或季号不一致时，下载种子用视频文件名再识别", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "rss_ai_identify", "label": "智能助手识别兜底", "hint": "以上手段仍失败或季号不一致时，交给智能助手（LLM）接管识别", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "rss_ai_add_words", "label": "自动添加自定义识别词", "hint": "智能助手识别成功后写入识别词，下次由 MoviePilot 自行识别", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "rss_ai_max", "label": "智能助手单轮调用上限", "type": "number", "min": 1, "max": 50, "hint": "每轮刷新最多调用次数", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 9}, "content": [
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "class": "mb-0"},
                         "content": [{"component": "div", "props": {"class": "text-caption"}, "text": "识别顺序：RSS报文标题 → 种子文件名 → 智能助手。智能助手使用「设定-智能助手」里配置的模型，识别成功后会按 TMDB ID 校验，并（可选）把识别词写入「设定-自定义识别词」，下次相同命名由 MoviePilot 自行识别，不再消耗智能助手额度。"}]}
                    ]},
                ]},
                divider,
                section("下载参数"),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "rss_th", "label": "洗版播放进度阈值（%）", "type": "number", "min": 1, "max": 100}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [{"component": "VTextField", "props": {"model": "rss_sz", "label": "种子大小过滤（GB）", "placeholder": "1-10"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VCronField", "props": {"model": "rss_cron", "label": "执行周期"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSelect", "props": {"model": "rss_dl", "label": "下载器", "items": dls, "clearable": True}}]},
                ]},
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSelect", "props": {"model": "rss_rule_group", "label": "优先级规则组", "items": groups, "clearable": True, "hint": "留空不过滤", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 5}, "content": [{"component": "VTextField", "props": {"model": "rss_save_path", "label": "自定义保存路径", "placeholder": "留空使用默认路径", "hint": "支持 <storage>:<path> 格式", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [{"component": "VSwitch", "props": {"model": "rss_proxy_retry", "label": "代理重试", "hint": "取种失败时用系统代理重试一次", "persistent-hint": True}}]},
                ]},
                divider,
                section("RSS 源与过滤"),
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextarea", "props": {"model": "rss_urls", "label": "RSS链接", "rows": 4, "hint": "支持多个RSS链接，一行一个", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "props": {"dense": True}, "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "rss_inc", "label": "包含(正则)", "hint": "示例：字幕组A|字幕组B；| 表示“或”，不区分大小写", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "rss_exc", "label": "排除(正则)", "hint": "示例：合集|繁体|720p；命中即跳过该报文", "persistent-hint": True}}]},
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
            "delete_count": 1, "check_cron": "0 */6 * * *",
            "dry_run": False, "delete_other_versions": True, "delete_by_record": False, "notify": True,
            "clean_downloader": [], "run_now": False,
            "pb_filter_watched": True, "watched_threshold": 85,
            "rss_on": False, "rss_cron": "*/30 * * * *", "rss_urls": "",
            "rss_dl": "", "rss_rule_group": "", "rss_sz": "", "rss_inc": "", "rss_exc": "",
            "rss_once": False, "rss_ntf": True, "rss_th": 85, "rss_wash_mode": False,
            "rss_fname_identify": False, "rss_ai_identify": False, "rss_ai_add_words": True, "rss_ai_max": 5,
            "rss_proxy_retry": False, "rss_save_path": "",
        }

    # ==================== 详情页 ====================

    _DATA_TABS = (("pb", "播放缓存"), ("history", "删除记录"), ("rss", "RSS 记录"), ("cache", "识别缓存"))
    _PB_SIZES = (10, 20, 50)
    _PB_FILTERS = (("all", "全部"), ("watched", "已看完"), ("unwatched", "未看完"), ("prio", "优先"))
    _PB_SORTS = ("time", "title", "progress", "status")
    _HISTORY_FILTERS = (("all", "全部"), ("deleted", "已删除"), ("failed", "失败"))
    _TABLE_HEIGHT = "26rem"
    _ARM_TTL = 60  # 危险操作二次确认有效期（秒）
    _VIEW_DEFAULTS = {
        "tab": "pb",
        "page": 1,
        "size": 20,
        "sort_by": "time",
        "sort_desc": True,
        "filter": "",
        "search": "",
        "dh_filter": "all",
        "notice": None,
        "arm": None,
    }

    # ---------- 视图状态 ----------

    def _get_view(self) -> dict:
        """读取详情页视图状态。API 与页面渲染可能落在不同插件实例，状态一律存插件数据。"""
        raw = self.get_data("pb_view_state") or {}
        view = dict(self._VIEW_DEFAULTS)
        if isinstance(raw, dict):
            for key in view:
                if key in raw:
                    view[key] = raw[key]
        if view.get("tab") not in [t for t, _ in self._DATA_TABS]:
            view["tab"] = self._DATA_TABS[0][0]
        view["page"] = self._to_int(view.get("page"), 1, 1)
        view["size"] = view["size"] if view.get("size") in self._PB_SIZES else self._VIEW_DEFAULTS["size"]
        if view.get("sort_by") not in self._PB_SORTS:
            view["sort_by"] = "time"
        view["sort_desc"] = bool(view.get("sort_desc", True))
        if view.get("filter") not in [f for f, _ in self._PB_FILTERS]:
            # 从未手动切换过筛选时沿用配置里的默认值
            view["filter"] = "watched" if self._pb_filter_watched else "all"
        if view.get("dh_filter") not in [f for f, _ in self._HISTORY_FILTERS]:
            view["dh_filter"] = "all"
        view["search"] = str(view.get("search") or "").strip()
        return view

    def _patch_view(self, **kwargs) -> dict:
        """更新并持久化详情页视图状态。"""
        view = self._get_view()
        view.update(kwargs)
        self.save_data("pb_view_state", view)
        return view

    def _set_notice(self, text: str, level: str = "info") -> None:
        """在详情页顶部显示一条操作结果提示。"""
        self._patch_view(notice={"type": level, "text": text,
                                 "time": datetime.now().strftime("%H:%M:%S")})

    def _arm_pending(self, view: dict, action: str) -> bool:
        """该危险操作是否处于「等待二次确认」状态。"""
        arm = view.get("arm")
        if not isinstance(arm, dict) or arm.get("action") != action:
            return False
        try:
            return (time.time() - float(arm.get("ts") or 0)) <= self._ARM_TTL
        except (TypeError, ValueError):
            return False

    def _need_confirm(self, action: str, confirm: Any, text: str) -> bool:
        """危险操作二次确认：返回 True 表示本次只置位待确认状态，不执行动作。"""
        if str(confirm or "") == "1":
            self._patch_view(arm=None)
            return False
        self._patch_view(arm={"action": action, "ts": time.time()},
                         notice={"type": "warning", "text": f"{text}（{self._ARM_TTL} 秒内有效）",
                                 "time": datetime.now().strftime("%H:%M:%S")})
        return True

    # ---------- 渲染小工具 ----------

    def _page_api(self, path: str, **params) -> dict:
        """构造详情页点击事件；前端调用插件 API 后会自动重新拉取页面数据。"""
        query = {k: v for k, v in params.items() if v is not None}
        query["apikey"] = settings.API_TOKEN
        return {"click": {"api": f"plugin/{self.__class__.__name__}/{path}", "method": "get", "params": query}}

    @staticmethod
    def _fmt_gb(size_gb: Any) -> str:
        """按体积自动选择 GB / TB 单位。"""
        try:
            value = float(size_gb)
        except (TypeError, ValueError):
            return "-"
        return f"{value / 1024:.2f} TB" if value >= 1024 else f"{value:.1f} GB"

    @staticmethod
    def _next_run_text(cron: str) -> str:
        """按 cron 表达式计算下一次执行时间，供详情页显示。"""
        cron = (cron or "").strip()
        if not cron:
            return "未设置"
        try:
            trigger = CronTrigger.from_crontab(cron, timezone=settings.TZ)
            nxt = trigger.get_next_fire_time(None, datetime.now(trigger.timezone))
            return nxt.strftime("%m-%d %H:%M") if nxt else "未知"
        except Exception:
            return "表达式无效"

    @staticmethod
    def _chip(text: str, color: str = "default", icon: Optional[str] = None, variant: str = "tonal") -> dict:
        props = {"size": "small", "variant": variant, "color": color}
        if icon:
            props["prepend-icon"] = icon
        return {"component": "VChip", "props": props, "text": text}

    def _btn(self, text: str, path: str, color: str = "default", icon: Optional[str] = None,
             variant: str = "tonal", disabled: bool = False, **params) -> dict:
        props = {"size": "small", "variant": variant, "color": color, "class": "text-none"}
        if icon:
            props["prepend-icon"] = icon
        if disabled:
            props["disabled"] = True
        return {"component": "VBtn", "props": props, "text": text, "events": self._page_api(path, **params)}

    def _danger_btn(self, view: dict, action: str, path: str, text: str, icon: str) -> dict:
        """危险操作按钮：首次点击进入待确认状态，再次点击才真正执行。"""
        if self._arm_pending(view, action):
            return {"component": "VBtn",
                    "props": {"size": "small", "variant": "flat", "color": "error",
                              "prepend-icon": "mdi-alert-circle", "class": "text-none"},
                    "text": f"确认{text}", "events": self._page_api(path, confirm=1)}
        return {"component": "VBtn",
                "props": {"size": "small", "variant": "tonal", "color": "error",
                          "prepend-icon": icon, "class": "text-none"},
                "text": text, "events": self._page_api(path)}

    # 图标按钮：详情页渲染器总会给组件传入默认插槽，Vuetify 的 VBtn 只在「没有默认插槽」
    # 时才渲染 icon 属性，独立 VIcon 节点也会把插槽里的空文本当成图标名，两者都会渲染成空白。
    # 唯一可靠的方式是用 VBtn 自己生成的 prepend-icon（内部 VIcon 不带插槽），
    # 再用内联样式压掉 min-width/padding，避免图标按钮撑开表格列。
    _ICON_BTN_STYLE = "min-width: 0; padding: 0 5px;"

    def _icon_btn(self, icon: str, title: str, path: str, color: str = "default",
                  disabled: bool = False, **params) -> dict:
        props = {"size": "small", "variant": "text", "color": color, "title": title,
                 "prepend-icon": icon, "class": "text-none", "style": self._ICON_BTN_STYLE}
        if disabled:
            props["disabled"] = True
        return {"component": "VBtn", "props": props, "events": self._page_api(path, **params)}

    def _icon_link(self, icon: str, title: str, href: str) -> dict:
        """图标外链按钮（新窗口打开，不触发插件 API）。"""
        return {"component": "VBtn",
                "props": {"size": "small", "variant": "text", "title": title, "prepend-icon": icon,
                          "class": "text-none", "style": self._ICON_BTN_STYLE,
                          "href": href, "target": "_blank"}}

    @staticmethod
    def _caption(text: str, cls: str = "") -> dict:
        return {"component": "div", "props": {"class": ("text-caption text-medium-emphasis " + cls).strip()},
                "text": text}

    @staticmethod
    def _tile(label: str, value: str, sub: str = "", color: str = "") -> dict:
        """概览小卡片。"""
        value_cls = "text-subtitle-1 font-weight-bold"
        if color:
            value_cls += f" text-{color}"
        content = [{"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": label},
                   {"component": "div", "props": {"class": value_cls}, "text": value}]
        if sub:
            content.append({"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": sub})
        return {"component": "VCol", "props": {"cols": 6, "md": 3},
                "content": [{"component": "VCard", "props": {"variant": "tonal", "class": "pa-2 h-100"},
                             "content": content}]}

    def _table(self, headers: List[dict], items: List[dict], empty: str) -> dict:
        """只读表格。

        详情页渲染器总会传入默认插槽，VDataTable 的表体会被这个空插槽顶掉，
        因此这里统一用 VDataTableVirtual（内部走 wrapper 插槽），并保留表头排序。
        """
        props = {"headers": headers, "items": items, "density": "compact", "fixed-header": True,
                 "hover": True, "class": "text-body-2", "no-data-text": empty}
        if len(items) > 10:
            props["height"] = self._TABLE_HEIGHT
        return {"component": "VDataTableVirtual", "props": props}

    # ---------- 数据源 ----------

    def _page_pb_items(self) -> List[dict]:
        """详情页播放缓存数据源：以插件数据为准，避免渲染到过期的内存副本。"""
        stored = self.get_data("pb")
        raw = stored if isinstance(stored, list) else self._get_playback_pb()
        items = []
        for record in raw or []:
            if not isinstance(record, dict):
                continue
            try:
                progress = float(record.get("p", 0) or 0)
            except (TypeError, ValueError):
                progress = 0.0
            season = self._to_int(record.get("s"), 0, 0)
            episode = self._to_int(record.get("e"), 0, 0)
            name = self._normalize_cached_name(str(record.get("n") or ""), season, episode)
            title = re.sub(r"\s+S\d{2}E\d{2}\s*.*$", "", name).strip() or name
            key = str(record.get("k") or "")
            tmdb_id = key.split(":")[0] if ":" in key else ""
            is_episode = season > 0 and episode > 0
            items.append({
                "k": key,
                "title": title,
                "se": f"S{season:02d}E{episode:02d}" if is_episode else "电影",
                "is_movie": not is_episode,
                "tmdb_id": tmdb_id if tmdb_id.isdigit() else "",
                "progress": progress,
                "watched": progress >= self._watched_threshold,
                "time": str(record.get("t") or ""),
                "prio": bool(record.get("prio")),
            })
        return items

    def _page_history_items(self) -> List[dict]:
        """删除记录，最新的排在前面。"""
        items = []
        for record in self._get_delete_history() or []:
            if not isinstance(record, dict):
                continue
            items.append({"time": str(record.get("time") or ""),
                          "title": str(record.get("title") or ""),
                          "action": str(record.get("action") or "")})
        items.reverse()
        return items

    @staticmethod
    def _page_titles(pb_items: List[dict], success_cache: List[dict]) -> Dict[str, str]:
        """tmdbid -> 标题，用于把洗版记录里的纯 ID 显示成可读标题。"""
        titles: Dict[str, str] = {}
        for item in success_cache or []:
            tmdb_id = str(item.get("tmdb_id") or "")
            title = str(item.get("title") or "").strip()
            if tmdb_id and title:
                titles[tmdb_id] = title
        for item in pb_items or []:
            if item.get("tmdb_id") and item.get("title"):
                titles.setdefault(item["tmdb_id"], item["title"])
        return titles

    def _page_washed_items(self, titles: Dict[str, str]) -> List[dict]:
        """洗版记录，最新的排在前面。"""
        items = []
        for key in reversed(list(self.get_data("rss_washed") or [])):
            raw = str(key or "")
            if not raw:
                continue
            tmdb_id, _, se = raw.partition(":")
            items.append({"title": titles.get(tmdb_id) or f"TMDB {tmdb_id}",
                          "se": "电影" if se.upper() == "M" else (se or "-"),
                          "tmdb": tmdb_id})
        return items

    # ---------- 页面 ----------

    def get_page(self) -> Optional[List[dict]]:
        view = self._get_view()
        pb_items = self._page_pb_items()
        history = self._page_history_items()
        success_cache = self._load_api_recognize_success_cache()
        negative_cache = self._load_api_recognize_cache()
        washed = self._page_washed_items(self._page_titles(pb_items, success_cache))
        seen_count = len(self.get_data("rss_seen") or [])
        counts = {"pb": len(pb_items), "history": len(history), "rss": len(washed),
                  "cache": len(success_cache) + len(negative_cache)}

        if view["tab"] == "history":
            body = self._page_tab_history(view, history)
        elif view["tab"] == "rss":
            body = self._page_tab_rss(view, washed, seen_count)
        elif view["tab"] == "cache":
            body = self._page_tab_cache(view, success_cache, negative_cache)
        else:
            body = self._page_tab_pb(view, pb_items)

        cards = []
        notice = self._page_notice(view)
        if notice:
            cards.append(notice)
        cards.append(self._page_overview(view))
        cards.append({"component": "VCard", "props": {"variant": "flat", "class": "mt-2"},
                      "content": [self._page_tabs(view, counts),
                                  {"component": "VDivider"},
                                  {"component": "VCardText", "props": {"class": "pa-0"}, "content": body}]})
        return cards

    def _page_notice(self, view: dict) -> Optional[dict]:
        """顶部操作结果提示。"""
        notice = view.get("notice")
        if not isinstance(notice, dict) or not notice.get("text"):
            return None
        level = notice.get("type") if notice.get("type") in ("success", "info", "warning", "error") else "info"
        stamp = str(notice.get("time") or "")
        return {"component": "VAlert",
                "props": {"type": level, "variant": "tonal", "density": "compact", "class": "mb-2"},
                "content": [{"component": "div",
                             "props": {"class": "d-flex align-center justify-space-between ga-2"},
                             "content": [
                                 {"component": "div",
                                  "props": {"class": "text-body-2", "style": "white-space: pre-wrap;"},
                                  "text": f"{stamp} {notice.get('text')}".strip()},
                                 {"component": "VBtn",
                                  "props": {"variant": "text", "size": "x-small", "class": "text-none"},
                                  "text": "关闭", "events": self._page_api("notice_clear")},
                             ]}]}

    def _page_overview(self, view: dict) -> dict:
        """顶部概览：空间水位 + 运行状态 + 快捷操作。"""
        content: List[dict] = []
        space = self._get_space_info()
        if space:
            free_pct = space["free_percent"]
            color = ("error" if free_pct < self._min_free_percent
                     else "warning" if free_pct < self._target_free_percent else "success")
            content.append({"component": "div",
                            "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2 mb-2"},
                            "content": [
                                {"component": "div", "content": [
                                    self._caption("磁盘剩余空间"),
                                    {"component": "div", "props": {"class": "d-flex align-baseline ga-2"}, "content": [
                                        {"component": "span", "props": {"class": "text-h5 font-weight-bold"},
                                         "text": self._fmt_gb(space["free_gb"])},
                                        self._caption(f"/ 总计 {self._fmt_gb(space['total_gb'])}"
                                                      f" · 已用 {self._fmt_gb(space['used_gb'])}"),
                                    ]},
                                ]},
                                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": [
                                    self._chip(f"剩余 {free_pct:.1f}%", color=color, variant="flat"),
                                    self._chip(f"触发 {self._min_free_percent}%"),
                                    self._chip(f"目标 {self._target_free_percent}%"),
                                ]},
                            ]})
            content.append({"component": "VProgressLinear",
                            "props": {"modelValue": max(0.0, min(100.0, 100 - free_pct)), "color": color,
                                      "height": 10, "rounded": True, "class": "mb-3"}})
        else:
            content.append({"component": "VAlert",
                            "props": {"type": "warning", "variant": "tonal", "density": "compact", "class": "mb-3"},
                            "text": "无法获取磁盘空间信息，请检查媒体库目录配置"})

        clean_next = self._next_run_text(self._check_cron) if self._enabled else "已停用"
        rss_next = self._next_run_text(self._rss_cron) if self._rss_on else "已停用"
        status_chips = [
            self._chip(f"清理 {'启用' if self._enabled else '停用'} · 下次 {clean_next}",
                       color="success" if self._enabled else "default", icon="mdi-broom"),
            self._chip(f"RSS {'启用' if self._rss_on else '停用'} · 下次 {rss_next}",
                       color="primary" if self._rss_on else "default", icon="mdi-rss"),
            self._chip(f"{'洗版模式' if self._rss_wash_mode else '普通模式'}"
                       f"{' · 文件名兜底' if self._rss_fname_identify else ''}"
                       f"{' · 智能助手' if self._rss_ai_identify else ''}", icon="mdi-magnify-scan"),
            self._chip(f"扫描下载器 {'、'.join(self._clean_downloader) if self._clean_downloader else '全部'}",
                       icon="mdi-harddisk"),
            self._chip(f"单次删除 {self._delete_count} 个 · 已看阈值 {self._watched_threshold}%", icon="mdi-tune"),
        ]
        if self._delete_by_target:
            status_chips.append(self._chip(f"删至剩余 {self._target_free_percent}%", color="info",
                                           icon="mdi-target"))
        if self._dry_run:
            status_chips.append(self._chip("试运行模式（不实际删除）", color="warning", icon="mdi-flask-outline"))
        content.append({"component": "div", "props": {"class": "d-flex flex-wrap ga-2"}, "content": status_chips})
        content.append({"component": "VDivider", "props": {"class": "my-3"}})
        content.append({"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": [
            self._btn("刷新数据", "refresh", icon="mdi-refresh"),
            self._danger_btn(view, "clean", "quick_clean", "立即清理", "mdi-delete-sweep"),
            self._btn("刷新RSS", "rss_run_once", color="primary", icon="mdi-rss",
                      disabled=not (self._rss_on and bool(self._rss_urls))),
        ]})
        content.append(self._caption("播放进度依赖媒体服务器 Webhook，配置方法见 "
                                     "github.com/cudamin/MoviePilot-Plugins", cls="mt-3"))
        return {"component": "VCard", "props": {"variant": "flat"},
                "content": [{"component": "VCardText", "props": {"class": "pa-4"}, "content": content}]}

    def _page_tabs(self, view: dict, counts: Dict[str, int]) -> dict:
        tabs = []
        for key, label in self._DATA_TABS:
            count = counts.get(key) or 0
            tabs.append({"component": "VTab", "props": {"value": key, "class": "text-none"},
                         "text": f"{label} {count}" if count else label,
                         "events": self._page_api("dt_tab", tab=key)})
        return {"component": "VTabs",
                "props": {"modelValue": view["tab"], "color": "primary", "density": "comfortable", "grow": True},
                "content": tabs}

    # ---------- 播放缓存 ----------

    def _page_tab_pb(self, view: dict, records: List[dict]) -> List[dict]:
        watched_total = sum(1 for r in records if r["watched"])
        prio_total = sum(1 for r in records if r["prio"])
        counts = {"all": len(records), "watched": watched_total,
                  "unwatched": len(records) - watched_total, "prio": prio_total}

        mode = view["filter"]
        items = list(records)
        if mode == "watched":
            items = [r for r in items if r["watched"]]
        elif mode == "unwatched":
            items = [r for r in items if not r["watched"]]
        elif mode == "prio":
            items = [r for r in items if r["prio"]]
        search = view["search"]
        if search:
            keyword = search.lower()
            items = [r for r in items
                     if keyword in r["title"].lower() or keyword in r["se"].lower()]

        sort_by, desc = view["sort_by"], view["sort_desc"]
        if sort_by == "title":
            items.sort(key=lambda x: x["title"], reverse=desc)
        elif sort_by == "progress":
            items.sort(key=lambda x: x["progress"], reverse=desc)
        elif sort_by == "status":
            items.sort(key=lambda x: (x["watched"], x["title"]), reverse=desc)
        else:
            items.sort(key=lambda x: x["time"], reverse=desc)

        size = view["size"]
        total_pages = max(1, (len(items) + size - 1) // size)
        page = min(max(1, view["page"]), total_pages)
        page_items = items[(page - 1) * size: page * size]

        # 工具栏：筛选 / 每页条数 / 搜索 / 批量操作
        filter_chips = []
        for key, label in self._PB_FILTERS:
            active = mode == key
            filter_chips.append({"component": "VChip",
                                 "props": {"size": "small", "variant": "flat" if active else "tonal",
                                           "color": "primary" if active else "default"},
                                 "text": f"{label} {counts.get(key, 0)}",
                                 "events": self._page_api("pb_filter", mode=key)})
        size_chips = [self._caption("每页", cls="mr-1")]
        for value in self._PB_SIZES:
            active = size == value
            size_chips.append({"component": "VChip",
                               "props": {"size": "small", "variant": "flat" if active else "text",
                                         "color": "primary" if active else "default"},
                               "text": str(value),
                               "events": self._page_api("pb_size", size=value)})
        toolbar = [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 pt-3"}, "content": [
                {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": filter_chips},
                {"component": "div", "props": {"style": "flex: 1 1 auto;"}},
                {"component": "div", "props": {"class": "d-flex align-center ga-1"}, "content": size_chips},
            ]},
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 py-2"}, "content": [
                {"component": "div", "props": {"style": "flex: 1 1 220px; min-width: 180px;"},
                 "html": self._page_search_html(search)},
                {"component": "VBtn", "props": {"id": "sc-pb-refresh", "style": "display:none;",
                                                "variant": "text", "size": "x-small"},
                 "text": "刷新", "events": self._page_api("refresh")},
                self._btn("全部标记已看", "pb_mark_all_watched", color="success", icon="mdi-check-all",
                          disabled=(len(records) - watched_total) <= 0),
                self._danger_btn(view, "clear_pb", "clear_pb", "清空缓存", "mdi-delete-outline"),
            ]},
        ]

        def head(field: str, label: str, style: str) -> dict:
            active = view["sort_by"] == field
            arrow = (" ↓" if desc else " ↑") if active else ""
            return {"component": "VBtn",
                    "props": {"variant": "text", "size": "x-small",
                              "color": "primary" if active else "default",
                              "class": "px-1 text-none font-weight-medium",
                              "style": f"{style} justify-content: flex-start; letter-spacing: 0; font-size: inherit;"},
                    "text": label + arrow, "events": self._page_api("pb_sort", sort_by=field)}

        rows = [{"component": "div",
                 "props": {"class": "d-flex align-center px-3 py-1 text-caption text-medium-emphasis border-b"},
                 "content": [
                     head("title", "标题", "flex: 1 1 auto; min-width: 0;"),
                     {"component": "div", "props": {"style": "width: 84px;"}, "text": "季集"},
                     head("progress", "进度", "width: 120px;"),
                     head("status", "状态", "width: 78px;"),
                     head("time", "时间", "width: 110px;"),
                     {"component": "div", "props": {"style": "width: 150px;"}, "text": "操作"},
                 ]}]
        for item in page_items:
            rows.append(self._page_pb_row(item))
        if not page_items:
            rows.append({"component": "div",
                         "props": {"class": "pa-8 text-center text-body-2 text-medium-emphasis"},
                         "text": "没有符合条件的播放缓存" if records else
                                 "暂无播放缓存，媒体服务器上报播放进度后会自动出现"})

        summary = f"筛选后 {len(items)} 条 / 全部 {len(records)} 条"
        if search:
            summary += f" · 搜索「{search}」"
        pager = [
            self._icon_btn("mdi-page-first", "首页", "pb_page", disabled=page <= 1, page=1),
            self._icon_btn("mdi-chevron-left", "上一页", "pb_page", disabled=page <= 1,
                           page=max(1, page - 1)),
            {"component": "VChip", "props": {"size": "small", "variant": "tonal"}, "text": f"{page}/{total_pages}"},
            self._icon_btn("mdi-chevron-right", "下一页", "pb_page", disabled=page >= total_pages,
                           page=min(total_pages, page + 1)),
            self._icon_btn("mdi-page-last", "末页", "pb_page", disabled=page >= total_pages,
                           page=total_pages),
        ]
        footer = {"component": "div",
                  "props": {"class": "d-flex flex-wrap align-center justify-space-between ga-2 px-3 py-2 border-t"},
                  "content": [self._caption(summary),
                              {"component": "div", "props": {"class": "d-flex align-center ga-1"}, "content": pager}]}
        return toolbar + [{"component": "div", "content": rows}, footer]

    def _page_pb_row(self, item: dict) -> dict:
        """单条播放缓存行：标题 / 季集 / 进度 / 状态 / 时间 / 操作。"""
        progress = item["progress"]
        bar_color = "success" if item["watched"] else "primary" if progress > 0 else "grey"
        title_content = []
        if item["prio"]:
            title_content.append({"component": "span",
                                  "props": {"class": "text-warning mr-1", "title": "已标记优先删除"},
                                  "text": "★"})
        title_content.append({"component": "span", "props": {"class": "text-truncate"}, "text": item["title"]})
        ops = [
            self._icon_btn("mdi-check-circle-outline", "标记为已看", "pb_mark_watched",
                           color="success", disabled=item["watched"], k=item["k"]),
            self._icon_btn("mdi-star" if item["prio"] else "mdi-star-outline",
                           "取消优先删除" if item["prio"] else "标记优先删除", "pb_toggle_prio",
                           color="warning" if item["prio"] else "default", k=item["k"]),
            self._icon_btn("mdi-close-circle-outline", "删除这条播放缓存", "del_pb_item",
                           color="error", k=item["k"]),
        ]
        if item["tmdb_id"]:
            media_path = "movie" if item["is_movie"] else "tv"
            ops.append(self._icon_link("mdi-open-in-new", "在 TMDB 中查看",
                                       f"https://www.themoviedb.org/{media_path}/{item['tmdb_id']}"))
        stamp = item["time"]
        return {"component": "div", "props": {"class": "d-flex align-center px-3 py-1 border-t text-body-2"}, "content": [
            {"component": "div",
             "props": {"class": "d-flex align-center", "title": item["title"],
                       "style": "flex: 1 1 auto; min-width: 0; overflow: hidden;"},
             "content": title_content},
            {"component": "div", "props": {"style": "width: 84px;", "class": "text-caption"}, "text": item["se"]},
            {"component": "div", "props": {"style": "width: 120px;", "class": "d-flex align-center ga-2 pr-2"},
             "content": [
                 {"component": "VProgressLinear",
                  "props": {"modelValue": max(0.0, min(100.0, progress)), "color": bar_color, "height": 6,
                            "rounded": True, "style": "min-width: 40px;"}},
                 {"component": "span", "props": {"class": "text-caption text-medium-emphasis"},
                  "text": f"{progress:.0f}%"},
             ]},
            {"component": "div", "props": {"style": "width: 78px;"}, "content": [
                {"component": "VChip",
                 "props": {"size": "x-small", "variant": "tonal", "color": "success" if item["watched"] else "default"},
                 "text": "已看完" if item["watched"] else "未看完"}]},
            {"component": "div",
             "props": {"style": "width: 110px;", "class": "text-caption text-medium-emphasis", "title": stamp},
             "text": stamp[5:16] if len(stamp) >= 16 else stamp},
            {"component": "div", "props": {"style": "width: 150px;", "class": "d-flex align-center"}, "content": ops},
        ]}

    def _page_search_html(self, search: str) -> str:
        """搜索框：详情页事件只支持静态参数，输入值先提交到搜索 API，再触发隐藏按钮刷新页面。"""
        escaped = (search or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        token = settings.API_TOKEN
        plugin_id = self.__class__.__name__
        submit_js = (
            "var i=document.getElementById('sc-pb-q');"
            f"fetch('/api/v1/plugin/{plugin_id}/pb_search?q='+encodeURIComponent(i?i.value:'')+'&apikey={token}')"
            ".finally(function(){var b=document.getElementById('sc-pb-refresh');if(b){b.click();}});"
        )
        clear_js = (
            "var i=document.getElementById('sc-pb-q');if(i){i.value='';}"
            f"fetch('/api/v1/plugin/{plugin_id}/pb_search?q=&apikey={token}')"
            ".finally(function(){var b=document.getElementById('sc-pb-refresh');if(b){b.click();}});"
        )
        return (
            '<div style="display:flex;align-items:center;gap:8px;width:100%;">'
            f'<input id="sc-pb-q" type="text" placeholder="搜索标题或季集，回车确认" value="{escaped}" '
            'style="flex:1 1 auto;min-width:100px;padding:5px 10px;border:1px solid rgba(128,128,128,.45);'
            'border-radius:6px;font-size:12px;background:transparent;color:inherit;outline:none;" '
            f'onkeydown="if(event.key===\'Enter\'){{event.preventDefault();{submit_js}}}">'
            '<button type="button" style="padding:5px 14px;border-radius:6px;font-size:12px;border:none;cursor:pointer;'
            'background:rgb(var(--v-theme-primary));color:#fff;white-space:nowrap;" '
            f'onclick="{submit_js}">搜索</button>'
            '<button type="button" style="padding:5px 12px;border-radius:6px;font-size:12px;cursor:pointer;'
            'border:1px solid rgba(128,128,128,.45);background:transparent;color:inherit;white-space:nowrap;" '
            f'onclick="{clear_js}">清除</button>'
            '</div>'
        )

    # ---------- 删除记录 ----------

    def _page_tab_history(self, view: dict, history: List[dict]) -> List[dict]:
        failed_total = sum(1 for h in history if "失败" in h["action"])
        counts = {"all": len(history), "deleted": len(history) - failed_total, "failed": failed_total}
        mode = view["dh_filter"]
        if mode == "deleted":
            items = [h for h in history if "失败" not in h["action"]]
        elif mode == "failed":
            items = [h for h in history if "失败" in h["action"]]
        else:
            items = list(history)

        chips = []
        for key, label in self._HISTORY_FILTERS:
            active = mode == key
            chips.append({"component": "VChip",
                          "props": {"size": "small", "variant": "flat" if active else "tonal",
                                    "color": "primary" if active else "default"},
                          "text": f"{label} {counts.get(key, 0)}",
                          "events": self._page_api("dh_filter", mode=key)})
        latest = history[0]["time"] if history else "-"
        toolbar = {"component": "div",
                   "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 py-3"},
                   "content": [
                       {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2"}, "content": chips},
                       {"component": "div", "props": {"style": "flex: 1 1 auto;"}},
                       self._caption(f"最近 {latest} · 最多保留 {self._HISTORY_MAX} 条 · 试运行不写入记录", cls="mr-2"),
                       self._danger_btn(view, "clear_history", "dh_clear", "清空记录", "mdi-notification-clear-all"),
                   ]}
        headers = [{"title": "时间", "key": "time", "width": 170},
                   {"title": "标题", "key": "title"},
                   {"title": "动作", "key": "action", "width": 150}]
        return [toolbar, {"component": "VDivider"},
                self._table(headers, items, "暂无删除记录")]

    # ---------- RSS 记录 ----------

    def _page_tab_rss(self, view: dict, washed: List[dict], seen_count: int) -> List[dict]:
        tiles = [
            self._tile("已处理报文", str(seen_count), f"上限 {self._rss_seen_max} 条"),
            self._tile("已洗版集数", str(len(washed)), f"上限 {self._rss_washed_max} 条"),
            self._tile("执行周期", self._rss_cron or "未设置",
                       f"下次 {self._next_run_text(self._rss_cron)}" if self._rss_on else "未启用"),
            self._tile("下载器", self._rss_dl or "默认",
                       f"规则组 {self._rss_rule_group or '不过滤'}"),
        ]
        url_count = len([u for u in (self._rss_urls or "").split("\n") if u.strip()])
        info_chips = [
            self._chip(f"RSS 链接 {url_count} 个", icon="mdi-link-variant"),
            self._chip(f"保存路径 {self._rss_save_path or '默认'}", icon="mdi-folder-outline"),
            self._chip(f"体积过滤 {self._rss_sz or '不限'} GB", icon="mdi-scale"),
            self._chip(f"洗版阈值 {self._rss_th}%", icon="mdi-percent-outline"),
        ]
        if self._rss_ai_identify:
            info_chips.append(self._chip(f"智能助手单轮上限 {self._rss_ai_max} 次", color="info",
                                         icon="mdi-robot-outline"))
        toolbar = {"component": "div",
                   "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 py-2"},
                   "content": [
                       self._btn("立即刷新RSS", "rss_run_once", color="primary", icon="mdi-rss",
                                 disabled=not (self._rss_on and bool(self._rss_urls))),
                       self._danger_btn(view, "clear_seen", "rss_ca", "清除已处理报文", "mdi-broom"),
                       self._danger_btn(view, "clear_washed", "rss_wash_clear", "清除洗版记录",
                                        "mdi-playlist-remove"),
                   ]}
        headers = [{"title": "媒体", "key": "title"},
                   {"title": "季集", "key": "se", "width": 110},
                   {"title": "TMDB ID", "key": "tmdb", "width": 120}]
        return [
            {"component": "VRow", "props": {"dense": True, "class": "ma-0 pa-2"}, "content": tiles},
            {"component": "div", "props": {"class": "d-flex flex-wrap ga-2 px-3 pb-2"}, "content": info_chips},
            toolbar,
            {"component": "VDivider"},
            {"component": "div", "props": {"class": "px-3 pt-2"},
             "content": [self._caption("洗版记录：已按「一集一个槽位」下载过的剧集，清除后同一集会被重新洗版下载")]},
            self._table(headers, washed, "暂无洗版记录"),
        ]

    # ---------- 识别缓存 ----------

    def _page_tab_cache(self, view: dict, success_cache: List[dict], negative_cache: List[dict]) -> List[dict]:
        success_items = []
        for item in reversed(success_cache or []):
            success_items.append({"key": str(item.get("key") or ""),
                                  "title": str(item.get("title") or ""),
                                  "year": str(item.get("year") or "") or "-",
                                  "media_type": str(item.get("media_type") or "") or "-",
                                  "tmdb_id": str(item.get("tmdb_id") or "")})
        negative_items = []
        for item in reversed(negative_cache or []):
            negative_items.append({"key": str(item.get("key") or ""),
                                   "name": str(item.get("name") or "")})
        blocks = [
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 pt-3 pb-2"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-2 font-weight-bold"}, "text": "识别成功缓存"},
                self._chip(f"{len(success_items)} / {self._api_recognize_success_cache_max}", color="success"),
                {"component": "div", "props": {"style": "flex: 1 1 auto;"}},
                self._btn("清空", "cache_clear", color="error", icon="mdi-delete-outline", kind="success"),
            ]},
            self._table([{"title": "解析名称", "key": "key"},
                         {"title": "标题", "key": "title"},
                         {"title": "年份", "key": "year", "width": 90},
                         {"title": "类型", "key": "media_type", "width": 90},
                         {"title": "TMDB ID", "key": "tmdb_id", "width": 110}],
                        success_items, "暂无识别成功缓存"),
            {"component": "VDivider", "props": {"class": "my-2"}},
            {"component": "div", "props": {"class": "d-flex flex-wrap align-center ga-2 px-3 pt-2 pb-2"}, "content": [
                {"component": "div", "props": {"class": "text-subtitle-2 font-weight-bold"}, "text": "识别失败缓存"},
                self._chip(f"{len(negative_items)} / {self._api_recognize_cache_max}", color="warning"),
                {"component": "div", "props": {"style": "flex: 1 1 auto;"}},
                self._btn("清空", "cache_clear", color="error", icon="mdi-delete-outline", kind="negative"),
            ]},
            self._table([{"title": "解析名称", "key": "key"},
                         {"title": "原始名称", "key": "name"}],
                        negative_items, "暂无识别失败缓存"),
            {"component": "div", "props": {"class": "pa-3"}, "content": [
                {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact"},
                 "content": [self._caption("识别顺序：MoviePilot 本地缓存 → 识别成功缓存 → 识别失败缓存（命中则跳过）→ "
                                           "TMDB API → 种子文件名兜底 → 智能助手。新增自定义识别词后建议清空识别失败缓存，"
                                           "让对应报文重新识别。")]},
            ]},
        ]
        return blocks

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

    def _list_raw_torrents(self) -> Optional[List[RawTorrent]]:
        """直接从下载器原生接口读取种子列表，避开统一接口的逐种子名称识别。

        任一选中下载器取种失败或类型不支持时返回 None，由调用方整体回退到
        统一接口：只拿到部分下载器的数据会让辅种漏检、并误报「不在扫描范围」。
        """
        try:
            from app.helper.downloader import DownloaderHelper
            services = DownloaderHelper().get_services(name_filters=self._clean_downloader or None)
        except Exception as e:
            logger.debug(f"SC 获取下载器实例失败，回退统一接口: {str(e)}")
            return None
        if not services:
            return None
        result: List[RawTorrent] = []
        for name, service in services.items():
            instance = getattr(service, "instance", None)
            if not instance:
                return None
            try:
                if instance.is_inactive():
                    logger.warning(f"SC 下载器 {name} 未连接，跳过扫描")
                    continue
            except Exception:
                pass
            service_type = (getattr(service, "type", "") or "").lower()
            if service_type not in ("qbittorrent", "transmission"):
                logger.debug(f"SC 下载器 {name} 类型 {service_type} 不支持原生取种，回退统一接口")
                return None
            try:
                torrents, err = instance.get_torrents()
            except Exception as e:
                logger.warning(f"SC 下载器 {name} 原生取种失败，回退统一接口: {str(e)}")
                return None
            if err:
                logger.warning(f"SC 下载器 {name} 原生取种返回异常，回退统一接口")
                return None
            for t in torrents or []:
                if service_type == "qbittorrent":
                    torrent_hash = t.get("hash")
                    if not torrent_hash:
                        continue
                    result.append(RawTorrent(
                        torrent_hash=torrent_hash,
                        name=(t.get("name") or "").strip(),
                        size=int(t.get("total_size") or t.get("size") or 0),
                        save_path=str(t.get("save_path") or ""),
                        content_path=str(t.get("content_path") or ""),
                        downloader=name,
                    ))
                else:
                    torrent_hash = getattr(t, "hashString", None)
                    if not torrent_hash:
                        continue
                    download_dir = str(getattr(t, "download_dir", "") or "")
                    torrent_name = (getattr(t, "name", "") or "").strip()
                    result.append(RawTorrent(
                        torrent_hash=torrent_hash,
                        name=torrent_name,
                        size=int(getattr(t, "total_size", 0) or 0),
                        save_path=download_dir,
                        content_path=str(Path(download_dir) / torrent_name) if download_dir and torrent_name else "",
                        downloader=name,
                    ))
        return result

    def _get_cached_torrents(self, chain: ChainBase) -> List[Any]:
        now = time.time()
        if self._all_torrents_cache is not None and now - self._all_torrents_cache_time < self._torrents_cache_ttl:
            return self._all_torrents_cache
        start = time.time()
        all_t: Optional[List[Any]] = self._list_raw_torrents()
        source = "下载器原生接口"
        if all_t is None:
            # 回退路径：统一接口会对每个种子做一次 MetaInfo 名称识别，CPU 开销明显
            source = "MoviePilot 统一接口"
            all_t = []
            for dl in (self._clean_downloader or [None]):
                all_t.extend(chain.list_torrents(downloader=dl or None, include_all_tags=True) or [])
        logger.info(f"SC 已获取下载器种子 {len(all_t)} 个（{source}，耗时 {time.time() - start:.1f}s）")
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
        event = threading.Event()
        self._scheduler_event = event
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, args=(event,), daemon=True,
                                                 name="SC-Scheduler")
        self._scheduler_thread.start()

    def _scheduler_loop(self, event: threading.Event) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = BackgroundScheduler(timezone=settings.TZ)
        cron = (self._check_cron or "").strip() or "0 */6 * * *"
        try:
            try:
                trigger = CronTrigger.from_crontab(cron)
            except Exception as exc:
                logger.error(f"SC 执行周期表达式无效（{cron}）: {exc}，回退为 0 */6 * * *")
                cron = "0 */6 * * *"
                trigger = CronTrigger.from_crontab(cron)
            scheduler.add_job(self._check_and_clean, trigger)
            scheduler.start()
            # 用本地 event 等待，避免 stop_service 把 _scheduler_event 置 None 后空指针
            event.wait()
        except Exception as e:
            logger.error(f"SC 定时任务启动失败: {str(e)}")
        finally:
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                pass

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
        now = time.time()
        if self._pb_cache is not None and now - self._pb_cache_time < self._pb_cache_ttl:
            return self._pb_cache
        with self._pb_lock:
            pb_copy = list(self._pb)
        self._pb_cache = pb_copy
        self._pb_cache_time = now
        return pb_copy

    def _configured_dirs(self) -> List[Path]:
        """读取系统「目录配置」中的下载目录与媒体库目录。

        MoviePilot V2 只有 SystemConfigKey.Directories 一个键，每项内含
        download_path / library_path；早期实现读的 DownloadDirectories /
        LibraryDirectories 并不存在，会让所有目录保护逻辑静默失效。
        """
        dirs: List[Path] = []
        for item in (self.systemconfig.get("Directories") or []):
            if not isinstance(item, dict):
                continue
            for key in ("download_path", "library_path"):
                path = str(item.get(key) or "").strip()
                if path:
                    p = Path(path)
                    if p not in dirs:
                        dirs.append(p)
        return dirs

    def _get_space_info(self) -> Optional[Dict[str, float]]:
        try:
            all_dirs = self._configured_dirs() or [Path(settings.CONFIG_PATH)]
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
        # 定时任务与「立即运行」可能并发触发，用锁保证「检查+置位」原子，避免双跑
        with self._run_lock:
            if self._running:
                logger.info("SC 上一轮清理仍在进行，跳过本次触发")
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
        # 试运行不会真正释放空间，若按目标百分比删除会遍历全部单元，因此仍按单次数量限制
        md = self._delete_count if (not self._delete_by_target or self._dry_run) else 0
        fr = ""
        # 删除单元的收集与排序逻辑与详情页「试运行」预览共用，保证预览顺序与真实清理一致
        delete_units = self._collect_delete_units()
        if not delete_units:
            return

        # 一轮清理只拉取一次下载器种子列表并建索引：种子名解析（MetaInfo + 自定义识别词）
        # 是 CPU 密集操作，每个删除单元重复全量拉取会造成明显的 CPU 峰值。
        # 删种后由 _delete_downloader_torrents 从索引与缓存中剔除已删 hash，保持数据新鲜。
        torrent_index = self._build_torrent_index(self._get_cached_torrents(chain))
        for unit in delete_units:
            if md and dc >= md:
                fr = "limit"
                break
            # 完全删除上一个资源后强制刷新空间信息，再检查是否达到目标，避免缓存旧值导致多删
            cs = self._get_cached_space_info()
            # 试运行不会真正释放空间，跳过空间达标判断，否则首轮就会误判为已达标
            if cs and not self._dry_run:
                if self._delete_by_target and cs["free_percent"] >= self._target_free_percent:
                    logger.info(f"SC 空间已达到目标阈值 {self._target_free_percent}% (当前 {cs['free_percent']:.1f}%)，停止清理")
                    fr = "space_ok"
                    break
                if not self._delete_by_target and cs["free_percent"] >= self._min_free_percent:
                    logger.info(f"SC 空间已恢复至触发阈值 {self._min_free_percent}% (当前 {cs['free_percent']:.1f}%)，停止清理")
                    fr = "space_ok"
                    break
            # 完整删除一个资源（种子+文件+记录），删除完成后立即使空间缓存失效，
            # 下一次循环重新查询真实剩余空间，达标即停，否则继续删除下一个
            self._delete_unit(unit, chain, cs or space_info, torrent_index)
            dc += 1
            self._cached_space_info = None
            # 单元之间稍作停顿，把删种/删文件/清目录带来的瞬时占用摊平
            if not self._dry_run and self._unit_delete_interval > 0:
                time.sleep(self._unit_delete_interval)
        reason = {"limit": "达到单次删除数量上限", "space_ok": "空间已达标"}.get(fr, "已处理全部候选资源")
        logger.info(f"SC 清理完成，删除 {dc} 个资源（{reason}）")

    def _dry_run_preview(self, limit: int = 10) -> dict:
        """试运行预览：按真实清理顺序列出将被删除的资源，全程只读、不删除任何东西。

        与定时清理共用 _collect_delete_units，因此预览结果就是真实清理会处理的资源；
        与空间阈值无关，空间充足时同样给出候选列表，方便随时确认删除顺序是否符合预期。
        """
        space = self._get_space_info() or {}
        try:
            free_pct = float(space.get("free_percent"))
        except (TypeError, ValueError):
            free_pct = None
        units = self._collect_delete_units(log_skipped=False)
        items: List[dict] = []
        if units:
            chain = self._get_chain()
            torrent_index = self._build_torrent_index(self._get_cached_torrents(chain))
            for unit in units[:max(1, limit)]:
                records = unit.get("records") or []
                others = self._find_other_version_records(records) if self._delete_other_versions else []
                counted = set()
                for dh in self._collect_record_torrent_hashes(records + others, torrent_index):
                    for torrent_hash, _name, _cross, _dl in self._collect_torrents_to_delete(dh, torrent_index):
                        counted.add(torrent_hash)
                items.append({
                    "title": unit.get("display") or "未知",
                    "records": len(records),
                    "others": len(others),
                    "torrents": len(counted),
                    "prio": bool(unit.get("prio")),
                    "time": str((unit.get("lib_time") if self._delete_by_record else unit.get("sort_time")) or ""),
                })
        return {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "free_percent": free_pct,
            "free_gb": space.get("free_gb"),
            "threshold": self._min_free_percent,
            "target": self._target_free_percent,
            "delete_by_target": self._delete_by_target,
            "delete_count": self._delete_count,
            "sort_by": "整理记录最早入库时间" if self._delete_by_record else "播放缓存最早标记时间",
            "needs_cleanup": bool(free_pct is not None and free_pct < self._min_free_percent),
            "space_unknown": free_pct is None,
            "total": len(units),
            "items": items,
        }

    def _collect_delete_units(self, log_skipped: bool = True) -> List[dict]:
        """收集本轮满足删除条件的删除单元，按「优先标记 + 时间」排序后返回。

        只读操作：不删除任何种子、文件或记录。真实清理与详情页试运行预览共用，
        保证预览列出的资源与顺序就是真实清理会处理的资源与顺序。
        """
        from app.db import ScopedSession
        from sqlalchemy import asc
        sess = ScopedSession()
        try:
            pb = self._get_playback_pb()
            # 播放缓存一次遍历建好索引，避免后续每个删除单元都重新线性扫描整份缓存：
            #   pb_tmdbids:       有播放缓存的 tmdbid（没有缓存的整理记录不可能满足"已看完"）
            #   pb_by_key:        缓存键 -> 缓存条目，用于按 {tmdbid}:M / {tmdbid}:SxxExx 直接取
            #   pb_season_times:  (tmdbid, 季) -> 该季各集标记时间，取最小值即最早标记时间
            #   prio_tmdbids:     被标记优先删除的 tmdbid（字符串形式）
            pb_tmdbids = set()
            pb_by_key: Dict[str, dict] = {}
            pb_season_times: Dict[tuple, List[str]] = {}
            prio_tmdbids = set()
            for p in pb:
                key = p.get("k", "") or ""
                km = re.match(r'^(\d+):', key)
                if not km:
                    continue
                tid_str = km.group(1)
                pb_tmdbids.add(int(tid_str))
                pb_by_key.setdefault(key, p)
                if p.get("prio"):
                    prio_tmdbids.add(tid_str)
                sm = re.match(r'^(\d+):S(\d+)E\d+$', key)
                if sm:
                    pb_season_times.setdefault((int(sm.group(1)), int(sm.group(2))), []).append(
                        p.get("t", "9999"))

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

            def _snap(r):
                return {"id": r.id, "title": r.title or "未知", "type": r.type or "",
                        "seasons": r.seasons or "", "episodes": r.episodes or "",
                        "src": r.src or "", "dest": r.dest or "", "tmdbid": r.tmdbid,
                        "download_hash": r.download_hash or "",
                        "src_fileitem": r.src_fileitem or {},
                        "dest_fileitem": r.dest_fileitem or {}}

            def _ep_max(rec):
                mm = re.findall(r'\d+', rec.episodes or "0")
                return max(int(e) for e in mm) if mm else 0

            def _movie_watched_time(rec):
                """电影：pb 中 {tmdbid}:M 已看完才返回其缓存时间，否则 None。"""
                p = pb_by_key.get(f"{rec.tmdbid}:M")
                if not p:
                    return None
                if (p.get("p", 0) or 0) >= self._watched_threshold:
                    return p.get("t", "9999")
                return None

            def _unit_earliest_mark_time(tmdbid, season=None):
                """取该单元在播放缓存中最早的标记时间（排序用）。

                电影取 {tmdbid}:M 的缓存时间；电视剧取该季各集缓存时间的最小值。
                以"最早标记"作为删除/跳过顺序依据：越早标记的越先处理。
                无匹配缓存时返回 "9999"（排到最后）。
                """
                if season is None:
                    p = pb_by_key.get(f"{tmdbid}:M")
                    return p.get("t", "9999") if p else "9999"
                times = pb_season_times.get((tmdbid, season)) or []
                return min(times) if times else "9999"

            def _unit_earliest_lib_time(recs):
                """取该单元在媒体整理记录中最早的入库时间（排序用）。

                以整理记录 date 字段为准（date 为入库时间字符串，字典序即时间序），
                date 缺失的记录视为 "9999"（排到最后）。
                """
                times = [str(r.date or "").strip() for r in recs if str(r.date or "").strip()]
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
                p = pb_by_key.get(k)
                if p:
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
                    "lib_time": _unit_earliest_lib_time(recs),
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
                    "lib_time": _unit_earliest_lib_time(recs),
                    "prio": str(tmdbid) in prio_tmdbids,
                })

            # 汇总所有记录（有/无 download_hash）后重新分组
            all_records: List[TransferHistory] = []
            for recs in hash_groups.values():
                all_records.extend(recs)
            all_records.extend(no_hash_records)

            # 电视剧：按 tmdbid+season 归并（跨种子/跨版本），整季作为一个删除单元
            tv_season_groups: Dict[str, List[TransferHistory]] = {}
            # 电影：按 tmdbid 归并（跨种子/跨版本），同一部电影的所有版本只生成一个删除单元
            movie_tmdb_groups: Dict[int, List[TransferHistory]] = {}
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
                    movie_tmdb_groups.setdefault(r.tmdbid, []).append(r)

            # 电视剧：同一 TMDB 存在多个季度时，升级为整剧删除单元。
            # 规则：必须等待整理记录中最后一季的最后一集播放完成后，才删除所有季度；
            # 若最后一季最后一集未看完，则跳过该剧所有季度，让其他已看资源优先删除。
            tv_show_groups: Dict[int, List[TransferHistory]] = {}
            for recs in tv_season_groups.values():
                if recs and recs[0].tmdbid:
                    tv_show_groups.setdefault(recs[0].tmdbid, []).extend(recs)

            def _add_tv_show_unit(recs):
                seasons = sorted({self._norm_season(r.seasons or "") for r in recs if self._norm_season(r.seasons or "") is not None})
                if len(seasons) <= 1:
                    _add_tv_season_unit(recs)
                    return

                # 多季度电视剧：只检查最后一季最后一集
                last_season = seasons[-1]
                last_season_records = [r for r in recs if self._norm_season(r.seasons or "") == last_season]
                t, reason = _season_last_watched_time(last_season_records)
                mark_time = _unit_earliest_mark_time(recs[0].tmdbid, None)
                if t is None:
                    title = recs[0].title or "未知"
                    skip_logs.append((mark_time, f"{title}: 多季度整剧最后一季 S{last_season:02d} 最后一集未看完，跳过全部季度"))
                    return

                rep = max(recs, key=_ep_max)
                dh = next((r.download_hash for r in recs if r.download_hash), "")
                ep_count = len({e for r in recs for e in self._episode_set(r.episodes or "")})
                delete_units.append({
                    "records": [_snap(r) for r in recs],
                    "hash": dh,
                    "tmdbid": rep.tmdbid,
                    "season": None,
                    "is_tv": True,
                    "display": f"{rep.title or '未知'}（整剧 {len(seasons)} 季 {ep_count} 集）",
                    "sort_time": mark_time,
                    "lib_time": _unit_earliest_lib_time(recs),
                    "prio": str(rep.tmdbid) in prio_tmdbids,
                })

            for recs in tv_show_groups.values():
                _add_tv_show_unit(recs)
            for recs in movie_tmdb_groups.values():
                _add_movie_unit(recs)

            # 输出因未看完/无播放记录而跳过的电视剧季，按最早标记时间排序，每行 5 个
            if skip_logs and log_skipped:
                skip_logs.sort(key=lambda x: x[0])
                reasons = [s for _, s in skip_logs]
                logger.info(f"SC 以下 {len(reasons)} 个电视剧季不满足删除条件，已跳过（按最早标记时间排序）：")
                for i in range(0, len(reasons), 5):
                    logger.info("SC   - " + " ｜ ".join(reasons[i:i + 5]))

            if not delete_units:
                logger.info("SC 未发现满足删除条件的资源（已看完且在转移历史中）")
                return []

            # 排序：优先删除被标记的资源；按媒体整理记录删除开启时按整理记录最早入库时间（升序），否则按播放缓存最早标记时间（升序）
            sort_key = "lib_time" if self._delete_by_record else "sort_time"
            sort_label = "整理记录最早入库时间" if self._delete_by_record else "最早标记时间"
            logger.info(f"SC 共 {len(delete_units)} 个删除单元满足条件，按优先标记与{sort_label}排序")
            delete_units.sort(key=lambda u: (not u["prio"], u.get(sort_key) or "9999"))
        finally:
            sess.close()
        return delete_units

    def _delete_unit(self, unit, chain, space_info, torrent_index=None):
        """删除一个删除单元（合集的所有集一起删除）。

        使用预先快照的记录字典，避免跨 Session 访问 ORM 懒加载属性。
        torrent_index 为调用方预建的种子索引（见 _build_torrent_index），一轮清理复用同一份。
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
        if torrent_index is None:
            torrent_index = self._build_torrent_index(self._get_cached_torrents(chain))
        if self._dry_run:
            # 统计将删除的种子（主种子 + 辅种）。部分不同版本整理记录没有
            # download_hash，需要通过源文件路径反查下载器任务及其辅种。
            main_hashes = self._collect_record_torrent_hashes(all_recs, torrent_index)
            counted_hashes = set()
            main_cnt = cross_cnt = 0
            for dh in main_hashes:
                tl = self._collect_torrents_to_delete(dh, torrent_index)
                for torrent_hash, _, is_cross, _dl in tl:
                    if torrent_hash in counted_hashes:
                        continue
                    counted_hashes.add(torrent_hash)
                    if is_cross:
                        cross_cnt += 1
                    else:
                        main_cnt += 1
            unit_type = "电视剧整季" if is_tv else "电影"
            logger.info(
                f"【试运行】将删除 [{unit_type}] {display_name}："
                f"整理记录 {len(records)} 条"
                + (f"，不同版本 {len(other_versions)} 条" if other_versions else "")
                + f"，种子 {main_cnt} 个"
                + (f"（含辅种 {cross_cnt} 个）" if cross_cnt else "")
            )
            # 试运行只是预演，不写入删除记录（删除记录只保留真实删除与失败结果）
            return
        try:
            unit_type = "电视剧整季" if is_tv else "电影"
            logger.info(f"SC 开始删除 [{unit_type}] {display_name}："
                        f"整理记录 {len(records)} 条"
                        + (f"，不同版本 {len(other_versions)} 条" if other_versions else ""))
            # 1) 从下载器删除该单元涉及的全部种子及其辅种（整季可能跨多个种子）
            #    删种时 delete_file=True 会一并删除下载目录中的源文件
            torrents_deleted = 0
            main_hashes = self._collect_record_torrent_hashes(all_recs, torrent_index)
            if not main_hashes and download_hash:
                main_hashes = [download_hash]
            for dh in main_hashes:
                torrents_deleted += self._delete_downloader_torrents(
                    chain, dh, display_name, torrent_index
                )
            if not main_hashes:
                logger.info(f"SC [{display_name}] 无关联种子（可能为无 hash 记录），跳过删种")
            # 2) 删种后删除源文件、媒体库文件及残留空目录
            #    复用 StorageChain.delete_media_file（含配置目录保护，不会误删下载/媒体库根目录）
            storage_chain = StorageChain()
            cleanup_dirs: set = set()  # 收集所有被处理文件所在的父目录路径
            for rec in all_recs:
                # 删除下载源文件（有种子的已随删种删除，此处兜底处理无 hash 记录）
                src_fileitem = rec.get("src_fileitem", {})
                if src_fileitem:
                    fi = schemas.FileItem(**src_fileitem)
                    storage_chain.delete_media_file(fi)
                else:
                    # 无 fileitem 时兜底删除 src 路径
                    src = rec.get("src", "")
                    if src:
                        sp = Path(src)
                        if sp.exists():
                            self._safe_delete_path(sp)
                            cleanup_dirs.add(sp.parent)
                # 删除媒体库文件（链接+重命名后的成品）
                dest_fileitem = rec.get("dest_fileitem", {})
                if dest_fileitem:
                    fi = schemas.FileItem(**dest_fileitem)
                    storage_chain.delete_media_file(fi)
                else:
                    dest = rec.get("dest", "")
                    if dest:
                        dp = Path(dest)
                        if dp.exists():
                            self._safe_delete_path(dp)
                            cleanup_dirs.add(dp.parent)
            # 收集 src/dest 路径的父目录（含 fileitem 场景），同时记录本单元自己的
            # 文件路径：下载器 delete_file 为异步操作，这些文件可能仍短暂存在，
            # 清理目录时应视为已删除，避免单文件种子的专属目录被误判为「仍有视频文件」
            unit_paths: set = set()
            for rec in all_recs:
                src = rec.get("src", "")
                if src:
                    cleanup_dirs.add(Path(src).parent)
                    unit_paths.add(str(Path(src)))
                dest = rec.get("dest", "")
                if dest:
                    cleanup_dirs.add(Path(dest).parent)
                    unit_paths.add(str(Path(dest)))
            # 清理下载残留目录和媒体库空目录，并记录仍未清理的目录。
            for d in cleanup_dirs:
                self._cleanup_download_dir(d, unit_paths)
                self._delete_media_dir(d, max_levels=3, ignore_paths=unit_paths)
            leftover_dirs = self._find_residual_dirs(cleanup_dirs, unit_paths)
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
                leftover_line = ""
                if leftover_dirs:
                    leftover_line = "\n残留目录:\n" + "\n".join(str(path) for path in leftover_dirs)
                self.post_message(title="空间清理器 - 资源已删除",
                                  text=f"资源: {display_name}{ver_line}\n删除种子: {torrents_deleted} 个\n当前剩余空间: {space_info['free_gb']:.2f} GB ({space_info['free_percent']:.1f}%){leftover_line}")
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
                        "src_fileitem": r.src_fileitem or {},
                        "dest_fileitem": r.dest_fileitem or {},
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
            self._pb_cache = None
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

        # 存在可解析季号记录的剧集集合，避免在 _is_orphan 中对每个缓存键线性扫描季集合
        covered_tv_season_tmdbids = {t for (t, _s) in covered_tv_seasons}

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
                if tid not in covered_tv_season_tmdbids:
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

    # ==================== 种子索引 ====================

    def _torrent_paths(self, torrent: Any) -> List[str]:
        """收集一个下载器任务对应的磁盘路径（内容路径、任务路径、保存目录+名称）。"""
        paths = []
        for attr in ("content_path", "path"):
            value = getattr(torrent, attr, None)
            if value:
                paths.append(str(value))
        save_path = getattr(torrent, "save_path", None)
        name = self._torrent_name(torrent)
        if save_path and name:
            paths.append(str(Path(str(save_path)) / name))
        return paths

    def _build_torrent_index(self, torrents: List[Any]) -> Dict[str, dict]:
        """把种子列表预处理为索引，避免删除阶段反复做 O(整理记录 × 种子) 线性扫描。

        - by_hash:    hash -> 种子对象
        - by_content: (体积, 名称) -> [hash, ...]，用于常数级查找辅种
        - by_path:    磁盘路径 -> hash，用于无 download_hash 的整理记录按源文件反查任务
        """
        index: Dict[str, dict] = {"by_hash": {}, "by_content": {}, "by_path": {}}
        for torrent in torrents or []:
            torrent_hash = getattr(torrent, "hash", None)
            if not torrent_hash:
                continue
            index["by_hash"][torrent_hash] = torrent
            size = self._torrent_size(torrent)
            name = self._torrent_name(torrent)
            if size and name:
                index["by_content"].setdefault((size, name), []).append(torrent_hash)
            for path in self._torrent_paths(torrent):
                # 同一路径可能对应多个辅种，保留首个即可：辅种会在 by_content 中一并取出
                index["by_path"].setdefault(path, torrent_hash)
        return index

    def _drop_torrents_from_index(self, index: Optional[dict], hashes: set) -> None:
        """种子删除后同步剔除索引与列表缓存，无需清空缓存重新全量拉取。"""
        if not hashes:
            return
        if index:
            for h in hashes:
                index["by_hash"].pop(h, None)
            for key, hs in list(index["by_content"].items()):
                kept = [h for h in hs if h not in hashes]
                if kept:
                    index["by_content"][key] = kept
                else:
                    index["by_content"].pop(key, None)
            for key, h in list(index["by_path"].items()):
                if h in hashes:
                    index["by_path"].pop(key, None)
        if self._all_torrents_cache:
            self._all_torrents_cache = [t for t in self._all_torrents_cache
                                        if getattr(t, "hash", None) not in hashes]

    def _collect_record_torrent_hashes(self, records: List[dict], index: dict) -> List[str]:
        """收集整理记录关联的主种子哈希，缺少哈希时按源路径反查下载器任务。

        反查用路径索引从源文件自身逐级向上匹配父目录，命中最深的任务路径即停：
        既避免遍历全部种子，也不再依赖 Path.relative_to 抛异常来判断包含关系。
        """
        hashes = []
        seen = set()
        by_path = (index or {}).get("by_path") or {}
        for record in records:
            download_hash = record.get("download_hash", "")
            if download_hash:
                if download_hash not in seen:
                    seen.add(download_hash)
                    hashes.append(download_hash)
                continue
            src = record.get("src", "")
            if not src or not by_path:
                continue
            src_path = Path(src)
            for candidate in (src_path, *src_path.parents):
                torrent_hash = by_path.get(str(candidate))
                if not torrent_hash:
                    continue
                if torrent_hash not in seen:
                    seen.add(torrent_hash)
                    hashes.append(torrent_hash)
                break
        return hashes

    def _collect_torrents_to_delete(self, download_hash: str, index: dict) -> List[tuple]:
        """收集该主种子及其辅种，返回 [(hash, name, is_cross, downloader), ...]（含主种子本身）。

        辅种定义：与主种子内容相同（体积一致且名称一致）但 tracker 不同的私有种子。
        辅种共享同一份磁盘文件，通常由不同站点重复做种；删除时必须一并处理，
        否则残留的辅种会重新占用/锁定文件。用于删种与试运行统计（不执行删除）。
        """
        result = []
        if not download_hash:
            return result
        by_hash = (index or {}).get("by_hash") or {}
        main_t = by_hash.get(download_hash)
        main_name = self._torrent_name(main_t)
        main_size = self._torrent_size(main_t)
        result.append((download_hash, main_name or download_hash, False, self._torrent_downloader(main_t)))
        if not main_size or not main_name:
            return result
        for h in ((index or {}).get("by_content") or {}).get((main_size, main_name), []):
            if h == download_hash:
                continue
            t = by_hash.get(h)
            result.append((h, self._torrent_name(t) or h, True, self._torrent_downloader(t)))
        return result

    def _delete_downloader_torrents(self, chain, download_hash, display_name, index) -> int:
        """删除主种子及其辅种（cross-seed），返回实际删除的种子数量。

        删除请求按种子实际所属下载器下发：MoviePilot 的 remove_torrents 不带下载器名时
        只作用于默认下载器，其他下载器中的种子既删不掉、qb 又会返回成功，导致漏删被静默。
        扫描范围为「扫描下载器」选中的下载器，其中包含非 MP 管理的种子。
        """
        if not download_hash:
            logger.warning(f"SC 无 download_hash，跳过删种: {display_name}")
            return 0
        to_delete = self._collect_torrents_to_delete(download_hash, index)
        if not ((index or {}).get("by_hash") or {}).get(download_hash):
            # 种子不在扫描范围内（未选中的下载器 / 已被移除），此时拿不到所属下载器，
            # 删除请求只能下发到默认下载器，且无法发现它的辅种
            logger.warning(f"SC 种子 {download_hash} 不在扫描下载器范围内，"
                           f"将按默认下载器尝试删除且不检索辅种: {display_name}")
        cross_cnt = sum(1 for item in to_delete if item[2])
        main_cnt = len(to_delete) - cross_cnt
        logger.info(f"SC 准备删种 [{display_name}]: 主种子 {main_cnt} 个" +
                    (f"，辅种 {cross_cnt} 个" if cross_cnt else "，无辅种"))
        deleted = 0
        deleted_hashes = set()
        for h, name, is_cross, downloader in to_delete:
            role = "辅种" if is_cross else "主种子"
            dl_label = f" @{downloader}" if downloader else ""
            try:
                # remove_torrents 返回布尔，失败时不能计入删除数，否则统计虚高且失败被静默
                if chain.remove_torrents(hashs=h, delete_file=True, downloader=downloader or None):
                    logger.info(f"SC   已删除{role}: {name} ({h}){dl_label}")
                    deleted += 1
                    deleted_hashes.add(h)
                else:
                    logger.warning(f"SC   删除{role}未成功: {name} ({h}){dl_label}")
            except Exception as e:
                logger.error(f"SC   删除{role}失败 {name} ({h}){dl_label}: {str(e)}")
        # 只从索引与列表缓存中剔除已删种子，避免后续删除单元重新全量拉取种子列表
        self._drop_torrents_from_index(index, deleted_hashes)
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

    @staticmethod
    def _torrent_downloader(t) -> str:
        """取种子所属下载器名称，删种时据此定向下发请求；无法获取时返回空串。"""
        if t is None:
            return ""
        return str(getattr(t, "downloader", None) or "").strip()

    def _safe_delete_path(self, path: Path) -> bool:
        """删除文件或目录，返回是否确实删除成功。"""
        try:
            if path.is_file() or path.is_symlink():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            return not path.exists()
        except Exception as e:
            logger.warning(f"SC 删除路径失败 {path}: {e}")
            return False

    # 判定「残留元数据目录」时使用的视频扩展名（目录内含这些文件则不删除）
    _LEFTOVER_VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".mov", ".wmv", ".flv",
                            ".iso", ".rmvb", ".rm", ".mpg", ".mpeg", ".m4v", ".webm", ".vob", ".strm"}

    def _dir_is_leftover_metadata(self, path: Path, ignore_paths: Optional[set] = None) -> bool:
        """目录是否为「资源删除后残留的元数据目录」——可安全删除。

        判定为 True 的条件：目录内不含任何子目录，且不含任何视频文件；
        即只剩 nfo、海报/背景图、字幕、系统垃圾等刮削/元数据文件。
        这类目录（如某剧删除全部季后仅剩 tvshow.nfo、poster.jpg 的剧名根目录）
        应连同删除。若目录内仍有子目录（如同类目录下的其他剧集、其他季）
        或仍存在视频文件，则返回 False 以停止上溯，避免误删。

        ignore_paths 为「本次删除单元自己的文件路径」集合：下载器 delete_file
        为异步操作，删种后源文件可能仍短暂存在，这类文件本就属于本次删除范围，
        判定时应视为已删除，否则单文件种子的专属目录会被误判为「仍有视频文件」
        而永久残留。
        """
        try:
            ignore = ignore_paths or set()
            for e in path.iterdir():
                if e.is_dir():
                    return False  # 存在子目录（其他剧集/其他季），保留
                if e.is_file() and e.suffix.lower() in self._LEFTOVER_VIDEO_EXTS:
                    if str(e) in ignore:
                        continue  # 本单元自己的文件（下载器异步删除中），视为已删
                    return False  # 仍有其他视频文件，保留
            return True
        except Exception:
            return False

    def _dir_is_protected(self, path: Path, configured_dirs: List[Path]) -> bool:
        """目录是否受保护（不允许删除）。

        受保护的情况：根目录、挂载点、系统配置中的下载/媒体库目录本身，
        以及这些配置目录的任意上级目录。
        """
        try:
            if not path or path.parent == path:
                return True
            if os.path.ismount(str(path)):
                return True
            for cfg in configured_dirs:
                if path == cfg or cfg.is_relative_to(path):
                    return True
        except Exception:
            return True
        return False

    def _delete_media_dir(self, media_dir: Path, max_levels: int = 3, ignore_paths: Optional[set] = None):
        """删除媒体库中该资源所在目录（MP 软链接/硬链接、重命名、刮削生成的成品目录）。

        MP 通常为每部电影/每季电视剧建立独立目录，目录内除媒体文件外还含
        nfo、海报、fanart 等刮削文件；删除资源时应连同整个目录一并删除。
        电视剧的季目录（如 .../剧名 (2026) {tmdbid=x}/Season 1）删除后，
        若上层剧名目录随之变空，也一并向上清理（最多 max_levels 层），
        遇到仍有内容的目录（如同类目录下的其他剧集）、挂载点或根目录即停止。
        目标目录必须只剩元数据文件，且不能是配置的下载/媒体库目录或其上级目录，
        否则只记录日志并跳过，避免整目录误删。
        """
        try:
            if not media_dir or not media_dir.exists() or not media_dir.is_dir():
                return
            configured_dirs = self._configured_dirs()
            if self._dir_is_protected(media_dir, configured_dirs):
                logger.debug(f"SC 跳过删除目录（挂载点/根目录/配置的下载或媒体库目录）: {media_dir}")
                return
            if not self._dir_is_leftover_metadata(media_dir, ignore_paths):
                logger.info(f"SC 目录仍有子目录或视频文件，跳过删除: {media_dir}")
                return
            if self._safe_delete_path(media_dir):
                logger.info(f"SC 已删除媒体库目录: {media_dir}")
            else:
                logger.warning(f"SC 删除媒体库目录未成功: {media_dir}")
                return
            # 向上清理因删除季目录而残留的空目录（如剧名根目录）
            cur = media_dir.parent
            for _ in range(max_levels):
                if not cur or not cur.exists() or not cur.is_dir():
                    break
                if self._dir_is_protected(cur, configured_dirs):
                    break
                if not self._dir_is_leftover_metadata(cur, ignore_paths):
                    break  # 仍有子目录或视频文件（如同目录下别的剧集），停止上溯
                parent = cur.parent
                if self._safe_delete_path(cur):
                    logger.info(f"SC 已删除残留空目录: {cur}")
                cur = parent
        except Exception as e:
            logger.error(f"SC 删除媒体库目录失败 {media_dir}: {e}")

    def _cleanup_download_dir(self, download_dir: Path, ignore_paths: Optional[set] = None):
        """清理下载目录中残留的空目录。

        下载器删种时 delete_file=True 只删除种子对应的文件，不会删除种子所在的
        父目录（如 qBittorrent 为每个种子创建的独立子目录）。此方法仅删除该目录
        本身（若已空），不向上追溯，避免误删下载根目录或媒体库目录。
        ignore_paths 为本次删除单元自己的文件路径，下载器异步删除尚未完成时
        也视为已删除。
        """
        try:
            if not download_dir or not download_dir.exists() or not download_dir.is_dir():
                return
            if self._dir_is_protected(download_dir, self._configured_dirs()):
                return
            # 检查目录是否为空（或仅剩元数据/垃圾文件、本单元待删文件）
            if not self._dir_is_leftover_metadata(download_dir, ignore_paths):
                return  # 目录内仍有子目录或其他视频文件，不清理
            if self._safe_delete_path(download_dir):
                logger.info(f"SC 已清理下载残留目录: {download_dir}")
        except Exception as e:
            logger.error(f"SC 清理下载残留目录失败 {download_dir}: {e}")

    def _find_residual_dirs(self, directories: set, ignore_paths: Optional[set] = None) -> List[Path]:
        """检查资源删除后仍存在内容的目录，返回需要在通知中提示的路径。

        只跳过配置的下载/媒体库目录本身及其上级目录；位于配置目录「之内」的
        资源目录才是需要提示的残留对象（旧实现判断方向相反，结果恒为空）。
        ignore_paths 为本次删除单元自己的文件路径，下载器异步删除可能尚未完成，
        不计入残留。
        """
        configured_dirs = self._configured_dirs()
        ignore = ignore_paths or set()
        residual = []
        seen = set()
        for directory in directories:
            path = Path(directory)
            if str(path) in seen or not path.exists() or not path.is_dir():
                continue
            seen.add(str(path))
            if self._dir_is_protected(path, configured_dirs):
                continue
            try:
                if any(str(e) not in ignore for e in path.iterdir()):
                    residual.append(path)
            except OSError:
                continue
        return sorted(residual, key=str)

    def _add_delete_history(self, title: str, action: str):
        h = self._sanitize_history(self.get_data("delete_history"))
        h.append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "title": title, "action": action})
        if len(h) > self._HISTORY_MAX:
            h = h[-self._HISTORY_MAX:]
        self.save_data("delete_history", h)

    def _sanitize_history(self, raw: Any) -> List[Dict[str, str]]:
        """规范化删除记录：丢弃历史遗留的试运行条目，并裁剪到上限条数。"""
        items = []
        for record in raw or []:
            if not isinstance(record, dict):
                continue
            if "试运行" in str(record.get("action") or ""):
                continue
            items.append(record)
        return items[-self._HISTORY_MAX:]

    def _migrate_delete_history(self) -> None:
        """一次性清理旧数据：删除记录不再缓存试运行，上限收敛到 _HISTORY_MAX 条。"""
        raw = self.get_data("delete_history")
        if not isinstance(raw, list) or not raw:
            return
        items = self._sanitize_history(raw)
        if len(items) != len(raw):
            self.save_data("delete_history", items)
            logger.info(f"SC 删除记录已清理：{len(raw)} 条 -> {len(items)} 条（移除试运行记录并限制为 {self._HISTORY_MAX} 条）")

    def _get_delete_history(self) -> List[Dict[str, str]]:
        return self._sanitize_history(self.get_data("delete_history"))

    # ==================== API ====================

    def api_dry_run(self):
        """对外试运行接口：与详情页「试运行」共用同一套删除单元收集逻辑。"""
        preview = self._dry_run_preview()
        if preview["space_unknown"]:
            return {"success": False, "message": "无法获取磁盘空间"}
        space_info = {"total_gb": None, "free_gb": preview["free_gb"], "used_gb": None,
                      "free_percent": preview["free_percent"],
                      "threshold_percent": preview["threshold"],
                      "needs_cleanup": preview["needs_cleanup"]}
        si = self._get_space_info() or {}
        space_info.update({k: v for k, v in si.items() if k in ("total_gb", "free_gb", "used_gb", "free_percent")})
        return {"success": True, "space_info": space_info, "would_delete": preview["items"],
                "total": preview["total"],
                "message": f"试运行完成，共 {preview['total']} 个候选资源"}

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
        if not self._rss_urls:
            return
        urls = [u.strip() for u in self._rss_urls.split("\n") if u.strip()]
        if not urls:
            return
        with self._run_lock:
            if self._rss_busy:
                logger.info("SC-RSS 上一轮刷新仍在进行，跳过本次触发")
                return
            self._rss_busy = True
        logger.info("SC-RSS 开始运行...")
        # 重置智能助手兜底的单轮预算与失败标题记录
        self._rss_ai_calls = 0
        self._rss_ai_failed = {}
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
            try:
                items = RssHelper().parse(url)
            except Exception as e:
                logger.error(f"SC-RSS 解析 RSS 失败 [{url}]: {e}")
                continue
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
                    self._rss_seen[e] = None
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
                    fb_m, fb_meta, fb_name = self._rss_filename_fallback(item, t, "报文标题识别失败")
                    if fb_m and fb_meta:
                        m, meta, video_name = fb_m, fb_meta, fb_name
                        self._rss_log("文件名回退命中", getattr(m, "title", t), "改用种子文件名识别结果")
                    else:
                        # 报文与文件名都失败：交给智能助手接管识别（并尝试写入自定义识别词）
                        ai_m, ai_meta, ai_name = self._rss_ai_fallback(item, t, "报文与种子文件名均识别失败")
                        if ai_m and ai_meta:
                            m, meta, video_name = ai_m, ai_meta, ai_name
                        else:
                            self._rss_log("识别失败", t)
                            if self._rss_ntf:
                                self.post_message(title="SC-RSS识别失败",
                                                  text=f"资源无法识别: {t}")
                            continue
                # 跳过无 TMDB ID 的识别结果
                if not m.tmdb_id:
                    ai_m, ai_meta, ai_name = self._rss_ai_fallback(item, t, "TMDB API 未识别到媒体（无 TMDB ID）")
                    if ai_m and ai_meta:
                        m, meta, video_name = ai_m, ai_meta, ai_name
                    else:
                        self._rss_log("跳过无TMDB", t, "未识别到 TMDB ID")
                        if self._rss_ntf:
                            self.post_message(title="SC-RSS跳过",
                                              text=f"未识别到 TMDB ID: {t}")
                        continue
                # 判断电视剧 / 电影，电视剧用 MP 剧集解析引擎重新提取季/集
                is_tv = (getattr(m, "type", None) == MediaType.TV) or (m.season is not None) or (meta.begin_episode is not None)
                if is_tv:
                    s_season, s_episode = self._rss_tv_season_episode(m, meta, video_name)
                    if s_episode is None:
                        # 报文未解析出集号时，按配置回退到种子文件名识别。
                        fb_m, fb_meta, fb_name = self._rss_filename_fallback(item, t, "报文未识别到集号")
                        if fb_m and fb_meta:
                            m, meta, video_name = fb_m, fb_meta, fb_name
                            s_season, s_episode = self._rss_tv_season_episode(m, meta, video_name)
                            if s_episode is not None:
                                self._rss_log("文件名回退命中", m.title, "改用种子文件名识别集号")
                    # 电视剧无集号则交给智能助手兜底，仍失败才跳过
                    if s_episode is None:
                        ai_m, ai_meta, ai_name = self._rss_ai_fallback(item, t, "电视剧未识别到集号")
                        if ai_m and ai_meta:
                            m, meta, video_name = ai_m, ai_meta, ai_name
                            s_season, s_episode = self._rss_tv_season_episode(m, meta, video_name)
                    if s_episode is None:
                        self._rss_log("跳过无集号", t, "电视剧未识别到集号")
                        if self._rss_ntf:
                            self.post_message(title="SC-RSS跳过",
                                              text=f"电视剧未识别到集号: {t}")
                        continue
                else:
                    s_season, s_episode = None, None
                if is_tv:
                    se_fmt = f"S{int(s_season):02d}E{int(s_episode):02d}" if s_episode is not None else f"S{int(s_season):02d}"
                    # 同一 TMDB 已有播放缓存但季号不一致时，先尝试种子文件名兜底识别，仍不一致才跳过。
                    cached_seasons = self._rss_cached_seasons(m.tmdb_id)
                    if cached_seasons and int(s_season) not in cached_seasons:
                        cached_text = "、".join(f"S{season:02d}" for season in cached_seasons)
                        fb_m, fb_meta, fb_name = self._rss_filename_fallback(
                            item, t, f"报文季号 {se_fmt} 与播放缓存季（{cached_text}）不一致")
                        fb_ok = False
                        if fb_m and fb_meta:
                            fb_season, fb_episode = self._rss_tv_season_episode(fb_m, fb_meta, fb_name)
                            fb_cached = self._rss_cached_seasons(fb_m.tmdb_id) if fb_m.tmdb_id else []
                            if fb_episode is not None and (not fb_cached or int(fb_season) in fb_cached):
                                m, meta, video_name = fb_m, fb_meta, fb_name
                                s_season, s_episode = fb_season, fb_episode
                                se_fmt = f"S{int(s_season):02d}E{int(s_episode):02d}"
                                self._rss_log("文件名回退命中", m.title, f"改用文件名识别结果 {se_fmt}")
                                fb_ok = True
                        if not fb_ok:
                            # 文件名兜底仍不一致：交给智能助手接管识别（并尝试写入自定义识别词）
                            ai_m, ai_meta, ai_name = self._rss_ai_fallback(
                                item, t, f"报文季号 {se_fmt} 与播放缓存季（{cached_text}）不一致",
                                cached_seasons=cached_seasons)
                            if ai_m and ai_meta:
                                ai_season, ai_episode = self._rss_tv_season_episode(ai_m, ai_meta, ai_name)
                                ai_cached = self._rss_cached_seasons(ai_m.tmdb_id) if ai_m.tmdb_id else []
                                if ai_episode is not None and (not ai_cached or int(ai_season) in ai_cached):
                                    m, meta, video_name = ai_m, ai_meta, ai_name
                                    s_season, s_episode = ai_season, ai_episode
                                    se_fmt = f"S{int(s_season):02d}E{int(s_episode):02d}"
                                    self._rss_log("智能助手兜底命中", m.title, f"改用智能助手识别结果 {se_fmt}")
                                    fb_ok = True
                        if not fb_ok:
                            self._rss_log("跳过季号不一致", m.title,
                                          f"RSS={se_fmt}，播放缓存季={cached_text}，疑似分季策略不同")
                            if self._rss_ntf:
                                self.post_message(
                                    title="SC-RSS季号不一致",
                                    text=f"资源: {m.title}\nRSS识别: {se_fmt}\n播放缓存季: {cached_text}\n"
                                         "疑似分季策略不同，已跳过下载"
                                )
                            continue
                else:
                    se_fmt = "电影"
                cr = self._rss_ck(m, meta, s_season, s_episode)
                # 洗版模式：若该季已有更新集的播放记录，则跳过之前的所有旧集
                # 例：缓存中已有第 10 集记录，则该季 1-9 集不再洗版下载
                if is_tv and s_episode is not None:
                    latest_ep = self._rss_latest_watched_ep(m.tmdb_id, s_season)
                    if latest_ep is not None and int(s_episode) < latest_ep:
                        self._rss_log("跳过旧集", m.title,
                                      f"{se_fmt} 已看到 E{latest_ep:02d}，跳过更早的集")
                        if self._rss_ntf:
                            self.post_message(title="SC-RSS跳过旧集",
                                              text=f"{m.title} {se_fmt} 已看到第{latest_ep}集，跳过旧集")
                        continue
                # 洗版模式：播放进度低于阈值才触发洗版
                if cr["s"]:
                    # 已看完（进度 >= 阈值），跳过
                    self._rss_log("跳过", m.title, cr["r"])
                    if self._rss_ntf:
                        self.post_message(title="SC-RSS跳过",
                                          text=f"{m.title} {se_fmt} {cr['r']}")
                    continue
                # 构造去重 key：电视剧按 tmdb+季+集，电影按 tmdb，缺字段时用 enclosure
                if is_tv and m.tmdb_id and s_episode is not None:
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
                    self._rss_washed[ep_key] = None
                self._rss_log("下载", m.title)
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 已添加下载",
                                      text=self._rss_notify_text(item, meta, m, se_fmt))
            else:
                self._rss_log("下载失败", item.get("title", ""), "推送下载器失败")
                if self._rss_ntf:
                    self.post_message(title="SC-RSS 添加失败",
                                      text=f"名称: {m.title} {se_fmt}")
        self._rss_save_dedup()

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
            if isinstance(v, datetime):
                # RssHelper 解析结果通常已是带时区的 datetime，直接取时间戳
                try:
                    return v.timestamp()
                except (OverflowError, OSError, ValueError):
                    continue
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt).timestamp()
                except (ValueError, TypeError):
                    continue
            # 兜底：ISO 格式（如 2022-10-15 14:02:54+08:00）
            try:
                return datetime.fromisoformat(s).timestamp()
            except (ValueError, TypeError):
                continue
        return float("inf")

    @staticmethod
    def _rss_earlier(ts_a: float, ts_b: float) -> bool:
        """ts_a 是否比 ts_b 更早发布。"""
        return ts_a < ts_b

    def _rss_save_dedup(self) -> None:
        """裁剪并持久化 RSS 去重容器。

        _rss_seen / _rss_washed 用 dict 保留插入顺序，超限时丢弃最早记录；
        旧实现用 set 转 list 后切片，丢弃的是随机记录，会导致老资源重复下载。
        """
        with self._rss_lk:
            if len(self._rss_seen) > self._rss_seen_max:
                self._rss_seen = dict.fromkeys(list(self._rss_seen)[-self._rss_seen_max:])
            if len(self._rss_washed) > self._rss_washed_max:
                self._rss_washed = dict.fromkeys(list(self._rss_washed)[-self._rss_washed_max:])
            seen_snapshot = list(self._rss_seen)
            washed_snapshot = list(self._rss_washed)
        self.save_data("rss_seen", seen_snapshot)
        self.save_data("rss_washed", washed_snapshot)

    def _rss_proc(self, url: str):
        """普通模式（未开启洗版）：不做 TMDB 识别，直接添加种子到下载器。
        去重由 _rss_seen（enclosure 有序集合，持久化）保证，避免重复添加同一个种子。"""
        try:
            items = RssHelper().parse(url)
        except Exception as e:
            logger.error(f"SC-RSS 解析 RSS 失败 [{url}]: {e}")
            return
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
                self._rss_seen[e] = None
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
        self._rss_save_dedup()

    def _rss_ck(self, m, meta: MetaInfo, season: Optional[int] = None, episode: Optional[int] = None) -> dict:
        """检查 RSS 资源对应媒体是否已达到洗版跳过条件。"""
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

    def _rss_latest_watched_ep(self, tmdb_id, season) -> Optional[int]:
        """洗版模式：查找某剧某季在播放缓存中“已有播放记录”的最大集号。

        用于跳过旧集：若缓存中已有第 10 集的播放记录，说明用户已看到第 10 集，
        则该季 1-9 集无需再洗版下载。只要该集存在任意播放记录（不论进度）即计入，
        因为“有记录”本身就代表已获取过更新的集。

        返回最大已观看集号；无任何记录返回 None。"""
        if not tmdb_id or season is None:
            return None
        prefix = f"{tmdb_id}:S{int(season):02d}E"
        latest = None
        with self._pb_lock:
            for r in self._pb:
                k = r.get("k") or ""
                if not k.startswith(prefix):
                    continue
                try:
                    ep = int(k[len(prefix):])
                except (ValueError, TypeError):
                    continue
                if latest is None or ep > latest:
                    latest = ep
        return latest

    def _rss_cached_seasons(self, tmdb_id: int) -> List[int]:
        """获取指定电视剧在播放缓存中已有记录的季号列表。"""
        if not tmdb_id:
            return []
        seasons = set()
        pattern = re.compile(rf"^{re.escape(str(tmdb_id))}:S(\d+)E")
        with self._pb_lock:
            for record in self._pb:
                match = pattern.match(str(record.get("k") or ""))
                if match:
                    seasons.add(int(match.group(1)))
        return sorted(seasons)

    @staticmethod
    def _normalize_api_media_name(name: str) -> str:
        """规范化媒体名称，用于让同一媒体的不同集共享缓存。"""
        return re.sub(r"[\W_]+", "", str(name or "").strip().casefold(), flags=re.UNICODE)

    @classmethod
    def _api_recognize_cache_key_from_name(cls, name: str) -> str:
        """根据媒体名称生成标题级独立缓存键。"""
        return cls._normalize_api_media_name(name)

    @classmethod
    def _api_recognize_cache_key(cls, meta: MetaInfo) -> str:
        """根据解析名称生成标题级独立缓存键，忽略媒体类型、季号和集号差异。"""
        name = str(getattr(meta, "name", "") or "").strip()
        # 仅按媒体标题缓存，使同一媒体不同集、类型推断差异和候选来源共享记录。
        return cls._api_recognize_cache_key_from_name(name)

    @classmethod
    def _normalize_api_cache_key(cls, key: str, name: str = "") -> str:
        """将旧版带媒体类型、季集字段的缓存键迁移为标题级缓存键。"""
        raw_key = str(key or "").strip()
        parts = raw_key.split("|")
        # 旧键格式为“媒体类型|名称|年份|季号|TMDBID”，仅保留名称，忽略其余字段。
        raw_name = str(name or "").strip() or (parts[1] if len(parts) > 1 else parts[0])
        return cls._api_recognize_cache_key_from_name(raw_name)

    def _load_api_recognize_cache(self) -> List[dict]:
        """加载独立负缓存，最多保留 _api_recognize_cache_max 条媒体标题记录。"""
        raw = self.get_data("api_recognize_cache") or []
        if not isinstance(raw, list):
            return []
        cache = []
        seen = set()
        for item in raw:
            if not isinstance(item, dict) or not item.get("negative"):
                continue
            old_key = str(item.get("key") or "").strip()
            if not old_key:
                continue
            name = str(item.get("name") or "").strip()
            key = self._normalize_api_cache_key(old_key, name=name)
            if key in seen:
                continue
            seen.add(key)
            cache.append({"key": key, "name": name, "negative": True})
        cache = cache[-self._api_recognize_cache_max:]
        # 清除旧版本保存的独立正缓存，避免无效数据继续占用缓存空间。
        if cache != raw:
            self.save_data("api_recognize_cache", cache)
        return cache

    def _load_api_recognize_success_cache(self) -> List[dict]:
        """加载识别成功独立正缓存，最多保留 100 条媒体标题识别结果。"""
        raw = self.get_data("api_recognize_success_cache") or []
        if not isinstance(raw, list):
            return []
        cache = []
        seen = set()
        for item in raw:
            if not isinstance(item, dict) or not item.get("tmdb_id"):
                continue
            key = str(item.get("key") or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            cache.append({
                "key": key,
                "name": str(item.get("name") or "").strip(),
                "media_type": item.get("media_type"),
                "title": str(item.get("title") or "").strip(),
                "year": str(item.get("year") or "").strip() or None,
                "tmdb_id": int(item.get("tmdb_id")),
            })
        cache = cache[-self._api_recognize_success_cache_max:]
        if cache != raw:
            self.save_data("api_recognize_success_cache", cache)
        return cache

    def _save_api_success_cache(self, key: str, name: str, media: MediaInfo) -> None:
        """保存一次识别成功结果到独立正缓存，超过 100 条时覆盖最早记录。"""
        if not key or not media or not media.tmdb_id:
            return
        entry = {
            "key": key,
            "name": str(name or "").strip(),
            "media_type": media.type.value if hasattr(media.type, "value") else str(media.type or ""),
            "title": str(media.title or "").strip(),
            "year": str(media.year or "").strip() or None,
            "tmdb_id": int(media.tmdb_id),
        }
        with self._api_recognize_cache_lock:
            self._api_recognize_success_cache = [
                item for item in self._api_recognize_success_cache
                if item.get("key") != key
            ]
            self._api_recognize_success_cache.append(entry)
            # 超出上限时丢弃最早的缓存（列表头部为最早写入的记录）。
            self._api_recognize_success_cache = self._api_recognize_success_cache[-self._api_recognize_success_cache_max:]
            snapshot = list(self._api_recognize_success_cache)
        self.save_data("api_recognize_success_cache", snapshot)
        logger.info(f"SC-RSS 写入识别成功独立缓存（{len(snapshot)}/{self._api_recognize_success_cache_max}）: "
                    f"{name} -> TMDB={media.tmdb_id} 《{media.title}》")

    def _get_api_success_cache_media(self, cache_key: str, meta: MetaInfo) -> Optional[MediaInfo]:
        """命中识别成功独立缓存时直接重建媒体信息，不调用 TMDB API。"""
        if not cache_key:
            return None
        entry = None
        with self._api_recognize_cache_lock:
            for item in reversed(self._api_recognize_success_cache):
                if item.get("key") == cache_key:
                    entry = item
                    break
        if not entry or not entry.get("tmdb_id"):
            return None
        # 同名不同年份（如 Alien 1979 / 2017）不能共用缓存，年份冲突时视为未命中
        meta_year = str(getattr(meta, "year", "") or "").strip()
        entry_year = str(entry.get("year") or "").strip()
        if meta_year and entry_year and meta_year != entry_year:
            return None
        try:
            media_type = entry.get("media_type")
            if isinstance(media_type, str):
                try:
                    media_type = MediaType(media_type)
                except (TypeError, ValueError):
                    media_type = MediaType.UNKNOWN
            media = MediaInfo(
                source="themoviedb",
                type=media_type or MediaType.UNKNOWN,
                title=entry.get("title") or getattr(meta, "name", ""),
                year=entry.get("year") or None,
                season=getattr(meta, "begin_season", None),
                tmdb_id=int(entry.get("tmdb_id")),
            )
            media.recognize_cache_hit = True
            return media
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _get_tmdb_local_cache_media(meta: MetaInfo) -> Optional[MediaInfo]:
        """从 MoviePilot 本地识别缓存直接重建媒体信息，不调用 TMDB API。"""
        original_type = getattr(meta, "type", None)
        try:
            from app.modules.themoviedb.tmdb_cache import TmdbCache

            candidates = []
            if original_type in (MediaType.TV, MediaType.MOVIE, MediaType.UNKNOWN):
                candidates.append(original_type)
            candidates.extend(mt for mt in (MediaType.TV, MediaType.MOVIE, MediaType.UNKNOWN) if mt not in candidates)
            cache = TmdbCache()
            for media_type in candidates:
                meta.type = media_type
                cached = cache.get(meta)
                if not isinstance(cached, dict) or not cached.get("id"):
                    continue
                cached_type = cached.get("type") or media_type
                if not isinstance(cached_type, MediaType):
                    try:
                        cached_type = MediaType(cached_type)
                    except (TypeError, ValueError):
                        cached_type = media_type
                media = MediaInfo(
                    source="themoviedb",
                    type=cached_type,
                    title=cached.get("title") or getattr(meta, "name", ""),
                    year=str(cached.get("year") or "") or None,
                    season=getattr(meta, "begin_season", None),
                    tmdb_id=int(cached.get("id")),
                )
                media.recognize_cache_hit = True
                return media
        except Exception as exc:
            logger.warning(f"SC-RSS 读取 MoviePilot 本地识别缓存失败: {exc}")
        finally:
            meta.type = original_type
        return None

    def _complete_media_by_tmdbid(self, meta: MetaInfo, media: MediaInfo) -> MediaInfo:
        """缓存命中后按 tmdb_id 补全媒体详情，并让 MoviePilot 的季集修正逻辑生效。

        独立正缓存与 MoviePilot 本地识别缓存只保存 tmdb_id、标题、年份等精简字段，
        据此重建的 MediaInfo 缺少 genre_ids、seasons、number_of_seasons 等详情；
        而 MoviePilot 生态里的季集修正（例如把动画的绝对集号 "- 79" 分离成 S04E13
        的插件，通过 TMDB 模块构建 MediaInfo 时的 set_category 钩子生效）依赖这些
        详情字段。若缓存命中后直接返回精简 MediaInfo，季集就会停留在报文解析出的
        绝对集号，与 MoviePilot 入库时的季集不一致。

        因此电视剧命中缓存后，用 tmdb_id 调用 TMDB 模块按 ID 查询一次详情
        （不是按名称搜索识别，开销远小于重新识别），复用 MoviePilot 的标准构建流程，
        同时让季集修正作用在同一个 meta 上。补全失败时保留精简结果，不影响下载。
        """
        try:
            if not media or not getattr(media, "tmdb_id", None):
                return media
            if getattr(media, "type", None) != MediaType.TV:
                # 电影无季集换算需求，无需额外请求详情
                return media
            tmdb_module = self.chain.modulemanager.get_running_module("TheMovieDbModule")
            if not tmdb_module:
                return media
            before = (meta.begin_season, meta.begin_episode)
            full = tmdb_module.recognize_media(meta=meta, mtype=MediaType.TV,
                                               tmdbid=int(media.tmdb_id))
            if not full:
                self._rss_log("详情补全失败", getattr(media, "title", ""),
                              f"TMDB={media.tmdb_id} 未取到详情，沿用缓存精简信息")
                return media
            after = (meta.begin_season, meta.begin_episode)
            if after != before:
                self._rss_log("季集修正", getattr(full, "title", ""),
                              f"缓存命中后按 TMDB 详情修正 "
                              f"S{before[0] or 1:02d}E{before[1] or 0:02d} -> "
                              f"S{after[0] or 1:02d}E{after[1] or 0:02d}")
            return full
        except Exception as exc:
            self._rss_log("详情补全异常", getattr(media, "title", ""), str(exc))
            return media

    def _has_api_negative_cache(self, key: str) -> bool:
        """判断是否已有该解析名称的 TMDB 识别失败缓存。"""
        if not key:
            return False
        with self._api_recognize_cache_lock:
            for index in range(len(self._api_recognize_cache) - 1, -1, -1):
                entry = self._api_recognize_cache[index]
                if entry.get("key") != key or not entry.get("negative"):
                    continue
                self._api_recognize_cache.append(self._api_recognize_cache.pop(index))
                return True
        return False

    def _save_api_negative_cache(self, key: str, name: str) -> None:
        """保存一次 TMDB 官方 API 识别失败结果，负缓存上限 _api_recognize_cache_max 条。"""
        if not key:
            return
        entry = {
            "key": key,
            "name": str(name or "").strip(),
            "negative": True,
        }
        with self._api_recognize_cache_lock:
            self._api_recognize_cache = [
                item for item in self._api_recognize_cache
                if item.get("key") != key
            ]
            self._api_recognize_cache.append(entry)
            self._api_recognize_cache = self._api_recognize_cache[-self._api_recognize_cache_max:]
            snapshot = list(self._api_recognize_cache)
        self.save_data("api_recognize_cache", snapshot)
        logger.info(f"SC-RSS 写入 TMDB API 失败独立缓存（{len(snapshot)}/{self._api_recognize_cache_max}）: {name}")

    def _rss_filename_fallback(self, item: dict, rt: str, reason: str):
        """在 RSS 报文识别失败或季号不一致时，按配置回退到种子文件名识别。"""
        if not self._rss_fname_identify:
            return None, None, ""
        self._rss_log("文件名回退", rt, reason)
        return self._rss_id(item, rt, filename_only=True)

    # ==================== 智能助手识别兜底 ====================

    @staticmethod
    def _run_coro(coro):
        """在插件线程里同步执行协程；当前线程已有事件循环时另起线程执行。"""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        box: Dict[str, Any] = {}

        def _worker():
            try:
                box["value"] = asyncio.run(coro)
            except BaseException as exc:  # noqa: BLE001
                box["error"] = exc

        t = threading.Thread(target=_worker, daemon=True, name="SC-RSS-AI")
        t.start()
        t.join()
        if "error" in box:
            raise box["error"]
        return box.get("value")

    def _rss_ai_ask(self, prompt: str) -> str:
        """调用「设定-智能助手」中配置的 LLM，返回纯文本回复。"""
        from app.agent.llm.helper import LLMHelper

        async def _call():
            llm = await LLMHelper.get_llm(streaming=False)
            return await asyncio.wait_for(llm.ainvoke(prompt), timeout=self._rss_ai_timeout)

        response = self._run_coro(_call())
        return LLMHelper.extract_text_content(getattr(response, "content", response), fallback_to_string=True).strip()

    @staticmethod
    def _rss_ai_parse_json(text: str) -> Optional[dict]:
        """从 LLM 回复中提取 JSON 对象，兼容 ```json 代码块与前后多余说明文字。"""
        raw = str(text or "").strip()
        if not raw:
            return None
        # 去掉 ``` 代码块围栏
        fence = re.search(r"```(?:json)?\s*(.+?)\s*```", raw, re.DOTALL | re.IGNORECASE)
        if fence:
            raw = fence.group(1).strip()
        if not raw.startswith("{"):
            start = raw.find("{")
            end = raw.rfind("}")
            if start < 0 or end <= start:
                return None
            raw = raw[start:end + 1]
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return None
        return data if isinstance(data, dict) else None

    def _rss_ai_prompt(self, rt: str, reason: str, meta, cached_seasons: Optional[List[int]],
                       file_names: Optional[List[str]] = None) -> str:
        """构造智能助手识别提示词：给出原始命名、MoviePilot 解析结果与识别词格式说明。"""
        hint_lines = []
        if meta is not None:
            hint_lines.append(f"解析名称: {getattr(meta, 'name', '') or '（空）'}")
            hint_lines.append(f"解析季号: {getattr(meta, 'begin_season', None)}")
            hint_lines.append(f"解析集号: {getattr(meta, 'begin_episode', None)}")
            hint_lines.append(f"套用的自定义识别词: {getattr(meta, 'apply_words', None) or '无'}")
        cached_text = "、".join(f"S{s:02d}" for s in (cached_seasons or [])) or "无"
        files_text = "\n".join(f"- {n}" for n in (file_names or [])[:8]) or "（未获取）"
        return (
            "你是 MoviePilot 的媒体识别专家。下面是一条 BT/RSS 资源的原始命名，MoviePilot 未能正确识别，请你判断它到底是哪部影视作品的第几季第几集。\n\n"
            f"【原始报文标题】\n{rt}\n\n"
            f"【种子内视频文件名】\n{files_text}\n\n"
            f"【MoviePilot 当前解析结果】\n" + ("\n".join(hint_lines) or "（无）") + "\n\n"
            f"【本地媒体库/播放缓存中该剧已有的季】\n{cached_text}\n\n"
            f"【触发原因】\n{reason}\n\n"
            "要求：\n"
            "1. 番剧常见的绝对集号（如 “- 91”、“- 19(91)”）需要换算成 TMDB 的季/集编号；若本地已有季信息，结果应与之保持同一分季策略。\n"
            "2. tmdb_id 必须是你确定的真实 TMDB ID，不确定就留空（null），不要编造。\n"
            "3. 额外给出一条 MoviePilot 自定义识别词，使下次同系列命名能被自动识别。识别词格式为 `被替换词 => 替换词`：\n"
            "   - 被替换词是作用在原始标题上的 Python 正则，必须包含该系列的专有片段（英文/罗马字原名、季次标记等），集号用捕获组 (\\d{1,3})；\n"
            "   - 替换词形如 `中文标题.S04E\\1 {[tmdbid=82684;type=tv;]}`，其中 \\1 引用集号捕获组，季号写死两位数；\n"
            "   - 只针对该系列做窄匹配，绝对不要写会命中其他作品的通用规则（例如不要只匹配 `(\\d+)` 或 `\\[(\\d+)\\]`）。\n"
            "4. 只输出 JSON，不要输出解释文字、不要用代码块。\n\n"
            "JSON 字段：\n"
            "{\n"
            '  "title": "中文标题（没有中文用原名）",\n'
            '  "year": "首播年份，如 2024，未知留空",\n'
            '  "media_type": "tv 或 movie",\n'
            '  "tmdb_id": 数字或 null,\n'
            '  "season": 季号数字（电影填 null）,\n'
            '  "episode": 集号数字（电影填 null）,\n'
            '  "word": "被替换词 => 替换词",\n'
            '  "reason": "一句话依据"\n'
            "}"
        )

    @staticmethod
    def _rss_ai_int(value: Any) -> Optional[int]:
        """把 LLM 回复里的数字字段安全转成 int，无效返回 None。"""
        if value is None or isinstance(value, bool):
            return None
        try:
            result = int(str(value).strip())
        except (TypeError, ValueError):
            return None
        return result if result >= 0 else None

    def _rss_ai_build_word(self, rt: str, title: str, season: Optional[int], episode: Optional[int],
                           tmdb_id: Optional[int], is_tv: bool) -> str:
        """兜底自造自定义识别词：把标题中与集号一致的数字换成捕获组，其余原文转义。

        作为 LLM 给出的识别词无效时的保底方案，只匹配该系列本次这种命名，
        不会命中其他作品；正片集号变化时仍能复用。
        """
        if not title or not is_tv or episode is None:
            return ""
        matches = [m for m in re.finditer(r"\d{1,4}", rt) if int(m.group()) == int(episode)]
        if not matches:
            return ""
        # 集号通常出现在标题靠后位置（前面可能有 "4th"、"2024" 等数字）
        target = matches[-1]
        prefix = rt[:target.start()]
        if len(prefix.strip()) < 4:
            return ""
        digits = len(target.group())
        pattern = re.escape(prefix) + r"(\d{%d,%d})" % (digits, max(digits, 3))
        tag = f" {{[tmdbid={int(tmdb_id)};type=tv;]}}" if tmdb_id else ""
        return f"{pattern} => {title}.S{int(season or 1):02d}E\\1{tag}"

    @staticmethod
    def _rss_ai_word_format_ok(word: str) -> bool:
        """校验识别词格式：必须是替换型，且被替换词是可编译的正则、不是空泛匹配。"""
        text = str(word or "").strip()
        if " => " not in text or text.startswith("#"):
            return False
        replaced, _, replace = text.partition(" => ")
        replaced, replace = replaced.strip(), replace.strip()
        if not replaced or not replace:
            return False
        # 拒绝过短或纯数字捕获的空泛规则，避免全局误伤其他资源
        bare = re.sub(r"\\d\{[\d,]*\}|\\d|[\\()\[\]{}?*+.^$|]", "", replaced)
        if len(bare.strip()) < 3:
            return False
        try:
            re.compile(replaced)
        except re.error:
            return False
        return True

    def _rss_ai_word_verify(self, word: str, rt: str, season: Optional[int], episode: Optional[int],
                            tmdb_id: Optional[int], is_tv: bool) -> bool:
        """用「现有识别词 + 新识别词」实际解析一次原始标题，验证新词能得到期望的季集。"""
        if not self._rss_ai_word_format_ok(word):
            self._rss_log("识别词校验失败", word, "格式或正则无效")
            return False
        try:
            words = list(self._current_custom_words()) + [word]
            meta = MetaInfo(title=rt, custom_words=words)
        except Exception as exc:
            self._rss_log("识别词校验异常", word, str(exc))
            return False
        applied = getattr(meta, "apply_words", None) or []
        if word not in applied:
            self._rss_log("识别词校验失败", word, "该识别词未命中原始标题")
            return False
        if is_tv:
            got_season = getattr(meta, "begin_season", None)
            got_episode = getattr(meta, "begin_episode", None)
            if episode is not None and got_episode != int(episode):
                self._rss_log("识别词校验失败", word, f"解析集号 {got_episode} != 期望 {episode}")
                return False
            if season is not None and (got_season or 1) != int(season):
                self._rss_log("识别词校验失败", word, f"解析季号 {got_season} != 期望 {season}")
                return False
        if tmdb_id and getattr(meta, "tmdbid", None) and int(meta.tmdbid) != int(tmdb_id):
            self._rss_log("识别词校验失败", word, f"解析 TMDB {meta.tmdbid} != 期望 {tmdb_id}")
            return False
        return True

    @staticmethod
    def _current_custom_words() -> List[str]:
        """读取当前生效的自定义识别词列表。"""
        try:
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey

            return list(SystemConfigOper().get(SystemConfigKey.CustomIdentifiers) or [])
        except Exception as exc:
            logger.warning(f"SC-RSS 读取自定义识别词失败: {exc}")
            return []

    def _rss_ai_save_word(self, word: str) -> bool:
        """把校验通过的识别词追加到「设定-自定义识别词」末尾，立即生效。"""
        try:
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey

            oper = SystemConfigOper()
            words = list(oper.get(SystemConfigKey.CustomIdentifiers) or [])
            if word in words:
                self._rss_log("识别词已存在", word, "跳过写入")
                return True
            words.append(word)
            oper.set(SystemConfigKey.CustomIdentifiers, words)
            self._rss_log("写入自定义识别词", word, f"当前共 {len(words)} 条")
            return True
        except Exception as exc:
            self._rss_log("写入自定义识别词失败", word, str(exc))
            return False

    def _drop_api_negative_cache_by_title(self, rt: str) -> None:
        """智能助手识别成功后，清掉该标题相关的独立负缓存，避免继续被跳过。"""
        keys = set()
        try:
            for cand in [rt] + list(self._rss_title_candidates(rt)):
                meta = MetaInfo(title=cand)
                if getattr(meta, "name", ""):
                    keys.add(self._api_recognize_cache_key(meta))
        except Exception:
            return
        if not keys:
            return
        with self._api_recognize_cache_lock:
            kept = [item for item in self._api_recognize_cache if item.get("key") not in keys]
            if len(kept) == len(self._api_recognize_cache):
                return
            self._api_recognize_cache = kept
            snapshot = list(kept)
        self.save_data("api_recognize_cache", snapshot)
        self._rss_log("清除独立负缓存", rt, "智能助手已识别成功")

    def _rss_ai_recognize_media(self, meta, guess: dict) -> Optional[MediaInfo]:
        """按智能助手给出的 TMDB ID（或标题）查询 TMDB，拿到完整媒体信息。"""
        tmdb_module = self.chain.modulemanager.get_running_module("TheMovieDbModule")
        if not tmdb_module:
            self._rss_log("智能助手识别异常", getattr(meta, "name", ""), "TMDB 官方识别模块未运行")
            return None
        mtype = MediaType.TV if str(guess.get("media_type") or "tv").lower() != "movie" else MediaType.MOVIE
        tmdb_id = self._rss_ai_int(guess.get("tmdb_id")) or self._rss_ai_int(getattr(meta, "tmdbid", None))
        try:
            if tmdb_id:
                return tmdb_module.recognize_media(meta=meta, mtype=mtype, tmdbid=int(tmdb_id))
            # 未给出 TMDB ID：用智能助手判断的标题+年份重新按名称识别
            title = str(guess.get("title") or "").strip()
            if not title:
                return None
            year = str(guess.get("year") or "").strip()
            probe = MetaInfo(title=f"{title} ({year})" if year else title)
            probe.type = mtype
            if mtype == MediaType.TV:
                probe.begin_season = self._rss_ai_int(guess.get("season")) or 1
            return tmdb_module.recognize_media(meta=probe, cache=False)
        except Exception as exc:
            self._rss_log("智能助手识别异常", getattr(meta, "name", ""), f"TMDB 查询失败: {exc}")
            return None

    def _rss_ai_fallback(self, item: dict, rt: str, reason: str,
                         cached_seasons: Optional[List[int]] = None):
        """智能助手识别兜底。

        触发场景：RSS 报文与种子文件名都识别失败、未识别到 TMDB ID、电视剧无集号，
        或识别出的季号与本地播放缓存的分季策略不一致。

        流程：把原始命名、MoviePilot 解析结果、本地已有季交给「设定-智能助手」配置的
        LLM，要求返回 JSON（标题/类型/TMDB ID/季集 + 一条自定义识别词）；随后
        1) 校验识别词（现有识别词 + 新词实际解析一次原始标题，季集/TMDB 必须符合预期），
           校验通过则写入「设定-自定义识别词」，下次由 MoviePilot 自行识别；
        2) 按 TMDB ID 查询完整媒体信息，季集以识别词解析结果为准，缺失时用 LLM 结果补齐；
        3) 清掉该标题的独立负缓存并写入独立正缓存。

        返回 (media, meta, name)，失败返回 (None, None, "")。
        """
        if not self._rss_ai_identify:
            return None, None, ""
        key = self._normalize_api_media_name(rt)
        if key and key in self._rss_ai_failed:
            self._rss_log("智能助手跳过", rt, f"本轮已失败：{self._rss_ai_failed[key]}")
            return None, None, ""
        if self._rss_ai_calls >= self._rss_ai_max:
            self._rss_log("智能助手跳过", rt, f"已达本轮调用上限 {self._rss_ai_max} 次")
            return None, None, ""
        self._rss_ai_calls += 1
        self._rss_log("智能助手识别", rt, f"{reason}（第 {self._rss_ai_calls}/{self._rss_ai_max} 次调用）")

        base_meta = None
        try:
            base_meta = MetaInfo(title=rt)
        except Exception:
            pass
        file_names = []
        if self._rss_fname_identify:
            try:
                file_names = self._rss_fnames(item.get("enclosure", "") or item.get("link", ""))
            except Exception:
                file_names = []

        try:
            reply = self._rss_ai_ask(self._rss_ai_prompt(rt, reason, base_meta, cached_seasons, file_names))
        except Exception as exc:
            self._rss_log("智能助手调用失败", rt, str(exc))
            if key:
                self._rss_ai_failed[key] = "调用失败"
            return None, None, ""
        guess = self._rss_ai_parse_json(reply)
        if not guess:
            self._rss_log("智能助手回复无效", rt, (reply or "")[:200])
            if key:
                self._rss_ai_failed[key] = "回复无法解析"
            return None, None, ""

        title = str(guess.get("title") or "").strip()
        tmdb_id = self._rss_ai_int(guess.get("tmdb_id"))
        is_tv = str(guess.get("media_type") or "tv").lower() != "movie"
        season = self._rss_ai_int(guess.get("season")) if is_tv else None
        episode = self._rss_ai_int(guess.get("episode")) if is_tv else None
        if is_tv and season is None:
            season = 1
        self._rss_log("智能助手结果", rt,
                      f"《{title}》 TMDB={tmdb_id or '无'} "
                      f"{'S%02d' % season if season is not None else ''}"
                      f"{'E%02d' % episode if episode is not None else ''} "
                      f"依据={str(guess.get('reason') or '')[:80]}")
        if not title and not tmdb_id:
            self._rss_log("智能助手识别失败", rt, "未给出标题与 TMDB ID")
            if key:
                self._rss_ai_failed[key] = "结果不完整"
            return None, None, ""
        if is_tv and episode is None:
            self._rss_log("智能助手识别失败", rt, "电视剧未给出集号")
            if key:
                self._rss_ai_failed[key] = "缺少集号"
            return None, None, ""
        if cached_seasons and is_tv and season is not None and int(season) not in cached_seasons:
            cached_text = "、".join(f"S{s:02d}" for s in cached_seasons)
            self._rss_log("智能助手结果不采用", rt,
                          f"S{int(season):02d} 仍与播放缓存季（{cached_text}）不一致")
            if key:
                self._rss_ai_failed[key] = "季号仍不一致"
            return None, None, ""

        # 生成并校验自定义识别词：优先用智能助手给出的，无效时自造窄匹配规则
        word_saved = ""
        if self._rss_ai_add_words:
            candidates = [str(guess.get("word") or "").strip(),
                          self._rss_ai_build_word(rt, title, season, episode, tmdb_id, is_tv)]
            for cand in candidates:
                if not cand:
                    continue
                if self._rss_ai_word_verify(cand, rt, season, episode, tmdb_id, is_tv):
                    if self._rss_ai_save_word(cand):
                        word_saved = cand
                    break

        # 识别词生效后重新解析原始标题；未写入识别词时直接用智能助手给出的季集
        meta = None
        if word_saved:
            try:
                meta = MetaInfo(title=rt)
                self._rss_log_meta("识别词生效后重解析", rt, meta)
            except Exception:
                meta = None
        if meta is None:
            try:
                meta = MetaInfo(title=rt)
            except Exception:
                self._rss_log("智能助手识别失败", rt, "标题解析异常")
                if key:
                    self._rss_ai_failed[key] = "标题解析异常"
                return None, None, ""
        if is_tv:
            if getattr(meta, "begin_season", None) is None or int(meta.begin_season) != int(season):
                meta.begin_season = int(season)
            if getattr(meta, "begin_episode", None) is None or int(meta.begin_episode) != int(episode):
                meta.begin_episode = int(episode)
            if not getattr(meta, "episode_list", None):
                meta.episode_list = [int(episode)]

        media = self._rss_ai_recognize_media(meta, guess)
        if not media or not getattr(media, "tmdb_id", None):
            self._rss_log("智能助手识别失败", rt, "TMDB 未查到对应媒体")
            if key:
                self._rss_ai_failed[key] = "TMDB 查询失败"
            return None, None, ""
        # TMDB 详情查询可能重置季集（按 ID 查询会重建 meta 的季信息），这里再兜一次
        if is_tv:
            if getattr(meta, "begin_episode", None) is None:
                meta.begin_episode = int(episode)
            if getattr(meta, "begin_season", None) is None:
                meta.begin_season = int(season)
        self._drop_api_negative_cache_by_title(rt)
        try:
            self._save_api_success_cache(self._api_recognize_cache_key(meta), getattr(meta, "name", "") or title, media)
        except Exception:
            pass
        se_text = f"S{int(meta.begin_season or 1):02d}E{int(meta.begin_episode):02d}" if is_tv else "电影"
        self._rss_log("智能助手识别成功", media.title,
                      f"TMDB={media.tmdb_id} {se_text}"
                      + (f"，已写入识别词：{word_saved}" if word_saved else "，未写入识别词"))
        if self._rss_ntf:
            self.post_message(
                title="SC-RSS 智能助手识别成功",
                text=f"资源: {rt}\n识别: {media.title} {se_text}\nTMDB: {media.tmdb_id}\n"
                     + (f"已添加识别词: {word_saved}" if word_saved else "未添加识别词（校验未通过）")
            )
        return media, meta, rt

    def _rss_id(self, item: dict, rt: str, filename_only: bool = False):
        """洗版模式：优先识别 RSS 报文，必要时用种子文件名回退识别。

        默认先使用 RSS 报文标题识别；filename_only 为 True 时仅下载种子并使用视频文件名识别，
        供报文识别失败、缺少集号或季号与播放缓存不一致时回退使用。

        识别时统一走 MoviePilot 的 MetaInfo()，它在解析前会自动套用用户在
        “设定-自定义识别词”里配置的识别词（屏蔽/替换/集偏移等），套用结果记录在
        meta.apply_words；随后按空间清理器独立缓存（识别成功正缓存/识别失败负缓存）、
        MoviePilot 本地识别缓存、TMDB 官方 API 的顺序识别。
        独立正缓存命中时直接用缓存重建媒体信息，不再请求 TMDB；独立负缓存命中时直接
        跳过识别和下载；MoviePilot 本地缓存命中时直接重建媒体信息；官方 API 识别成功的
        结果写入独立正缓存（最多 100 条，超出覆盖最早记录），API 识别失败的标题级负缓存
        最多 5 条。

        整个识别过程会写入日志：候选来源、原始串、套用的识别词、解析出的标题/季集、
        以及最终 TMDB 命中结果，便于排查识别错误。

        返回 (media, meta, title_name)，title_name 为用于识别的候选名称。"""
        # 仅 filename_only 回退分支下载种子；默认报文识别不下载种子。
        enc = item.get("enclosure", "") or item.get("link", "")
        fns = self._rss_fnames(enc) if filename_only and self._rss_fname_identify else []
        # 当前 RSS 资源内暂存所有失败候选；所有候选全部失败时才落盘负缓存。
        failed_candidates: Dict[str, str] = {}

        def _try_recognize(title: str, subtitle: str = "", source_label: str = ""):
            """尝试用给定标题做独立缓存、本地识别缓存和 TMDB 识别，返回 (media, meta, ok)。"""
            meta = MetaInfo(title=title, subtitle=subtitle)
            self._rss_log_meta(source_label, title, meta)
            if not meta.name:
                return None, meta, False

            cache_key = self._api_recognize_cache_key(meta)

            # 第一层：读取空间清理器识别成功独立缓存，命中后直接用缓存重建媒体信息。
            success_media = self._get_api_success_cache_media(cache_key, meta)
            if success_media:
                self._rss_log("命中空间清理器识别成功独立缓存", title,
                              f"TMDB={success_media.tmdb_id} 《{success_media.title}》")
                success_media = self._complete_media_by_tmdbid(meta, success_media)
                return success_media, meta, True

            # 第二层：读取空间清理器独立负缓存；命中后直接跳过识别及添加下载。
            if cache_key in failed_candidates:
                self._rss_log("命中本条 RSS 临时负缓存", title, "相同标题已失败，跳过重复 API 调用")
                return None, meta, False
            if self._has_api_negative_cache(cache_key):
                failed_candidates[cache_key] = meta.name
                self._rss_log("命中空间清理器独立负缓存", title, "跳过识别和下载")
                return None, meta, False

            # 第三层：读取 MoviePilot 自带识别缓存并重建 MediaInfo，不请求 TMDB 详情接口。
            native_media = self._get_tmdb_local_cache_media(meta)
            if native_media:
                self._rss_log("命中TMDB识别缓存", title,
                              f"TMDB={native_media.tmdb_id} 《{native_media.title}》")
                # 识别成功结果同步写入独立正缓存，后续相同标题报文直接命中。
                self._save_api_success_cache(cache_key, meta.name, native_media)
                native_media = self._complete_media_by_tmdbid(meta, native_media)
                return native_media, meta, True

            # 第四层：以上均未命中，直接调用正在运行的 TMDB 官方模块并绕过其识别缓存。
            tmdb_module = self.chain.modulemanager.get_running_module("TheMovieDbModule")
            if not tmdb_module:
                self._rss_log("识别异常", title, "TMDB 官方识别模块未运行")
                return None, meta, False
            try:
                media = tmdb_module.recognize_media(meta=meta, cache=False)
            except Exception as exc:
                self._rss_log("识别异常", title, f"TMDB 官方 API 调用失败: {exc}")
                return None, meta, False

            if media:
                # 写入识别成功独立缓存，后续相同标题报文直接使用缓存结果。
                self._save_api_success_cache(cache_key, meta.name, media)
                self._rss_log("TMDB官方API识别成功", title,
                              f"TMDB={media.tmdb_id} 《{media.title}》")
                return media, meta, True

            # 当前候选失败先暂存；只有本条 RSS 的所有候选都失败时，外层统一写入负缓存。
            failed_candidates[cache_key] = meta.name
            self._rss_log("识别未命中", title, "TMDB 官方 API 未匹配到媒体，暂存失败候选")
            return None, meta, False

        file_media, file_meta, file_base = None, None, ""
        # 仅在回退分支使用种子内视频文件名识别。
        if filename_only and fns:
            self._rss_log("识别", rt, f"种子含 {len(fns)} 个文件，优先用视频文件名识别")
            best_file_media, best_file_meta, best_file_base = None, None, ""
            for fn in fns:
                try:
                    base = fn.rsplit("/", 1)[-1]
                    media, meta, ok = _try_recognize(base, source_label="文件名候选")
                    if not ok:
                        continue
                    # 优先选用带集号的识别结果
                    if meta.begin_episode is not None:
                        return media, meta, base
                    if best_file_media is None:
                        best_file_media, best_file_meta, best_file_base = media, meta, base
                except Exception as ex:
                    self._rss_log("识别异常", fn, str(ex))
                    continue
            # 所有文件名候选都无集号，保留第一个成功的，从原始标题补充集号
            if best_file_media is not None:
                media, meta, base = best_file_media, best_file_meta, best_file_base
                meta = self._rss_merge_episode_from_title(meta, rt)
                if meta.begin_episode is not None:
                    return media, meta, base
                # 文件名识别无集号且标题补充失败：继续回退标题识别兜底
                self._rss_log("文件名无集号", rt, "视频文件名无法解析集号，回退标题识别")
                file_media, file_meta, file_base = media, meta, base
            else:
                file_media, file_meta, file_base = None, None, ""
        if filename_only:
            # 电视剧回退只接受带集号的结果，避免把整季文件误当作单集下载；电影没有集号，直接接受。
            if file_media is not None and file_meta is not None:
                is_movie = getattr(file_media, "type", None) == MediaType.MOVIE
                if file_meta.begin_episode is not None or is_movie:
                    return file_media, file_meta, file_base
            for failed_key, failed_name in failed_candidates.items():
                self._save_api_negative_cache(failed_key, failed_name)
            return None, None, ""

        # 直接使用 RSS 报文标题识别：按 "/" 拆分的不同译名逐个作为候选
        # （每个候选 = 译名 + 集号/质量标记，避免多个译名合并识别导致失败）。
        cands = self._rss_title_candidates(rt)
        self._rss_log("识别", rt, f"标题识别，共 {len(cands)} 个候选")
        best_media, best_meta, best_c = None, None, ""
        for c in cands:
            media, meta, ok = _try_recognize(c, subtitle=item.get("description", ""), source_label="标题候选")
            if not ok:
                continue
            # 优先选用带集号的识别结果
            if meta.begin_episode is not None:
                return media, meta, c
            if best_media is None:
                best_media, best_meta, best_c = media, meta, c
        if best_media is not None:
            self._rss_log("识别回退", best_c, "所有候选均无集号，从原始标题补充")
            meta = self._rss_merge_episode_from_title(best_meta, rt)
            return best_media, meta, best_c
        # 标题识别全部失败：若文件名识别有成功结果（仅缺集号），返回该结果避免误写负缓存
        if file_media is not None:
            return file_media, file_meta, file_base

        # 所有标题候选均失败后，才写入负缓存；同一标题只写一条。
        for failed_key, failed_name in failed_candidates.items():
            self._save_api_negative_cache(failed_key, failed_name)
        return None, None, ""

    @classmethod
    def _rss_title_candidates(cls, rt: str) -> List[str]:
        """把 RSS 报文标题拆分为多个识别候选。

        标题以 "/" 分隔不同译名（如 "[字幕组] 译名A / 译名B / 英文名 [04][1080P]"）时，
        每个译名 + 集号/质量标记组成一个候选，取其中一个译名识别；
        无 "/" 时直接用原始标题作为唯一候选（MetaInfo 能自动跳过【发布组】前缀）。
        """
        if "/" not in rt:
            return [rt]
        # 去掉开头的发布组标记（如 [字幕组]、【字幕组】★07月新番★）
        body = re.sub(r'^[\[【][^\]】\[]+[\]】]\s*(?:★[^★]*★\s*)?', "", rt).strip()
        parts = [p.strip() for p in body.split("/") if p.strip()] or [body]
        last = parts[-1]
        # 提取最后一个译名段末尾的集号/质量标记（[...] 序列）
        tail_match = re.search(r'((?:\[[^\[\]]*\]\s*)+)$', last)
        tail = tail_match.group(1).strip() if tail_match else ""
        cands = []
        for i, p in enumerate(parts):
            if i == len(parts) - 1 and tail:
                name = last[:len(last) - len(tail)].strip()
            else:
                name = p
            # 清理段首尾多余括号与段内其他标记
            name = name.strip('[【').strip('】]').strip()
            name = re.sub(r'\[[^\[\]]*\]', '', name).strip()
            cand = (name + " " + tail).strip()
            if cand and name:
                cands.append(cand)
        return cands or [rt]

    # 视频文件扩展名（种子文件名识别时按这些扩展名筛选文件）
    _VIDEO_EXTS_TORRENT = (".mp4", ".mkv", ".avi", ".ts", ".m2ts", ".wmv", ".mov",
                   ".flv", ".rmvb", ".rm", ".mpg", ".mpeg", ".webm", ".iso")

    @staticmethod
    def _rss_http_torrent(enc: str, proxies: Optional[dict] = None) -> Tuple[Optional[bytes], str]:
        """请求 .torrent 文件内容，返回 (内容, 错误信息)。"""
        try:
            r = RequestUtils(timeout=30, proxies=proxies).get_res(enc)
        except Exception as exc:
            return None, str(exc)
        if not r or r.status_code != 200:
            return None, f"status={getattr(r, 'status_code', None)}"
        if not r.content:
            return None, "响应内容为空"
        return r.content, ""

    def _rss_fetch_torrent(self, enc: str, tag: str = "") -> Optional[bytes]:
        """下载 .torrent 文件内容；失败且开启「代理重试」时，用系统代理服务器再重试一次。"""
        if not enc:
            return None
        content, err = self._rss_http_torrent(enc)
        if content is not None:
            return content
        if not self._rss_proxy_retry:
            logger.warning(f"SC-RSS 下载种子文件失败{tag}: {enc} {err}")
            return None
        if not settings.PROXY:
            logger.warning(f"SC-RSS 下载种子文件失败{tag}: {enc} {err}，未配置代理服务器，无法重试")
            return None
        logger.info(f"SC-RSS 下载种子文件失败{tag}: {err}，使用代理服务器重试")
        content, perr = self._rss_http_torrent(enc, proxies=settings.PROXY)
        if content is not None:
            logger.info(f"SC-RSS 代理重试下载种子文件成功{tag}: {enc}")
            return content
        logger.warning(f"SC-RSS 代理重试下载种子文件仍失败{tag}: {enc} {perr}")
        return None

    def _rss_fnames(self, enc: str) -> List[str]:
        """下载种子文件并解析文件列表，仅返回视频文件（按体积从大到小），
        无视频文件时回退到全部文件名。"""
        if not enc:
            return []
        try:
            import bencode

            content = self._rss_fetch_torrent(enc, tag="（文件名识别）")
            if not content:
                return []
            t = bencode.bdecode(content)
            info = t.get("info", {})
            files = info.get("files", [])
            if files:
                all_files = []  # (path, length)
                for f in files:
                    parts = [p.decode("utf-8", errors="replace") if isinstance(p, bytes) else str(p) for p in f.get("path", [])]
                    if parts:
                        all_files.append(("/".join(parts), f.get("length", 0) or 0))
                # 优先取视频文件，按体积从大到小（正片通常最大）
                videos = [(p, l) for p, l in all_files if p.lower().endswith(self._VIDEO_EXTS_TORRENT)]
                if videos:
                    videos.sort(key=lambda x: x[1], reverse=True)
                    return [p for p, _ in videos]
                return [p for p, _ in all_files]
            name = info.get("name", "")
            if isinstance(name, bytes):
                name = name.decode("utf-8", errors="replace")
            return [name] if name else []
        except Exception as exc:
            logger.warning(f"SC-RSS 解析种子文件失败: {enc} err={exc}")
            return []

    def _rss_merge_episode_from_title(self, meta, rt: str):
        """当 MetaInfo 解析结果缺少集号时，从原始 RSS 标题中重新提取季集信息，
        合并到 meta 中，确保插件识别的季集与 MP 入库时一致。

        MP 入库时 DownloadChain.download_single 使用 context.meta_info 的
        season/episode 写入下载记录，因此此处修改 meta.begin_season/begin_episode
        能直接影响入库季集。"""
        if meta.begin_episode is not None:
            return meta
        # 从原始 RSS 标题重新解析季集（直接用完整标题，MetaInfo 能跳过【发布组】等前缀）
        fallback = MetaInfo(title=rt)
        if fallback.begin_episode is None:
            # MetaInfo 无法解析集号时，兼容 "- 17(89)"（第17话/总第89话）、"E17/EP17"、"第17话" 等格式
            mm = re.search(r'[-_]\s*(\d+)\s*\(\s*\d+\s*\)', rt)
            if not mm:
                mm = re.search(r'(?i)(?:E|EP)\s*(\d+)', rt)
            if not mm:
                mm = re.search(r'第\s*(\d+)\s*[话集]', rt)
            if mm:
                fallback.begin_episode = int(mm.group(1))
        if fallback.begin_episode is not None:
            self._rss_log("季集补充", rt,
                          f"从原始标题提取 S{fallback.begin_season or 1:02d}E{fallback.begin_episode:02d}")
            meta.begin_season = fallback.begin_season if fallback.begin_season is not None else (meta.begin_season or 1)
            meta.begin_episode = fallback.begin_episode
            # 同时更新 episode_list 供 MP 下载链使用
            if not hasattr(meta, "episode_list") or meta.episode_list is None:
                meta.episode_list = [fallback.begin_episode]
        return meta

    def _rss_log_meta(self, stage: str, raw: str, meta) -> None:
        """输出 MetaInfo 的解析细节：套用的自定义识别词、解析出的名称与季集。"""
        try:
            parts = []
            name = getattr(meta, "name", "") or ""
            if name:
                parts.append(f"解析名称={name}")
            bs = getattr(meta, "begin_season", None)
            be = getattr(meta, "begin_episode", None)
            if bs is not None:
                parts.append(f"季={bs}")
            if be is not None:
                parts.append(f"集={be}")
            aw = getattr(meta, "apply_words", None)
            if aw:
                parts.append(f"套用识别词={aw}")
            else:
                parts.append("套用识别词=无")
            self._rss_log(stage, raw, "，".join(parts))
        except Exception:
            pass

    def _rss_tv_season_episode(self, m, meta, video_name: str):
        """电视剧洗版：解析季号与集号，用于洗版判重与“是否已看完”判断。

        季/集直接取自 MoviePilot 的识别结果（MetaInfo 已用 MetaVideo 解析并套用自定义
        识别词），不再自行用正则解析文件名，实际下载后的重命名仍由 MP 完成。
        """
        # 取 MP 识别（MetaVideo 解析）出的季/集
        season = meta.begin_season if meta.begin_season is not None else m.season
        episode = meta.begin_episode
        if season is None:
            season = 1
        try:
            season = int(season)
        except (ValueError, TypeError):
            season = 1
        try:
            episode = int(episode) if episode is not None else None
        except (ValueError, TypeError):
            episode = None
        self._rss_log("季集解析", getattr(m, "title", ""),
                      f"MP识别 S{season:02d}" + (f"E{episode:02d}" if episode is not None else "（无集号）"))
        return season, episode

    def _rss_rule_pass(self, item: dict, m=None) -> bool:
        """按选中的优先级规则组过滤 RSS 资源；未选择规则组时直接通过。"""
        group = (self._rss_rule_group or "").strip()
        if not group:
            return True
        try:
            ti = TorrentInfo(title=item.get("title", ""), description=item.get("description", "") or "",
                             enclosure=item.get("enclosure", "") or item.get("link", ""),
                             page_url=item.get("link", ""), size=item.get("size", 0))
            kept = self._get_chain().filter_torrents(rule_groups=[group], torrent_list=[ti], mediainfo=m)
            if not kept:
                self._rss_log("规则组过滤跳过", item.get("title", ""), f"不符合优先级规则组「{group}」")
                return False
            return True
        except Exception as e:
            logger.warning(f"SC-RSS 优先级规则组过滤失败（{group}）: {e}，本条不过滤")
            return True

    def _rss_dl_add(self, item: dict, m, meta: MetaInfo) -> bool:
        try:
            enc = item.get("enclosure", "") or item.get("link", "")
            if not enc:
                return False
            if not self._rss_rule_pass(item, m):
                return False
            ti = TorrentInfo(title=item.get("title", ""), description="",
                             enclosure=enc, page_url=item.get("link", ""), size=item.get("size", 0))
            ctx = Context(meta_info=meta, media_info=m, torrent_info=ti)

            def _do_download(content: Optional[bytes] = None) -> Tuple[Optional[str], Optional[str]]:
                result = DownloadChain().download_single(
                    context=ctx, torrent_content=content,
                    downloader=self._rss_dl or None,
                    save_path=self._rss_save_path or None,
                    username="SC-RSS", return_detail=True)
                if isinstance(result, tuple):
                    return result
                return result, None

            h, err = _do_download()
            if h:
                return True
            # 代理重试：自行用系统代理下载种子内容后再交给下载链
            if self._rss_proxy_retry and not enc.lower().startswith("magnet:"):
                if not settings.PROXY:
                    logger.warning(f"SC-RSS 下载失败: {m.title} {err}，未配置代理服务器，无法重试")
                    return False
                logger.info(f"SC-RSS 下载失败: {m.title} {err}，使用代理服务器重试")
                content, perr = self._rss_http_torrent(enc, proxies=settings.PROXY)
                if content is None:
                    logger.warning(f"SC-RSS 代理重试下载种子文件失败: {m.title} {perr}")
                    return False
                h2, err2 = _do_download(content)
                if h2:
                    logger.info(f"SC-RSS 代理重试下载成功: {m.title}")
                    return True
                logger.warning(f"SC-RSS 代理重试下载仍失败: {m.title} {err2}")
                return False
            if err:
                logger.warning(f"SC-RSS 下载失败: {m.title} {err}")
            return False
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
            if not self._rss_rule_pass(item):
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
                logger.warning("SC-RSS 未找到可用下载器，无法直接添加种子")
                return False
            downloader = svc.instance
            content = enc
            # 非磁链：先下载 .torrent 文件内容
            if not enc.lower().startswith("magnet:"):
                content = self._rss_fetch_torrent(enc, tag=f"（{item.get('title', '')}）")
                if not content:
                    return False
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
        """RSS 处理过程日志。下载历史不持久化，但过程写入 MoviePilot 运行日志，
        便于排查识别与洗版判定。格式：[SC-RSS] 动作 | 标题 | 详情。"""
        try:
            msg = f"[SC-RSS] {a}"
            if title:
                msg += f" | {title}"
            if r:
                msg += f" | {r}"
            logger.info(msg)
        except Exception:
            pass