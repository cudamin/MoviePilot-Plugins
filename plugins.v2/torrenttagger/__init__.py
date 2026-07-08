import threading
from typing import Any, Dict, List, Optional, Tuple

from app.chain import ChainBase
from app.core.meta.metavideo import MetaVideo
from app.helper.service import ServiceConfigHelper
from app.log import logger
from app.plugins import _PluginBase


class TorrentTagger(_PluginBase):
    """种子标签器插件。"""

    plugin_name = "种子标签器"
    plugin_desc = "扫描下载器种子，识别媒体信息并添加 tmdbid 标签。"
    plugin_icon = "label.png"
    plugin_version = "1.2.0"
    plugin_label = "系统工具"
    plugin_author = "tafei"
    author_url = "https://github.com/cudamin/MoviePilot-Plugins"
    plugin_config_prefix = "torrenttagger_"
    plugin_order = 11
    auth_level = 1

    _enabled = False
    _downloaders: List[str] = []
    _scan_interval = 24
    _run_now = False
    _scheduler_thread: Optional[threading.Thread] = None
    _scheduler_running = False
    _scheduler_event: Optional[threading.Event] = None

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._load_config(config)
        if self._enabled:
            self._start_scheduler()
        if self._run_now:
            self._run_now = False
            self.__update_config()
            self._start_scan_thread(name="TorrentTagger-RunNow")

    def _load_config(self, config: dict = None) -> None:
        """加载插件配置。"""
        self._enabled = False
        self._downloaders = []
        self._scan_interval = 24
        self._run_now = False
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        raw_downloaders = config.get("downloaders") or []
        if isinstance(raw_downloaders, list):
            self._downloaders = [str(item) for item in raw_downloaders if item]
        elif isinstance(raw_downloaders, str) and raw_downloaders:
            self._downloaders = [raw_downloaders]
        self._scan_interval = max(int(config.get("scan_interval") or 24), 1)
        self._run_now = bool(config.get("run_now"))

    def __update_config(self) -> None:
        """更新插件配置。"""
        self.update_config({
            "enabled": self._enabled,
            "downloaders": self._downloaders,
            "scan_interval": self._scan_interval,
            "run_now": self._run_now,
        })

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {
                "path": "/scan_now",
                "endpoint": self.api_scan_now,
                "methods": ["GET"],
                "summary": "立即扫描",
                "description": "立即扫描一次下载器种子并添加标签",
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        downloader_options = self._get_downloader_options()
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "run_now",
                                            "label": "立即运行",
                                            "hint": "开启后保存配置将立即执行一次扫描",
                                            "persistent-hint": True,
                                            "color": "primary",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "scan_interval",
                                            "label": "扫描间隔（小时）",
                                            "type": "number",
                                            "min": 1,
                                            "hint": "每隔多少小时扫描一次",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            }
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "downloaders",
                                            "label": "下载器",
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "hint": "勾选要扫描的下载器，不选则扫描所有下载器",
                                            "persistent-hint": True,
                                            "items": downloader_options,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "downloaders": [],
            "scan_interval": 24,
            "run_now": False,
        }

    @staticmethod
    def _get_downloader_options() -> List[Dict[str, str]]:
        """获取下载器选项。"""
        downloader_confs = ServiceConfigHelper.get_downloader_configs() or []
        return [
            {"title": item.name or "", "value": item.name or ""}
            for item in downloader_confs
            if item and item.name
        ]

    def get_page(self):
        """返回插件详情页面。"""
        pass

    def stop_service(self) -> None:
        """停止插件后台服务。"""
        self._scheduler_running = False
        if self._scheduler_event:
            self._scheduler_event.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=3)
        self._scheduler_thread = None
        self._scheduler_event = None

    def _start_scheduler(self) -> None:
        """启动后台扫描线程。"""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            return
        self._scheduler_running = True
        self._scheduler_event = threading.Event()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="TorrentTagger",
        )
        self._scheduler_thread.start()

    def _scheduler_loop(self) -> None:
        """执行后台扫描循环。"""
        if not self._scheduler_event:
            return
        interval_seconds = max(self._scan_interval, 1) * 3600
        self._scheduler_event.wait(interval_seconds)
        while self._scheduler_running:
            try:
                self._scan_and_tag()
            except Exception as err:
                logger.error(f"种子标签扫描异常: {err}")
            self._scheduler_event.clear()
            if self._scheduler_event.wait(interval_seconds):
                break

    def _start_scan_thread(self, name: str) -> None:
        """启动一次性扫描线程。"""
        threading.Thread(target=self._scan_and_tag, daemon=True, name=name).start()

    def _scan_and_tag(self) -> None:
        """扫描下载器种子并添加 tmdbid 标签。"""
        chain = ChainBase()
        torrents = self._list_torrents(chain)
        if not torrents:
            logger.info("未找到任何种子")
            return
        title_groups = self._group_untagged_torrents(torrents)
        if not title_groups:
            logger.info("所有种子已有 tmdbid 标签，无需处理")
            return
        logger.info(f"待识别标题数: {len(title_groups)}，待处理种子数: {sum(len(v) for v in title_groups.values())}")
        tagged, skipped = self._tag_title_groups(chain, title_groups)
        logger.info(f"扫描完成: 已标记 {tagged} 个种子，跳过 {skipped} 个")

    def _list_torrents(self, chain: ChainBase) -> List[Any]:
        """列出需要扫描的种子。"""
        downloaders = self._downloaders or []
        logger.info(f"开始扫描下载器种子，下载器: {downloaders or '全部'}")
        all_torrents: List[Any] = []
        if not downloaders:
            torrents = chain.list_torrents(downloader=None, include_all_tags=True)
            return list(torrents or [])
        for downloader in downloaders:
            torrents = chain.list_torrents(downloader=downloader, include_all_tags=True)
            if torrents:
                all_torrents.extend(torrents)
        return all_torrents

    @staticmethod
    def _group_untagged_torrents(torrents: List[Any]) -> Dict[str, List[Tuple[str, Optional[str]]]]:
        """按标题归并尚未添加 tmdbid 标签的种子。"""
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
        """识别标题分组并写入 tmdbid 标签。"""
        tagged = 0
        skipped = 0
        for title, hash_list in title_groups.items():
            tmdb_id = self._recognize_tmdb_id(chain, title)
            if not tmdb_id:
                skipped += len(hash_list)
                continue
            tmdb_tag = f"tmdbid={tmdb_id}"
            for torrent_hash, downloader in hash_list:
                try:
                    chain.set_torrents_tag(hashs=torrent_hash, tags=[tmdb_tag], downloader=downloader)
                    tagged += 1
                except Exception as err:
                    logger.error(f"为种子 {title} (hash={torrent_hash}) 打标签失败: {err}")
            logger.info(f"已为 {len(hash_list)} 个同名种子添加标签: {title} -> {tmdb_tag}")
        return tagged, skipped

    @staticmethod
    def _recognize_tmdb_id(chain: ChainBase, title: str) -> Optional[int]:
        """识别标题对应的 TMDB ID。"""
        try:
            meta = MetaVideo(title=title)
            if not meta.name:
                logger.debug(f"未能从标题解析出媒体名称: {title}")
                return None
            mediainfo = chain.recognize_media(meta=meta, cache=True)
            if not mediainfo or not mediainfo.tmdb_id:
                logger.debug(f"未能识别种子: {title}")
                return None
            return int(mediainfo.tmdb_id)
        except Exception as err:
            logger.error(f"处理种子失败: {title}, 错误: {err}")
            return None

    def api_scan_now(self) -> Dict[str, Any]:
        """API：立即扫描。"""
        try:
            self._start_scan_thread(name="TorrentTagger-ScanNow")
            return {"success": True, "message": "扫描已启动"}
        except Exception as err:
            return {"success": False, "message": str(err)}
