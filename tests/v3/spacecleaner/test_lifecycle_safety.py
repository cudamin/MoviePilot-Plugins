"""SpaceCleaner 停止代际与无身份播放事件的离线回归测试。"""

from __future__ import annotations

import copy
import threading
from types import SimpleNamespace

from app.plugins.spacecleaner import SpaceCleaner
from app.schemas.mediaserver import WebhookEventInfo


def _plugin():
    """创建仅使用内存存储、不启动任何调度或外部服务的插件。"""
    plugin = object.__new__(SpaceCleaner)
    plugin._stop_event = threading.Event()
    plugin._threads = set()
    plugin._thread_lock = threading.Lock()
    plugin._run_lock = threading.Lock()
    plugin._pb_lock = threading.Lock()
    plugin._scheduler_thread = None
    plugin._scheduler_event = None
    plugin._rss_s = plugin._rename_s = None
    plugin._running = plugin._rss_busy = plugin._rename_busy = False
    plugin._enabled = True
    plugin._rss_on = plugin._rename_on = False
    plugin._dry_run = True
    plugin._stop_wait_seconds = 0.02
    plugin._invalidate_caches = lambda: None
    stored = {}
    plugin.get_data = lambda key: copy.deepcopy(stored.get(key))
    plugin.save_data = lambda key, value: stored.__setitem__(key, copy.deepcopy(value))
    plugin._pb = []
    return plugin, stored


def test_unidentified_episode_does_not_mark_another_show_watched():
    """季集号相同不构成作品身份，不能覆盖已有剧的观看进度。"""
    plugin, stored = _plugin()
    original = [{"k": "123:S01E13", "n": "Show A", "s": 1, "e": 13, "p": 10}]
    stored["pb"] = copy.deepcopy(original)
    plugin._sync_pb_from_data()
    plugin.on_webhook(SimpleNamespace(event_data=WebhookEventInfo(
        event="PlaybackStop", media_type="Episode", item_name="Show B",
        season_id="1", episode_id="13", percentage=100,
    )))
    assert stored["pb"] == original
    assert plugin._pb == original


def test_stop_service_reports_busy_job_until_it_exits(monkeypatch):
    """尚未登记的在途回调也必须阻止停止成功和代际重载。"""
    plugin, _ = _plugin()
    started = threading.Event()
    release = threading.Event()
    monkeypatch.setattr("app.plugins.spacecleaner.time.time", _advancing_clock())

    def worker():
        """模拟已被调度器派发且尚在外部调用中的旧任务。"""
        plugin._running = True
        started.set()
        try:
            release.wait(2)
        finally:
            plugin._running = False

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert started.wait(1)
        assert plugin.stop_service() is False
        assert thread.is_alive()
    finally:
        release.set()
        thread.join(1)
    assert plugin.stop_service() is True


def _advancing_clock():
    """让旧实现的十秒墙钟等待迅速到期，不延长红阶段测试。"""
    value = [0.0]

    def read():
        """每次读取前进二十秒。"""
        value[0] += 20.0
        return value[0]

    return read


def test_init_preserves_stopped_generation_while_tracked_work_is_alive(monkeypatch):
    """监听任务停止超时后不应用新配置、不替换旧停止事件。"""
    plugin, _ = _plugin()
    started, release = threading.Event(), threading.Event()
    event = plugin._stop_event
    threads = plugin._threads
    plugin._migrate_delete_history = lambda: None
    plugin._load_api_recognize_cache = list
    plugin._load_api_recognize_success_cache = list
    plugin._load_anibt_cache = lambda: None
    plugin._load_rss_washed = lambda value: {}
    plugin._load_rss_fail = lambda value: {}
    plugin._backfill_washed_titles = lambda: None
    plugin._rss_split_urls = lambda: False
    plugin._rss_sources = list
    monkeypatch.setattr("app.plugins.spacecleaner.time.time", _advancing_clock())

    def worker():
        """代表没有 busy 标志的重命名监听任务。"""
        started.set()
        release.wait(2)

    thread = plugin._start_thread(worker, "SC-TestListener")
    try:
        assert started.wait(1)
        plugin.init_plugin({"enabled": False, "dry_run": False})
        assert plugin._stop_event is event
        assert plugin._threads is threads
        assert event.is_set()
        assert plugin._enabled is True
        assert plugin._dry_run is True
    finally:
        release.set()
        thread.join(1)
    plugin.init_plugin({"enabled": False, "dry_run": False})
    assert plugin._stop_event is not event
    assert not plugin._stop_event.is_set()
    assert plugin._enabled is False
    assert plugin._dry_run is False


def test_space_lookup_finishing_after_stop_does_not_enter_cleanup(monkeypatch):
    """阻塞空间读取恢复后必须重新检查停止，不能进入删除流程。"""
    plugin, _ = _plugin()
    plugin._min_free_percent = 10
    monkeypatch.setattr("app.plugins.spacecleaner.time.time", _advancing_clock())
    started, release = threading.Event(), threading.Event()
    cleanups = []

    def space():
        """磁盘读取在停止请求后才返回。"""
        started.set()
        release.wait(2)
        return {"free_percent": 1}

    plugin._get_cached_space_info = space
    plugin._clean_resources = lambda info: cleanups.append(info)
    thread = plugin._start_thread(plugin._check_and_clean, "SC-TestClean")
    try:
        assert started.wait(1)
        assert plugin.stop_service() is False
    finally:
        release.set()
        thread.join(1)
    assert cleanups == []
    assert plugin._running is False
    assert plugin.stop_service() is True


def test_stopped_generation_does_not_admit_new_threads():
    """停止标志设置后，新入口不能执行实际任务。"""
    plugin, _ = _plugin()
    plugin._stop_event.set()
    calls = []
    thread = plugin._start_thread(lambda: calls.append("ran"), "SC-Stopped")
    thread.join(1)
    assert calls == []


def test_delayed_scheduler_callback_cannot_use_next_generation(monkeypatch):
    """调度器停止前派发的延迟回调不能借用下一代未停止的事件。"""
    plugin, _ = _plugin()
    registered = []
    cleanups = []
    plugin._check_cron = "0 */6 * * *"
    plugin._check_and_clean = lambda: cleanups.append("clean")

    class Scheduler:
        """仅记录插件实际注册的回调，完全不运行真实调度器。"""

        def __init__(self, **kwargs):
            """接受原调度器构造配置。"""

        def add_job(self, target, trigger, **kwargs):
            """保存派发后可能延迟执行的回调。"""
            registered.append((target, kwargs))

        def start(self):
            """不创建真实服务线程。"""

        def shutdown(self, **kwargs):
            """保留已派发回调以模拟关闭时序。"""

    monkeypatch.setattr("apscheduler.schedulers.background.BackgroundScheduler", Scheduler)
    scheduler_event = threading.Event()
    scheduler_event.set()
    plugin._scheduler_loop(scheduler_event)
    assert len(registered) == 1
    plugin._stop_event.set()
    plugin._stop_event = threading.Event()
    target, kwargs = registered[0]
    result = target(*kwargs.get("args", ()), **kwargs.get("kwargs", {}))
    if isinstance(result, threading.Thread):
        result.join(1)
    assert cleanups == []


def test_manual_run_once_still_executes_when_generation_is_active():
    """只运行一次仍通过已有手动入口执行，不强制要求定时清理启用。"""
    plugin, _ = _plugin()
    plugin._enabled = False
    calls = []
    plugin._check_and_clean = lambda: calls.append("clean")
    thread = plugin._start_thread(plugin._run_now_task, "SC-ManualOnce")
    thread.join(1)
    assert calls == ["clean"]


def test_rss_stop_during_state_sync_does_not_process_sources():
    """存储读取返回时已停止，不进入 RSS 下载处理。"""
    plugin, _ = _plugin()
    plugin._rss_wash_mode = True
    plugin._rss_sources = lambda: [("https://example.invalid/feed", False)]
    plugin._sync_pb_from_data = lambda: plugin._stop_event.set()
    plugin._sync_rss_state_from_data = lambda: None
    processed = []
    plugin._rss_run_dedup = lambda sources: processed.append(sources)
    plugin._rss_run()
    assert processed == []
    assert plugin._rss_busy is False


def test_rss_state_sync_failure_releases_busy_flag():
    """读取缓存异常同样必须释放任务状态，避免永远无法重载。"""
    plugin, _ = _plugin()
    plugin._rss_sources = lambda: [("https://example.invalid/feed", False)]

    def fail():
        """模拟持久存储读取失败。"""
        raise OSError("storage unavailable")

    plugin._sync_pb_from_data = fail
    plugin._rss_run()
    assert plugin._rss_busy is False
    assert plugin.stop_service() is True


def test_listener_does_not_rename_after_blocking_recognition_stops(monkeypatch):
    """监听改名的识别调用返回后，停止必须阻止后续下载器写入。"""
    plugin, _ = _plugin()
    plugin._rename_on = True
    plugin._rename_downloader = []
    plugin._rename_skip_tagged = False
    plugin._rename_format = "{title}"
    monkeypatch.setattr("app.plugins.spacecleaner.time.time", _advancing_clock())
    writes = []
    qbc = SimpleNamespace(
        torrents_info=lambda **kwargs: [{"name": "Original", "tags": ""}],
        torrents_rename=lambda **kwargs: writes.append(kwargs),
    )
    helper = SimpleNamespace(get_services=lambda **kwargs: {
        "qb": SimpleNamespace(type="qbittorrent", instance=SimpleNamespace(qbc=qbc)),
    })
    monkeypatch.setattr("app.plugins.spacecleaner.DownloaderHelper", lambda: helper)
    started, release = threading.Event(), threading.Event()

    def recognize(name, **kwargs):
        """识别在停止之后才返回成功。"""
        started.set()
        release.wait(2)
        return SimpleNamespace(title="Recognized"), SimpleNamespace()

    plugin._recognize_media_by_name = recognize
    plugin._render_rename = lambda *args: "Recognized"
    plugin._rename_log = lambda *args: None
    thread = plugin._start_thread(plugin._rss_rename_after_add, "SC-Listener", "hash")
    try:
        assert started.wait(1)
        assert plugin.stop_service() is False
    finally:
        release.set()
        thread.join(1)
    assert writes == []
