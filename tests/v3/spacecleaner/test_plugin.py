"""SpaceCleaner v3 回归测试。

覆盖本轮修复的关键边界：合集删种、整剧/按季删除、试运行快照、
生命周期停止、Jellyfin/Plex Webhook、智能助手识别词污染。
"""
from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.plugins.spacecleaner import SpaceCleaner
from app.schemas.mediaserver import WebhookEventInfo
from app.schemas.types import MediaSource


def _plugin():
    plugin = object.__new__(SpaceCleaner)
    plugin._stop_event = threading.Event()
    plugin._threads = set()
    plugin._thread_lock = threading.Lock()
    plugin._pb = []
    plugin._pb_lock = threading.Lock()
    plugin._enabled = True
    plugin._rss_on = False
    plugin._watched_threshold = 85
    plugin._dry_run = False
    plugin._delete_other_versions = True
    plugin._notify = False
    plugin._delete_count = 1
    plugin._delete_by_target = False
    plugin._delete_by_record = False
    plugin._unit_delete_interval = 0
    plugin._all_torrents_cache = None
    plugin._all_torrents_cache_time = 0
    plugin._rss_ai_add_words = True
    return plugin


# ---------- P0：合集/跨季包不再盲删文件 ----------


def test_record_hash_never_deletes_files():
    plugin = _plugin()
    records = [{"download_hash": "h1", "src": r"C:\Downloads\Show\E01.mkv"}]
    assert plugin._collect_record_torrent_hashes(records, {}) == [("h1", False)]


def test_parent_dir_torrent_does_not_delete_files():
    plugin = _plugin()
    src = Path(r"C:\Downloads\Show\Season 1\E01.mkv")
    index = {"by_path": {str(src.parent): "h-parent"}}
    result = plugin._collect_record_torrent_hashes(
        [{"download_hash": "", "src": str(src)}], index
    )
    assert result == [("h-parent", False)]


def test_exact_file_torrent_can_delete_files():
    plugin = _plugin()
    src = Path(r"C:\Downloads\Show\Season 1\E01.mkv")
    index = {"by_path": {str(src): "h-file"}}
    result = plugin._collect_record_torrent_hashes(
        [{"download_hash": "", "src": str(src)}], index
    )
    assert result == [("h-file", True)]


def test_cross_seed_never_deletes_files():
    plugin = _plugin()
    main = SimpleNamespace(hash="main", name="Same.Name", size=100, save_path=r"D:\dl",
                           content_path=r"D:\dl\Same.Name", downloader="qb")
    cross = SimpleNamespace(hash="cross", name="Same.Name", size=100, save_path=r"D:\dl2",
                            content_path=r"D:\dl2\Same.Name", downloader="tr")
    index = plugin._build_torrent_index([main, cross])
    calls = []

    class Chain:
        def remove_torrents(self, **kwargs):
            calls.append(kwargs)
            return True

    deleted = plugin._delete_downloader_torrents(
        Chain(), "main", "Same.Name", index, delete_file=True
    )
    assert deleted == 2
    by_hash = {call["hashs"]: call["delete_file"] for call in calls}
    assert by_hash["main"] is True
    assert by_hash["cross"] is False


# ---------- P1：多季剧未看完的季不能被整剧带走 ----------


def _record(record_id, season, episode="13"):
    return SimpleNamespace(
        id=record_id,
        title="Show",
        type="电视剧",
        seasons=f"S{season:02d}",
        episodes=episode,
        src=rf"C:\Downloads\Show.S{season:02d}E{episode}.mkv",
        dest=rf"C:\Media\Show\Season {season}\Show.S{season:02d}E{episode}.mkv",
        download_hash="",
        downloader="",
        src_fileitem={},
        dest_fileitem={},
        date="2026-01-01 00:00:00",
        media_source="themoviedb",
        media_id="123",
    )


class _FakeQuery:
    def __init__(self, records):
        self._records = records
        self._offset = 0
        self._limit = None

    def filter(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def offset(self, value, *_args, **_kwargs):
        self._offset = int(value)
        return self

    def limit(self, value, *_args, **_kwargs):
        self._limit = int(value)
        return self

    def all(self):
        end = self._offset + self._limit if self._limit is not None else None
        return list(self._records[self._offset:end])


class _FakeSession:
    def __init__(self, records):
        self._records = records

    def query(self, *_args, **_kwargs):
        return _FakeQuery(self._records)

    def close(self):
        pass


def test_partial_multi_season_only_deletes_watched_season():
    plugin = _plugin()
    records = [_record(1, 1), _record(2, 2)]
    plugin._get_playback_pb = lambda: [
        {"k": "123:S02E13", "n": "Show S02E13", "s": 2, "e": 13, "p": 100,
         "t": "2026-01-02 00:00:00"},
    ]
    with patch("app.db.ScopedSession", lambda: _FakeSession(records)):
        units = plugin._collect_delete_units(log_skipped=False)
    assert len(units) == 1
    assert units[0]["season"] == 2
    assert len(units[0]["records"]) == 1


def test_all_seasons_watched_can_delete_as_show_unit():
    plugin = _plugin()
    records = [_record(1, 1), _record(2, 2)]
    plugin._get_playback_pb = lambda: [
        {"k": "123:S01E13", "n": "Show S01E13", "s": 1, "e": 13, "p": 100,
         "t": "2026-01-01 00:00:00"},
        {"k": "123:S02E13", "n": "Show S02E13", "s": 2, "e": 13, "p": 100,
         "t": "2026-01-02 00:00:00"},
    ]
    with patch("app.db.ScopedSession", lambda: _FakeSession(records)):
        units = plugin._collect_delete_units(log_skipped=False)
    assert len(units) == 1
    assert units[0]["season"] is None
    assert len(units[0]["records"]) == 2


# ---------- P1：试运行快照与停止生命周期 ----------


def test_delete_unit_uses_snapshot_dry_run():
    plugin = _plugin()
    plugin._dry_run = False  # 模拟运行中保存配置把 dry-run 关掉
    called = []
    plugin._delete_downloader_torrents = lambda *a, **k: called.append((a, k)) or 0
    unit = {"records": [], "display": "unit", "hash": "", "tmdbid": 1,
            "season": None, "is_tv": False}
    plugin._delete_unit(
        unit, chain=None, space_info={"free_gb": 1, "free_percent": 5},
        torrent_index={},
        options={"dry_run": True, "delete_other_versions": False, "notify": False,
                 "delete_count": 1, "delete_by_target": False, "unit_delete_interval": 0},
    )
    assert called == []


def test_stop_service_waits_for_tracked_thread(monkeypatch):
    plugin = _plugin()
    plugin._scheduler_thread = None
    plugin._scheduler_event = None
    plugin._rss_s = None
    plugin._rename_s = None
    plugin._running = plugin._rss_busy = plugin._rename_busy = False
    plugin._invalidate_caches = lambda: None
    started = threading.Event()

    def worker():
        started.set()
        plugin._stop_event.wait(30)

    thread = plugin._start_thread(worker, "SC-Test")
    assert started.wait(2)
    assert plugin.stop_service() is True
    assert not thread.is_alive()


# ---------- P1：Jellyfin/Plex Webhook ----------


def _webhook_plugin():
    plugin = _plugin()
    plugin._normalize_episode_display = lambda name, season, episode: name
    plugin._sync_pb_from_data = lambda: None
    plugin.save_data = lambda *args, **kwargs: None
    return plugin


def test_jellyfin_playback_stop_updates_playback_cache():
    plugin = _webhook_plugin()
    event = SimpleNamespace(event_data=WebhookEventInfo(
        event="PlaybackStop",
        media_type="Episode",
        media_source=MediaSource.TMDB,
        media_id="123",
        season_id="1",
        episode_id="2",
        percentage=100,
        item_name="Show S01E02",
    ))
    plugin.on_webhook(event)
    assert plugin._pb
    assert plugin._pb[-1]["k"] == "123:S01E02"
    assert plugin._pb[-1]["p"] == 100


def test_plex_scrobble_parses_tmdb_from_json_and_marks_watched():
    plugin = _webhook_plugin()
    event = SimpleNamespace(event_data=WebhookEventInfo(
        event="media.scrobble",
        item_type="TV",
        item_name="Show S01E02",
        season_id="1",
        episode_id="2",
        json_object={
            "Metadata": {
                "Guid": [{"id": "tmdb://123"}],
                "viewOffset": 90000,
                "duration": 100000,
            }
        },
    ))
    plugin.on_webhook(event)
    assert plugin._pb
    assert plugin._pb[-1]["k"] == "123:S01E02"
    assert plugin._pb[-1]["p"] == 100


# ---------- P1：AI 识别词安全 ----------


def test_ai_word_rejects_wildcard_and_generic_rules():
    plugin = _plugin()
    assert not plugin._rss_ai_word_format_ok(
        r"1080p.*?(\d{1,3}) => Hijack.S01E\1 {[tmdbid=999;type=tv;]}"
    )
    assert not plugin._rss_ai_word_format_ok(
        r"1080p\s+(\d{1,3}) => Hijack.S01E\1 {[tmdbid=999;type=tv;]}"
    )
    assert plugin._rss_ai_word_format_ok(
        r"My\ Show\ \-\ (\d{2,3}) => My Show.S01E\1 {[tmdbid=999;type=tv;]}"
    )


def test_ai_word_rejects_rule_matching_unrelated_title():
    plugin = _plugin()
    plugin._pb = [{"k": "999:S01E05", "n": "Another Show 05", "s": 1, "e": 5, "p": 100}]
    plugin._current_custom_words = list
    word = r"Show\ (\d{2,3}) => Hijack.S01E\1 {[tmdbid=999;type=tv;]}"
    assert not plugin._rss_ai_word_verify(
        word, "My Show 01", 1, 1, 999, True
    )
