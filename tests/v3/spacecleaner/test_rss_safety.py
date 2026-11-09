"""RSS 规则选择、取消及 TMDB 动态清单缓存的离线回归测试。"""
from __future__ import annotations

import threading
from copy import deepcopy
from types import SimpleNamespace

import pytest
from app.plugins import spacecleaner as plugin_module
from app.schemas.types import MediaSource, MediaType
from app.sdk.media import MediaInfo, MetaInfo

from .test_plugin import _plugin


@pytest.fixture
def rss_plugin(monkeypatch):
    """隔离数据、时间和网络边界，保留真实 RSS 去重及下载协调逻辑。"""
    from app.domain import metainfo
    from app.domain.meta import customization, releasegroup, words

    monkeypatch.setattr(words, "_custom_words_provider", list)
    monkeypatch.setattr(customization, "_customization_provider", list)
    monkeypatch.setattr(releasegroup, "_release_groups_provider", list)
    metainfo.clear_rust_parse_options_cache()
    monkeypatch.setattr(plugin_module, "settings", SimpleNamespace(PROXY=None))
    plugin = _plugin()
    plugin._rss_lk = threading.Lock()
    plugin._rss_seen = {}
    plugin._rss_washed = {}
    plugin._rss_fail = {}
    plugin._rss_inc = plugin._rss_exc = plugin._rss_sz = ""
    plugin._rss_ntf = plugin._rss_ai_identify = plugin._rss_proxy_retry = False
    plugin._rss_wash_mode = True
    plugin._rss_rule_group = "1080p-only"
    plugin._rss_dl = plugin._rss_save_path = ""
    plugin._rss_busy = False
    plugin._rss_anibt_packs = {}
    plugin._rss_anibt_shows = {}
    plugin._rss_tmdb_seasons = {}
    plugin._rss_tmdb_episodes = {}
    plugin._rss_tmdb_seasons_at = {}
    plugin._rss_tmdb_episodes_at = {}
    data = {}
    plugin.get_data = lambda key: deepcopy(data.get(key))
    plugin.save_data = lambda key, value: data.__setitem__(key, deepcopy(value))
    clock = [1000.0]
    monkeypatch.setattr(plugin_module.time, "time", lambda: clock[0])
    media = MediaInfo(media_source=MediaSource.TMDB, media_id="123", tmdb_id=123,
                      type=MediaType.TV, title="Show")
    plugin._rss_id = lambda item, title: (media, MetaInfo("Show.S01E01.1080p"), title)
    plugin._rss_log = lambda *args: None
    plugin._rss_schedule_rename = lambda *args, **kwargs: None
    yield plugin, data, clock
    metainfo.clear_rust_parse_options_cache()


def _mock_download(monkeypatch, outcomes):
    """替换下载器提交边界，记录实际被提交的 RSS 资源。"""
    submitted = []

    def download_single(**kwargs):
        submitted.append(kwargs["context"].torrent_info.title)
        return next(outcomes)

    monkeypatch.setattr(plugin_module, "DownloadChain", lambda: SimpleNamespace(download_single=download_single))
    return submitted


def _rule_chain(plugin, before_return=lambda: None):
    """模拟只允许 1080p 的规则组，拒绝的候选不得进入下载选择。"""
    def filter_torrents(rule_groups, torrent_list, mediainfo=None):
        before_return()
        return [torrent for torrent in torrent_list if "1080p" in torrent.title]

    plugin._get_chain = lambda: SimpleNamespace(filter_torrents=filter_torrents)


@pytest.mark.parametrize("reverse", [False, True])
def test_rule_filter_precedes_episode_version_selection(rss_plugin, monkeypatch, reverse):
    """更早发布的违规版本不能挤掉合格版本，也不应记成下载失败。"""
    plugin, _, _ = rss_plugin
    items = [
        {"title": "Show.S01E01.720p", "enclosure": "magnet:720p", "pubdate": 1},
        {"title": "Show.S01E01.1080p", "enclosure": "magnet:1080p", "pubdate": 2},
    ]
    plugin._rss_parse = lambda url: list(reversed(items)) if reverse else items
    _rule_chain(plugin)
    submitted = _mock_download(monkeypatch, iter([("mock-hash", None)]))
    plugin._rss_run_dedup([("mock://feed", False)])
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == ["Show.S01E01.1080p"]
    assert plugin._rss_washed == {"123:S01E01": "Show"}
    assert plugin._rss_fail == {}


def test_real_download_failure_remains_retryable(rss_plugin, monkeypatch):
    """通过规则但提交失败的资源继续按原有失败次数机制重试。"""
    plugin, _, _ = rss_plugin
    plugin._rss_parse = lambda url: [{"title": "Show.S01E01.1080p", "enclosure": "magnet:allowed"}]
    _rule_chain(plugin)
    submitted = _mock_download(monkeypatch, iter([(None, "mock offline"), ("mock-hash", None)]))
    plugin._rss_run_dedup([("mock://feed", False)])
    assert plugin._rss_fail == {"magnet:allowed": 1}
    assert "magnet:allowed" not in plugin._rss_seen
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == ["Show.S01E01.1080p", "Show.S01E01.1080p"]
    assert plugin._rss_fail == {}
    assert plugin._rss_washed == {"123:S01E01": "Show"}


def test_stop_during_collection_prevents_download(rss_plugin, monkeypatch):
    """采集阶段停止后，已经收集的候选也不能再进入统一下载循环。"""
    plugin, _, _ = rss_plugin
    plugin._rss_parse = lambda url: [{"title": "Show.S01E01.1080p", "enclosure": "magnet:allowed"}]
    recognize = plugin._rss_id

    def stop_after_recognize(item, title):
        result = recognize(item, title)
        plugin._stop_event.set()
        return result

    plugin._rss_id = stop_after_recognize
    _rule_chain(plugin)
    submitted = _mock_download(monkeypatch, iter([("mock-hash", None)]))
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == []
    assert plugin._rss_fail == {}
    assert plugin._rss_washed == {}
    assert plugin._rss_seen == {}
    plugin._stop_event.clear()
    plugin._rss_id = recognize
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == ["Show.S01E01.1080p"]


def test_stop_during_rule_check_does_not_submit_or_count_failure(rss_plugin, monkeypatch):
    """规则检查阻塞期间停止时，返回后不得提交下载或消耗失败次数。"""
    plugin, _, _ = rss_plugin
    plugin._rss_parse = lambda url: [{"title": "Show.S01E01.1080p", "enclosure": "magnet:allowed"}]
    _rule_chain(plugin, plugin._stop_event.set)
    submitted = _mock_download(monkeypatch, iter([("mock-hash", None)]))
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == []
    assert plugin._rss_fail == {}


def test_stop_after_multiple_candidates_preserves_retry_for_all(rss_plugin, monkeypatch):
    """取消后本轮未提交的所有候选恢复去重标记，已提交的资源仍保留。"""
    plugin, _, _ = rss_plugin
    items = [
        {"title": "Show.S01E01.1080p", "enclosure": "magnet:first"},
        {"title": "Show.S01E02.1080p", "enclosure": "magnet:second"},
    ]
    plugin._rss_parse = lambda url: items
    plugin._rss_id = lambda item, title: (
        MediaInfo(media_source=MediaSource.TMDB, media_id="123", tmdb_id=123,
                  type=MediaType.TV, title="Show"), MetaInfo(title), title,
    )
    _rule_chain(plugin)
    submitted = []

    def download_single(**kwargs):
        submitted.append(kwargs["context"].torrent_info.title)
        if len(submitted) == 1:
            plugin._stop_event.set()
        return "mock-hash", None

    monkeypatch.setattr(plugin_module, "DownloadChain", lambda: SimpleNamespace(download_single=download_single))
    plugin._rss_run_dedup([("mock://feed", False)])
    assert plugin._rss_seen == {"magnet:first": None}
    plugin._stop_event.clear()
    plugin._rss_run_dedup([("mock://feed", False)])
    assert submitted == ["Show.S01E01.1080p", "Show.S01E02.1080p"]


def test_stop_during_proxy_fetch_does_not_submit(rss_plugin, monkeypatch):
    """代理获取种子期间停止时，不再提交已经返回的种子或发起直连重试。"""
    plugin, _, _ = rss_plugin
    plugin._rss_proxy_retry = True
    monkeypatch.setattr(plugin_module.settings, "PROXY", {"https": "http://mock-proxy"})
    _rule_chain(plugin)

    def fetch(*args, **kwargs):
        plugin._stop_event.set()
        return b"mock-torrent", ""

    plugin._rss_http_torrent = fetch
    submitted = _mock_download(monkeypatch, iter([("mock-hash", None)]))
    item = {"title": "Show.S01E01.1080p", "enclosure": "https://fixture.invalid/torrent"}
    media, meta, _ = plugin._rss_id(item, item["title"])
    assert plugin._rss_dl_add(item, media, meta) is False
    assert submitted == []


def test_plain_rss_stop_during_parse_does_not_add(rss_plugin):
    """普通 RSS 拉取阻塞期间停止时，返回报文也不能开始下载。"""
    plugin, _, _ = rss_plugin
    submitted = []

    def parse(url):
        plugin._stop_event.set()
        return [{"title": "Show.S01E01.1080p", "enclosure": "magnet:allowed"}]

    plugin._rss_parse = parse
    plugin._rss_add_direct = lambda item: submitted.append(item["title"]) or True
    plugin._rss_proc("mock://feed")
    assert submitted == []
    assert plugin._rss_fail == {}


def test_plain_rss_cancellation_is_not_download_failure(rss_plugin):
    """取消当前提交后停止后续条目，不写入下载失败次数。"""
    plugin, _, _ = rss_plugin
    plugin._rss_parse = lambda url: [
        {"title": "Show.S01E01.1080p", "enclosure": "magnet:first"},
        {"title": "Show.S01E02.1080p", "enclosure": "magnet:second"},
    ]
    attempted = []

    def cancel_add(item):
        attempted.append(item["title"])
        plugin._stop_event.set()
        return False

    plugin._rss_add_direct = cancel_add
    plugin._rss_proc("mock://feed")
    assert attempted == ["Show.S01E01.1080p"]
    assert plugin._rss_fail == {}


def test_direct_add_stop_during_fetch_does_not_submit(rss_plugin, monkeypatch):
    """普通模式获取种子文件阻塞后复查停止，避免真正添加下载任务。"""
    plugin, _, _ = rss_plugin
    _rule_chain(plugin)
    submitted = []
    instance = SimpleNamespace(add_torrent=lambda **kwargs: submitted.append(kwargs) or (True, ["mock-hash"]))
    helper = SimpleNamespace(get_service=lambda **kwargs: SimpleNamespace(instance=instance))
    plugin._rss_dl = "mock-qb"
    monkeypatch.setattr(plugin_module, "DownloaderHelper", lambda: helper)

    def fetch(*args, **kwargs):
        plugin._stop_event.set()
        return b"mock-torrent"

    plugin._rss_fetch_torrent = fetch
    assert plugin._rss_add_direct({"title": "Show.1080p", "enclosure": "https://fixture.invalid/torrent"}) is False
    assert submitted == []


@pytest.fixture(params=["seasons", "episodes"])
def tmdb_list_case(request):
    """同一组恢复行为分别覆盖整剧季清单和单季集清单。"""
    if request.param == "seasons":
        return SimpleNamespace(
            field="seasons", storage="seasons", stamp="seasons_at", key="123",
            query=lambda plugin: plugin._rss_tmdb_show_seasons(123),
            method="get_info", first={"season_number": 1, "air_date": "2025-01-01", "episode_count": 12},
            new={"season_number": 2, "air_date": "2026-11-01", "episode_count": 12},
            expected={"season": 2, "air_date": "2026-11-01", "episodes": 12},
        )
    return SimpleNamespace(
        field="episodes", storage="eps", stamp="eps_at", key="123:1",
        query=lambda plugin: plugin._rss_tmdb_season_episodes(123, 1),
        method="get_tv_season_detail", first={"episode_number": 1, "air_date": "2025-01-01"},
        new={"episode_number": 2, "air_date": "2026-11-01"},
        expected={"episode": 2, "air_date": "2026-11-01"},
    )


def _tmdb_response(plugin, case, responses):
    """控制 TMDB 返回值或异常，不连接真实 TMDB 服务。"""
    def query(*args):
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return deepcopy(response)

    plugin._rss_tmdb_api = lambda: SimpleNamespace(**{case.method: query})


def test_nonempty_tmdb_lists_refresh_after_six_hours(rss_plugin, tmdb_list_case):
    """已缓存的清单过期后能发现新季或新集。"""
    plugin, _, clock = rss_plugin
    case = tmdb_list_case
    _tmdb_response(plugin, case, iter([{case.field: [case.first]}, {case.field: [case.new]}]))
    first = case.query(plugin)
    clock[0] += 21599
    assert case.query(plugin) == first
    clock[0] += 1
    assert case.query(plugin) == [case.expected]


def test_empty_tmdb_lists_retry_after_five_minutes(rss_plugin, tmdb_list_case):
    """合法空清单只短期缓存，不能把未播出结果永久固化。"""
    plugin, _, clock = rss_plugin
    case = tmdb_list_case
    _tmdb_response(plugin, case, iter([{case.field: []}, {case.field: [case.new]}]))
    assert case.query(plugin) == []
    clock[0] += 299
    assert case.query(plugin) == []
    clock[0] += 1
    assert case.query(plugin) == [case.expected]


@pytest.mark.parametrize("failure", [OSError("mock outage"), None])
def test_failed_tmdb_lists_retry_without_waiting(rss_plugin, tmdb_list_case, failure):
    """异常和无响应不缓存，服务恢复后的下一次读取立即重试。"""
    plugin, _, _ = rss_plugin
    case = tmdb_list_case
    _tmdb_response(plugin, case, iter([failure, {case.field: [case.new]}]))
    assert case.query(plugin) == []
    assert case.query(plugin) == [case.expected]


def test_missing_tmdb_module_does_not_create_empty_cache(rss_plugin, tmdb_list_case):
    """模块暂未运行不能生成持久化空清单。"""
    plugin, _, _ = rss_plugin
    case = tmdb_list_case
    plugin._rss_tmdb_api = lambda: None
    assert case.query(plugin) == []
    _tmdb_response(plugin, case, iter([{case.field: [case.new]}]))
    assert case.query(plugin) == [case.expected]


def test_tmdb_timestamps_survive_reload_without_resetting_age(rss_plugin, tmdb_list_case):
    """重新载入持久化缓存不能重新计算有效期或丢掉剩余有效时间。"""
    plugin, data, clock = rss_plugin
    case = tmdb_list_case
    _tmdb_response(plugin, case, iter([{case.field: [case.first]}, {case.field: [case.new]}]))
    first = case.query(plugin)
    assert data["rss_anibt_cache"][case.stamp][case.key] == 1000.0
    assert isinstance(data["rss_anibt_cache"][case.storage][case.key], list)
    clock[0] += 21599
    plugin._load_anibt_cache()
    assert case.query(plugin) == first
    clock[0] += 1
    assert case.query(plugin) == [case.expected]


def test_legacy_tmdb_lists_without_timestamp_refresh(rss_plugin, tmdb_list_case):
    """旧列表形状仍保留，但没有时间戳的条目首次读取即重新请求。"""
    plugin, data, _ = rss_plugin
    case = tmdb_list_case
    user_state = {"pb": [{"k": "123:S01E01", "p": 100}], "rss_seen": ["known"],
                  "rss_washed": {"123:S01E01": "Show"}}
    data.update(deepcopy(user_state))
    data["rss_anibt_cache"] = {"identity_version": plugin._ANIBT_IDENTITY_CACHE_VERSION,
                               case.storage: {case.key: []}}
    plugin._load_anibt_cache()
    _tmdb_response(plugin, case, iter([{case.field: [case.new]}]))
    assert case.query(plugin) == [case.expected]
    assert {key: data[key] for key in user_state} == user_state
