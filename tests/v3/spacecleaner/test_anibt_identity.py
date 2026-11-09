"""AniBT 候选身份隔离及旧映射迁移的离线回归测试。"""
from __future__ import annotations

import threading
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from app.plugins import spacecleaner as plugin_module
from app.schemas.types import MediaSource, MediaType
from app.sdk.media import MediaInfo, MetaInfo

from .test_plugin import _plugin

TITLE = "[亿次研同好会&云光字幕组] FX战士久留美 FX Senshi Kurumi-chan [04][1080P][简日内嵌]"


@pytest.fixture
def recognition_plugin(monkeypatch):
    """提供空自定义词表、内存缓存和不连接外部服务的插件实例。"""
    from app.domain import metainfo
    from app.domain.meta import customization, releasegroup, words

    monkeypatch.setattr(words, "_custom_words_provider", list)
    monkeypatch.setattr(customization, "_customization_provider", list)
    monkeypatch.setattr(releasegroup, "_release_groups_provider", list)
    metainfo.clear_rust_parse_options_cache()
    plugin = _plugin()
    plugin._rss_anibt_match = True
    plugin._rss_anibt_calls = 0
    plugin._rss_anibt_packs = {}
    plugin._rss_anibt_shows = {}
    plugin._rss_tmdb_seasons = {}
    plugin._rss_tmdb_episodes = {}
    plugin._rss_busy = False
    data = {}
    plugin.get_data = lambda key: deepcopy(data.get(key))
    plugin.save_data = lambda key, value: data.__setitem__(key, deepcopy(value))
    plugin._rss_log = Mock()
    yield plugin, data
    metainfo.clear_rust_parse_options_cache()


def _anime(anime_id, bgm_id, chinese, english):
    """构造具有完整候选身份的 AniBT 返回条目。"""
    return {
        "animeId": anime_id,
        "bgmId": bgm_id,
        "anime": {
            "_id": anime_id,
            "bgmId": bgm_id,
            "title": {"chinese": chinese, "english": english},
        },
    }


def _response():
    """第二个候选才是决策命中的作品，列表顺序不得成为选择依据。"""
    return {
        "decision": {"kind": "auto", "animeId": "fx", "bgmId": 111},
        "candidates": [
            _anime("tetsujin", 222, "超电动机器人 铁人28号FX", "Tetsujin 28-go FX"),
            _anime("fx", 111, "FX战士久留美", "FX Senshi Kurumi-chan"),
        ],
    }


def _set_response(monkeypatch, response):
    """只替换网络响应，候选筛选仍执行插件原方法。"""
    client = SimpleNamespace(post_json=lambda *args, **kwargs: deepcopy(response))
    monkeypatch.setattr(plugin_module, "RequestUtils", lambda **kwargs: client)


def _set_tmdb(plugin, recognize):
    """替换外部 TMDB 查询边界，保留实际身份映射流程。"""
    plugin.chain = SimpleNamespace(modulemanager=SimpleNamespace(
        get_running_module=lambda name: SimpleNamespace(recognize_media=recognize)
    ))


def _media(tmdb_id, title):
    """构造真实媒体对象以验证最终媒体身份。"""
    return MediaInfo(media_source=MediaSource.TMDB, media_id=str(tmdb_id),
                     tmdb_id=tmdb_id, type=MediaType.TV, title=title)


def test_match_pack_only_contains_selected_anime_titles(recognition_plugin, monkeypatch):
    """其它作品即使排名更靠前，也不能混入选中作品的译名。"""
    plugin, _ = recognition_plugin
    _set_response(monkeypatch, _response())
    pack = plugin._rss_anibt_match_pack(TITLE)
    assert pack["bgmId"] == 111
    assert pack["titles"] == ["FX战士久留美", "FX Senshi Kurumi-chan"]


@pytest.mark.parametrize("kind", ["manual", "none", "unavailable"])
def test_unconfirmed_match_does_not_create_identity_cache(recognition_plugin, monkeypatch, kind):
    """需人工确认或不可用的结果交回通用识别，不能自动采用首项。"""
    plugin, _ = recognition_plugin
    response = _response()
    response["decision"]["kind"] = kind
    _set_response(monkeypatch, response)
    assert plugin._rss_anibt_match_pack(TITLE) is None
    assert plugin._rss_anibt_packs == {}


@pytest.mark.parametrize("invalid", ["missing", "duplicate", "bgm", "nested_id"])
def test_auto_match_requires_one_consistent_candidate(recognition_plugin, monkeypatch, invalid):
    """未命中唯一候选或内外身份冲突时，不得缓存猜测结果。"""
    plugin, _ = recognition_plugin
    response = _response()
    if invalid == "missing":
        response["decision"]["animeId"] = "absent"
    elif invalid == "duplicate":
        response["candidates"].append(deepcopy(response["candidates"][1]))
    elif invalid == "bgm":
        response["decision"]["bgmId"] = 333
    else:
        response["candidates"][1]["anime"]["_id"] = "different"
    _set_response(monkeypatch, response)
    assert plugin._rss_anibt_match_pack(TITLE) is None
    assert plugin._rss_anibt_packs == {}


def test_unrecognized_selected_anime_never_uses_other_candidate(recognition_plugin, monkeypatch):
    """正确作品暂未匹配 TMDB 时，不能靠另一个 FX 候选生成错误映射。"""
    plugin, _ = recognition_plugin
    _set_response(monkeypatch, _response())

    def recognize(meta, **kwargs):
        if "Tetsujin" in meta.name or "铁人" in meta.name:
            return _media(44307, "超电动机器人 铁人28号FX")
        return None

    _set_tmdb(plugin, recognize)
    pack = plugin._rss_anibt_match_pack(TITLE)
    result = plugin._rss_anibt_show(pack["bgmId"], pack["titles"])
    assert result is None
    assert plugin._rss_anibt_shows == {}


@pytest.mark.parametrize("matched_bgm", [222, None])
def test_feed_identity_is_not_replaced_when_direct_lookup_fails(recognition_plugin, monkeypatch, matched_bgm):
    """指定番剧订阅查询失败时，不能把另一个自动候选绑定到订阅身份。"""
    plugin, _ = recognition_plugin
    response = {
        "decision": {"kind": "auto", "animeId": "tetsujin", "bgmId": matched_bgm},
        "candidates": [_anime("tetsujin", matched_bgm, "铁人28号FX", "Tetsujin 28-go FX")],
    }
    client = SimpleNamespace(get_json=lambda *args: {"byWeekday": []},
                             post_json=lambda *args, **kwargs: deepcopy(response))
    monkeypatch.setattr(plugin_module, "RequestUtils", lambda **kwargs: client)
    plugin._rss_pick_tmdb_season = lambda *args: None
    _set_tmdb(plugin, lambda **kwargs: _media(44307, "铁人28号FX"))
    result = plugin._rss_id_anibt({"title": TITLE}, TITLE, {"bgmId": 111})
    assert result == (None, None, "")
    assert plugin._rss_anibt_shows == {}


def test_explicit_tmdb_hint_replaces_conflicting_mapping(recognition_plugin):
    """描述中的明确媒体 ID 不能被过期 bgm 映射覆盖。"""
    plugin, _ = recognition_plugin
    plugin._rss_anibt_shows = {"bgm:111": {"tmdb_id": 44307, "title": "铁人28号FX"}}
    _set_tmdb(plugin, lambda **kwargs: _media(311842, "FX战士久留美"))
    result = plugin._rss_anibt_show(111, ["FX战士久留美"], tmdb_id=311842)
    assert result.tmdb_id == 311842
    assert plugin._rss_anibt_shows["bgm:111"]["tmdb_id"] == 311842


@pytest.mark.parametrize("returned_id", [None, 44307])
def test_failed_explicit_identity_never_reuses_conflicting_mapping(recognition_plugin, returned_id):
    """指定 ID 查询失败或返回别的作品时，应拒绝而不是沿用旧错映射。"""
    plugin, _ = recognition_plugin
    plugin._rss_anibt_shows = {"bgm:111": {"tmdb_id": 44307, "title": "铁人28号FX"}}
    _set_tmdb(plugin, lambda **kwargs: _media(returned_id, "铁人28号FX") if returned_id else None)
    assert plugin._rss_anibt_show(111, ["FX战士久留美"], tmdb_id=311842) is None
    assert "bgm:111" not in plugin._rss_anibt_shows


def test_legacy_mapping_migration_preserves_user_progress_and_tmdb_lists(recognition_plugin):
    """旧身份缓存重建不得清除播放、去重、洗版状态及直接获取的番剧资料。"""
    plugin, data = recognition_plugin
    untouched = {"pb": [{"k": "311842:S01E05", "p": 100}],
                 "rss_seen": ["fixture-release"], "rss_washed": {"311842:S01E04": "FX战士久留美"}}
    data.update(deepcopy(untouched))
    direct_pack = {"bgmId": 111, "titles": ["FX战士久留美"]}
    data["rss_anibt_cache"] = {
        "packs": {"bgm:111": direct_pack, "rt:polluted": {"titles": ["FX", "铁人"]}},
        "shows": {"bgm:111": {"tmdb_id": 44307, "title": "铁人28号FX"}},
        "seasons": {"311842": [{"season": 1}]},
        "eps": {"311842:1": [{"episode": 4, "air_date": "2026-11-01"}]},
    }
    plugin._load_anibt_cache()
    assert plugin._rss_anibt_packs == {"bgm:111": direct_pack}
    assert plugin._rss_anibt_shows == {}
    assert plugin._rss_tmdb_seasons == {"311842": [{"season": 1}]}
    assert plugin._rss_tmdb_episodes["311842:1"][0]["episode"] == 4
    assert {key: data[key] for key in untouched} == untouched
    assert data["rss_anibt_cache"]["shows"] == {}
    plugin._rss_anibt_shows["bgm:111"] = {"tmdb_id": 311842, "title": "FX战士久留美"}
    plugin._save_anibt_cache(force=True)
    plugin._load_anibt_cache()
    assert plugin._rss_anibt_shows["bgm:111"]["tmdb_id"] == 311842


@pytest.mark.parametrize("suffix", ["", ".mp4"])
def test_two_rss_sources_skip_same_old_episode_with_conflicting_anibt_cache(recognition_plugin, suffix):
    """英文缓存已看至第五集时，另一个来源的第四集不能以错误作品身份下载。"""
    plugin, _ = recognition_plugin
    plugin._api_recognize_success_cache = [{
        "key": "fxsenshikurumichan", "name": "Fx Senshi Kurumi Chan",
        "media_type": MediaType.TV.value, "title": "FX战士久留美", "year": "2026", "tmdb_id": 311842,
    }]
    plugin._api_recognize_cache = []
    plugin._api_recognize_cache_lock = threading.Lock()
    plugin._rss_anibt_packs = {"bgm:111": {"bgmId": 111, "titles": ["FX战士久留美"]}}
    plugin._rss_anibt_shows = {"bgm:111": {"tmdb_id": 44307, "title": "铁人28号FX"}}
    plugin._rss_pick_tmdb_season = lambda *args: None
    _set_tmdb(plugin, lambda **kwargs: _media(311842, "FX战士久留美"))
    plugin._rss_seen = {}
    plugin._rss_washed = {}
    plugin._rss_lk = threading.Lock()
    plugin._rss_inc = plugin._rss_exc = plugin._rss_sz = ""
    plugin._rss_ntf = False
    plugin._pb = [{"k": "311842:S01E05", "n": "FX战士久留美 S01E05", "s": 1, "e": 5, "p": 20}]
    anibt = {
        "title": TITLE + suffix, "enclosure": "https://example.invalid/anibt-release",
        "description": '<strong>FX战士久留美 / FX Senshi Kurumi-chan</strong> · 04 · v1 '
                       '<a href="https://www.themoviedb.org/tv/311842">TMDB</a>',
    }
    ordinary = {"title": TITLE, "enclosure": "https://example.invalid/ordinary-release"}
    plugin._rss_parse = lambda url: [anibt] if "bgmId" in url else [ordinary]
    submitted = []

    def submit(item, media, meta):
        submitted.append((item["title"], media.tmdb_id, meta.begin_episode))
        return True

    plugin._rss_dl_add = submit
    plugin._rss_run_dedup([("https://example.invalid/anime?bgmId=111", True),
                           ("https://example.invalid/ordinary", False)])
    assert MetaInfo(title=TITLE + suffix).name == "Fx Senshi Kurumi Chan"
    assert submitted == []
    assert plugin._rss_washed == {}
    assert plugin._rss_anibt_shows["bgm:111"]["tmdb_id"] == 311842
