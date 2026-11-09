"""
jackettBridge 插件关键路径回归：配置恢复、站点同步、解析容错与检索时限。
"""

from __future__ import annotations

import hashlib
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import jackettbridge as module
from jackettbridge import JackettBridge
from app.schemas.types import EventType


class _FakeSites:
    """
    站点索引助手替身。
    """

    def __init__(self, auth_level: int = 1):
        self.auth_level = auth_level
        self.indexers = {}
        self.add_calls = []

    def get_indexer(self, domain):
        return self.indexers.get(domain)

    def add_indexer(self, domain, indexer):
        self.add_calls.append((domain, indexer))
        self.indexers[domain] = indexer


class _FakeSiteOper:
    """
    站点仓储替身。
    """

    def __init__(self):
        self.sites = {}
        self.next_id = 1
        self.deleted = []

    def list_order_by_pri(self):
        return list(self.sites.values())

    def get_by_domain(self, domain):
        return self.sites.get(domain)

    def add(self, **payload):
        site = SimpleNamespace(id=self.next_id, **payload)
        self.next_id += 1
        self.sites[payload["domain"]] = site
        return True, "ok"

    def update(self, site_id, payload):
        for site in self.sites.values():
            if site.id == site_id:
                for key, value in payload.items():
                    setattr(site, key, value)
                return site
        return None

    def delete(self, site_id):
        self.deleted.append(site_id)
        for domain, site in list(self.sites.items()):
            if site.id == site_id:
                del self.sites[domain]


class _FakeEvents:
    """
    事件管理器替身。
    """

    def __init__(self):
        self.events = []

    def send_event(self, event_type, payload):
        self.events.append((event_type, payload))


def _indexer(indexer_id="aither-api", domain="jackett-aither-api.extend"):
    return {
        "id": f"jackettBridge-{indexer_id}",
        "name": f"jackettBridge-Aither ({indexer_id})",
        "origin_name": "Aither",
        "indexer_id": indexer_id,
        "domain": domain,
        "url": f"http://jackett.local/api/v2.0/indexers/{indexer_id}/results/torznab/",
        "rss": f"http://jackett.local/api/v2.0/indexers/{indexer_id}/results/torznab/api?apikey=key",
        "public": False,
        "privacy": "private",
        "proxy": False,
        "result_num": 100,
        "timeout": 5,
        "parser": "jackettBridge",
        "plugin": "jackettBridge",
        "category": {"movie": [2000], "tv": [5000], "music": [3000]},
        "search": {},
    }




def _make_init_plugin(monkeypatch, plugin, data, config):
    """
    为 init_plugin 装配最小宿主依赖。
    """
    sites = _FakeSites()
    site_oper = _FakeSiteOper()
    monkeypatch.setattr(module, "SitesHelper", lambda: sites)
    monkeypatch.setattr(module, "SiteOper", lambda: site_oper)
    plugin.stop_service = lambda: None
    plugin.get_data = lambda key: data.get(key)
    plugin.get_config = lambda: {}
    updates = []
    plugin.update_config = lambda value: updates.append(value)
    cleanup = Mock()
    start = Mock()
    setattr(plugin, "_JackettBridge__cleanup_managed_sites", cleanup)
    setattr(plugin, "_JackettBridge__start_sync_thread", start)
    plugin.init_plugin(config)
    return updates, cleanup, start


def test_onlyonce_while_disabled_refreshes_catalog_without_sites(monkeypatch, plugin):
    """关闭状态下的立即同步只刷新目录，不应注册虚拟站点。"""
    updates, cleanup, start = _make_init_plugin(
        monkeypatch,
        plugin,
        {"indexers": []},
        {"enabled": False, "onlyonce": True, "host": "jackett.local:9117", "api_key": "key"},
    )

    start.assert_called_once_with(register_sites=False)
    cleanup.assert_called_once()
    assert plugin._onlyonce is False
    assert updates and updates[0]["onlyonce"] is False


def test_onlyonce_while_enabled_registers_sites(monkeypatch, plugin):
    """开启状态下的立即同步仍应完整注册虚拟站点。"""
    _, cleanup, start = _make_init_plugin(
        monkeypatch,
        plugin,
        {"indexers": []},
        {"enabled": True, "onlyonce": True, "host": "http://jackett.local:9117", "api_key": "key"},
    )

    start.assert_called_once_with(register_sites=True)
    cleanup.assert_not_called()


def _signature(host="http://jackett.local:9117", api_key="key", selected=None, result_num=100):
    return {
        "plugin_version": JackettBridge.plugin_version,
        "host": host,
        "api_key_hash": hashlib.sha256(api_key.encode("utf-8")).hexdigest(),
        "selected_indexers": sorted(selected or []),
        "result_num": result_num,
    }


def test_restore_only_when_signature_matches(monkeypatch, plugin):
    """地址、Key 和选择未变化时只恢复本地快照。"""
    config = {
        "enabled": True,
        "host": "http://jackett.local:9117",
        "api_key": "key",
        "result_num": 100,
        "selected_indexers": ["aither-api"],
    }
    data = {
        "indexers": [_indexer()],
        "synced_plugin_version": JackettBridge.plugin_version,
        "sync_signature": _signature(selected=["aither-api"]),
    }

    _, _, start = _make_init_plugin(monkeypatch, plugin, data, config)

    start.assert_called_once_with(restore_only=True)


def test_full_sync_when_selection_changes(monkeypatch, plugin):
    """选择变化必须触发完整同步，而不是继续使用旧快照。"""
    config = {
        "enabled": True,
        "host": "http://jackett.local:9117",
        "api_key": "key",
        "result_num": 100,
        "selected_indexers": ["blutopia-api"],
    }
    data = {
        "indexers": [_indexer()],
        "synced_plugin_version": JackettBridge.plugin_version,
        "sync_signature": _signature(selected=["aither-api"]),
    }

    _, _, start = _make_init_plugin(monkeypatch, plugin, data, config)

    start.assert_called_once_with()


def test_full_sync_when_host_or_key_changes(monkeypatch, plugin):
    """Jackett 地址或 API Key 变化必须触发完整同步。"""
    config = {
        "enabled": True,
        "host": "http://new-jackett.local:9117",
        "api_key": "new-key",
        "result_num": 100,
    }
    data = {
        "indexers": [_indexer()],
        "synced_plugin_version": JackettBridge.plugin_version,
        "sync_signature": _signature(),
    }

    _, _, start = _make_init_plugin(monkeypatch, plugin, data, config)

    start.assert_called_once_with()


def test_indexer_matches_ignores_host_merged_identity(plugin):
    """宿主合并进来的 id/name/url 不应导致每次同步都判为更新。"""
    indexer = _indexer()
    registered = dict(indexer)
    registered.update({
        "id": 1,
        "name": "宿主合并后的名称",
        "url": "https://virtual.example/",
        "domain": "https://jackett-aither-api.extend/",
    })

    assert JackettBridge._JackettBridge__indexer_matches(registered, indexer)

    changed = dict(registered)
    changed["rss"] = "http://old/rss"
    assert not JackettBridge._JackettBridge__indexer_matches(changed, indexer)


def test_helper_registration_waits_for_user_auth(plugin):
    """用户认证未完成时不应触发 SitesHelper 的未认证错误。"""
    plugin.sites_helper = _FakeSites(auth_level=0)
    plugin._indexers = [_indexer()]

    registered, updated = plugin._JackettBridge__sync_helper_indexers()

    assert (registered, updated) == (0, 0)
    assert plugin.sites_helper.add_calls == []

    plugin.sites_helper = _FakeSites(auth_level=1)
    registered, updated = plugin._JackettBridge__sync_helper_indexers()

    assert (registered, updated) == (1, 0)
    assert len(plugin.sites_helper.add_calls) == 1


def test_parse_skips_bad_item_and_keeps_others(plugin, monkeypatch):
    """单条 TorrentInfo 构造失败不应丢掉同批其他资源。"""
    class FakeTorrentInfo:
        def __init__(self, **kwargs):
            if kwargs["title"] == "bad":
                raise ValueError("invalid item")
            self.__dict__.update(kwargs)

    monkeypatch.setattr(module, "TorrentInfo", FakeTorrentInfo)
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
      <channel>
        <item><title>good-1</title><enclosure url="magnet:?xt=urn:btih:1"/></item>
        <item><title>bad</title><enclosure url="magnet:?xt=urn:btih:2"/></item>
        <item><title>good-2</title><enclosure url="magnet:?xt=urn:btih:3"/></item>
      </channel>
    </rss>"""

    results = plugin._JackettBridge__parse_torznab(
        xml,
        site={"id": 1, "name": "Aither", "proxy": False},
        site_name="Aither",
    )

    assert [item.title for item in results] == ["good-1", "good-2"]


def test_site_delete_event_and_config_cleanup(plugin, monkeypatch):
    """删除虚拟站点时应发送带 site_id 的事件并清理系统站点配置。"""
    site = SimpleNamespace(
        id=10,
        domain="jackett-aither-api.extend",
        name="jackettBridge-Aither",
        url="https://jackett-aither-api.extend/",
    )
    plugin.site_oper.sites[site.domain] = site
    plugin._indexers = []
    plugin._indexers_authoritative = True

    class FakeConfig:
        def __init__(self):
            self.values = {
                module.SystemConfigKey.IndexerSites: [10, 99],
                module.SystemConfigKey.RssSites: [10],
            }

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value):
            self.values[key] = value

    config = FakeConfig()
    monkeypatch.setattr(module, "get_configured_system_config", lambda: config)

    plugin._JackettBridge__sync_site_records()

    assert plugin.site_oper.deleted == [10]
    deleted_events = [
        payload for event_type, payload in plugin.eventmanager.events
        if event_type == EventType.SiteDeleted
    ]
    assert deleted_events == [{
        "site_id": 10,
        "domain": site.domain,
        "name": site.name,
        "site_url": site.url,
    }]
    assert config.values[module.SystemConfigKey.IndexerSites] == [99]
    assert config.values[module.SystemConfigKey.RssSites] == []


def test_cleanup_managed_sites_emits_delete_events(plugin, monkeypatch):
    """关闭插件清理站点时也应发送完整删除事件并清理系统配置。"""
    site = SimpleNamespace(
        id=10,
        domain="jackett-aither-api.extend",
        name="jackettBridge-Aither",
        url="https://jackett-aither-api.extend/",
    )
    plugin.site_oper.sites[site.domain] = site

    class FakeConfig:
        def __init__(self):
            self.values = {
                module.SystemConfigKey.IndexerSites: [10],
                module.SystemConfigKey.RssSites: [10],
            }

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value):
            self.values[key] = value

    config = FakeConfig()
    monkeypatch.setattr(module, "get_configured_system_config", lambda: config)

    removed = plugin._JackettBridge__cleanup_managed_sites()

    assert removed == 1
    assert plugin.site_oper.deleted == [10]
    assert plugin.eventmanager.events == [(
        EventType.SiteDeleted,
        {
            "site_id": 10,
            "domain": site.domain,
            "name": site.name,
            "site_url": site.url,
        },
    )]


def test_stop_service_signals_and_joins_sync_thread(plugin):
    """停止插件时应通知并等待正在运行的同步线程退出。"""
    started = threading.Event()
    finished = threading.Event()

    def worker():
        started.set()
        plugin._sync_stop_event.wait(3)
        finished.set()

    thread = threading.Thread(target=worker)
    plugin._sync_thread = thread
    plugin.is_clone = True
    thread.start()
    assert started.wait(1)

    plugin.stop_service()

    assert plugin._sync_stop_event.is_set()
    assert finished.is_set()


def test_search_budget_returns_completed_results(plugin, monkeypatch):
    """总检索时限到达时应返回已完成结果，而不是等待慢索引器。"""
    release = threading.Event()
    plugin._search_budget = 1
    plugin._indexers = [
        _indexer("fast", "jackett-fast.extend"),
        _indexer("slow", "jackett-slow.extend"),
    ]
    for site_id, indexer in enumerate(plugin._indexers, 1):
        plugin.site_oper.sites[indexer["domain"]] = SimpleNamespace(id=site_id, is_active=True)
    plugin._last_sites_source = "unlimited"

    def resolve_site(indexer):
        return dict(indexer)

    def query(indexer_id, site, keyword=None, mtype=None, page=0):
        if indexer_id == "slow":
            release.wait(3)
            return []
        return [SimpleNamespace(title="fast-result")]

    setattr(plugin, "_JackettBridge__resolve_indexer_site", resolve_site)
    setattr(plugin, "_JackettBridge__query_jackett", query)

    started = time.monotonic()
    try:
        results = plugin._JackettBridge__search_bridged_indexers(keyword="test")
    finally:
        release.set()

    assert time.monotonic() - started < 2.5
    assert [item.title for item in results] == ["fast-result"]


def test_stopped_sync_thread_returns_without_writing(plugin):
    """停止信号已置位时，同步线程不应继续请求或写站点。"""
    plugin._sync_stop_event.set()
    plugin._host = "http://jackett.local:9117"
    plugin._api_key = "key"
    plugin.get_indexers = Mock(side_effect=AssertionError("不应请求 Jackett"))
    plugin.save_data = Mock()

    plugin.sync_indexers()

    plugin.get_indexers.assert_not_called()
    plugin.save_data.assert_not_called()
