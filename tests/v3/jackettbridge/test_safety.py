"""桥接实例隔离、同步代次与宿主搜索范围回归。"""

import asyncio
import copy
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import jackettbridge as module
from jackettbridge import JackettBridge
from .test_regressions import _indexer


def configure_runtime(monkeypatch, instance):
    """用内存替身提供初始化依赖，保留原同步及持久化顺序。"""
    data = {}
    config = {}
    instance.is_clone = False
    instance.systemconfig = SimpleNamespace(get=lambda key: ["JackettBridge"])
    instance.get_data = lambda key: copy.deepcopy(data.get(key))
    instance.save_data = lambda key, value: data.__setitem__(key, copy.deepcopy(value))
    instance.get_config = lambda: dict(config)
    instance.update_config = lambda value: config.update(copy.deepcopy(value))
    monkeypatch.setattr(module, "SitesHelper", lambda: instance.sites_helper)
    monkeypatch.setattr(module, "SiteOper", lambda: instance.site_oper)
    monkeypatch.setattr(module, "get_configured_system_config", lambda: SimpleNamespace(get=lambda key: [], set=lambda *args: None))
    return data, config


@pytest.mark.parametrize("reenable", [False, True])
def test_old_sync_cannot_publish_after_reconfigure(monkeypatch, plugin, reenable):
    """网络请求跨过停止等待期限后，旧响应不得改写当前配置或站点。"""
    data, _ = configure_runtime(monkeypatch, plugin)
    entered, release, fetched_new = threading.Event(), threading.Event(), threading.Event()
    old_indexer, new_indexer = _indexer("old", "jackett-old.extend"), _indexer("new", "jackett-new.extend")

    def fetch(instance):
        if instance._host == "http://new.invalid":
            fetched_new.set()
            return [new_indexer]
        entered.set()
        assert release.wait(10)
        return [old_indexer]

    monkeypatch.setattr(JackettBridge, "get_indexers", fetch)
    plugin._JackettBridge__start_sync_thread()
    assert entered.wait(2)
    old_thread = plugin._sync_thread
    real_join = old_thread.join
    monkeypatch.setattr(old_thread, "join", lambda timeout=None: real_join(0))
    try:
        plugin.init_plugin({"enabled": reenable, "host": "http://new.invalid", "api_key": "dummy"})
        if reenable:
            assert fetched_new.wait(2), "新配置同步不能被旧代锁跳过"
            plugin._sync_thread.join(2)
        release.set()
        real_join(2)
        assert not old_thread.is_alive()
        expected = {"jackett-new.extend"} if reenable else set()
        assert set(plugin.site_oper.sites) == expected
        assert [i["indexer_id"] for i in data.get("indexers", [])] == (["new"] if reenable else [])
    finally:
        release.set()
        real_join(2)


def test_disabled_clone_does_not_delete_main_site(monkeypatch, plugin):
    """宿主新建默认关闭分身时，主插件站点保持原样。"""
    configure_runtime(monkeypatch, plugin)
    original = SimpleNamespace(id=50, domain="jackett-aither-api.extend", name="Main", note={})
    plugin.site_oper.sites[original.domain] = original
    plugin.is_clone = True
    plugin.plugin_instance_id = "JackettBridgeClone1"
    plugin.plugin_name = "Second Jackett"
    plugin.init_plugin({"enabled": False, "host": "http://clone.invalid", "api_key": "dummy"})
    assert plugin.site_oper.sites == {original.domain: original}


def test_clone_domains_and_cleanup_are_independent(monkeypatch, plugin):
    """同索引器的不同实例不共用域名，各自只清理自己的站点。"""
    configure_runtime(monkeypatch, plugin)
    main_domain = plugin._JackettBridge__build_domain("aither-api")
    one = copy.copy(plugin)
    one.is_clone, one.plugin_instance_id = True, "JackettBridgeOne"
    two = copy.copy(plugin)
    two.is_clone, two.plugin_instance_id = True, "JackettBridgeTwo"
    domains = [main_domain, one._JackettBridge__build_domain("aither-api"), two._JackettBridge__build_domain("aither-api")]
    assert main_domain == "jackett-aither-api.extend"
    assert len(set(domains)) == 3
    for sid, domain in enumerate(domains, 1):
        plugin.site_oper.sites[domain] = SimpleNamespace(id=sid, domain=domain, name="Example", note={})
    one._JackettBridge__cleanup_managed_sites()
    assert set(plugin.site_oper.sites) == {domains[0], domains[2]}
    plugin._JackettBridge__cleanup_managed_sites()
    assert set(plugin.site_oper.sites) == {domains[2]}


def test_clone_domain_survives_host_domain_normalization(plugin):
    """分身域名经宿主 RSS 站点规范化后仍能精确找到原记录。"""
    from app.domain.site import extract_domain
    plugin.is_clone = True
    plugin.plugin_instance_id = "JackettBridgeOne"
    domain = plugin._JackettBridge__build_domain("aither-api")
    assert extract_domain(f"https://{domain}/") == domain


def test_clone_rejects_legacy_shared_snapshot(monkeypatch, plugin):
    """旧分身快照不能在升级同步失败后继续使用主实例的站点身份。"""
    configure_runtime(monkeypatch, plugin)
    plugin.is_clone = True
    plugin.plugin_instance_id = "JackettBridgeOne"
    saved = [_indexer()]
    plugin.get_data = lambda key: copy.deepcopy(saved) if key == "indexers" else None
    monkeypatch.setattr(plugin, "_JackettBridge__start_sync_thread", lambda **kw: None)
    plugin.init_plugin({"enabled": True, "host": "http://clone.invalid", "api_key": "dummy"})
    assert plugin._indexers == []


def test_clone_search_does_not_accept_foreign_snapshot(monkeypatch, plugin):
    """检索边界也必须拒绝其他实例的域名，不能靠快照绕过所有权。"""
    plugin.is_clone = True
    plugin.plugin_instance_id = "JackettBridgeOne"
    plugin._indexers = [_indexer()]
    plugin.site_oper.sites[plugin._indexers[0]["domain"]] = SimpleNamespace(id=9, is_active=True)
    calls = []
    monkeypatch.setattr(plugin, "_JackettBridge__query_jackett", lambda **kw: calls.append(kw) or [])
    plugin._JackettBridge__search_bridged_indexers(keyword="Example", sites=[9])
    assert calls == []


def test_instances_register_distinct_scheduled_jobs(plugin):
    """宿主按 job id 替换任务，两个分身不得覆盖主实例或彼此的同步。"""
    instances = [plugin, copy.copy(plugin), copy.copy(plugin)]
    for instance, identity in zip(instances[1:], ["JackettBridgeOne", "JackettBridgeTwo"]):
        instance.is_clone = True
        instance.plugin_instance_id = identity
    assert len({instance.get_service()[0]["id"] for instance in instances}) == 3


def test_sync_preserves_user_settings(plugin):
    """重新同步已有站点时保留禁用、优先级及用户附加配置。"""
    indexer = _indexer()
    plugin._indexers = [indexer]
    existing = SimpleNamespace(id=9, domain=indexer["domain"], pri=8, is_active=False, proxy=1,
                               timeout=45, note={"user_setting": "keep"})
    plugin.site_oper.sites[existing.domain] = existing
    plugin._JackettBridge__sync_site_records()
    assert (existing.pri, existing.is_active, existing.proxy, existing.timeout) == (8, False, 1, 45)
    assert existing.note["user_setting"] == "keep"
    assert existing.note["indexer_id"] == "aither-api"


def test_search_results_use_current_site_preferences(monkeypatch, plugin):
    """实际资源结果采用数据库优先级及下载设置，索引器快照不能覆盖用户设置。"""
    indexer = _indexer()
    plugin._indexers = [indexer]
    plugin.sites_helper.indexers[indexer["domain"]] = dict(indexer)
    plugin.site_oper.sites[indexer["domain"]] = SimpleNamespace(
        id=9, name="Configured name", domain=indexer["domain"], pri=8, is_active=True,
        proxy=1, cookie="dummy-cookie", ua="custom-agent", downloader="custom-downloader",
    )
    content = "<rss><channel><item><title>Example</title><enclosure url='magnet:?xt=urn:btih:dummy'/></item></channel></rss>"
    monkeypatch.setattr(module, "RequestUtils", lambda **kw: SimpleNamespace(
        get_res=lambda *a, **k: SimpleNamespace(status_code=200, text=content)))
    results = plugin.search_torrents(keyword="Example", sites=[9])
    assert len(results) == 1
    result = results[0]
    assert (result.site, result.site_order, result.site_proxy) == (9, 8, True)
    assert (result.site_cookie, result.site_ua, result.site_downloader) == (
        "dummy-cookie", "custom-agent", "custom-downloader")


@pytest.mark.parametrize("site_exists", [True, False])
def test_search_skips_disabled_or_deleted_site(plugin, monkeypatch, site_exists):
    """禁用或删除的站点不能仅靠索引器快照继续被查询。"""
    plugin._indexers = [_indexer()]
    plugin._indexers[0]["site_id"] = 9
    if site_exists:
        plugin.site_oper.sites[plugin._indexers[0]["domain"]] = SimpleNamespace(id=9, is_active=False)
    calls = []
    monkeypatch.setattr(plugin, "_JackettBridge__query_jackett", lambda **kwargs: calls.append(kwargs) or [])
    plugin._JackettBridge__search_bridged_indexers(keyword="Example", sites=[9])
    assert calls == []


@pytest.mark.parametrize("selected", [[1], []])
def test_search_sources_preserves_its_effective_sites(monkeypatch, plugin, selected):
    """真实宿主 SearchSources.fetch 使用自身有效站点，不能回退系统的 B 站。"""
    from app.chain.search.scan import SearchSources
    from app.schemas.types import MediaType

    plugin._indexers = [_indexer("a", "jackett-a.extend"), _indexer("b", "jackett-b.extend")]
    for sid, indexer in enumerate(plugin._indexers, 1):
        indexer["site_id"] = sid
        plugin.site_oper.sites[indexer["domain"]] = SimpleNamespace(id=sid, is_active=True)
    monkeypatch.setattr(module, "get_configured_system_config", lambda: SimpleNamespace(get=lambda key: [2]))
    called = []
    monkeypatch.setattr(plugin, "_JackettBridge__query_jackett", lambda **kw: called.append(kw["indexer_id"]) or [])
    sources = object.__new__(SearchSources)
    sources.sites = {str(sid): {"id": sid} for sid in selected}
    sources.queries = {"plugin:Example": {"site": "plugin", "keyword": "Example"}}
    sources.media = SimpleNamespace(type=MediaType.TV)
    sources.activity = None
    sources.owner = SimpleNamespace(search_plugin_torrents=plugin.search_torrents)
    sources.fetch("plugin:Example", 0)
    assert called == (["a"] if selected else [])


def test_async_search_keeps_authoritative_empty_scope(monkeypatch, plugin):
    """空有效范围跨线程池仍为空，不回退系统所选的其他站点。"""
    plugin._indexers = [_indexer()]
    plugin.site_oper.sites[plugin._indexers[0]["domain"]] = SimpleNamespace(id=2, is_active=True)
    monkeypatch.setattr(plugin, "_JackettBridge__extract_selected_sites", lambda: [])
    monkeypatch.setattr(module, "get_configured_system_config", lambda: SimpleNamespace(get=lambda key: [2]))
    calls = []
    monkeypatch.setattr(plugin, "_JackettBridge__query_jackett", lambda **kw: calls.append(kw) or [])
    asyncio.run(plugin.async_search_torrents(keyword="Example"))
    assert calls == []
    assert plugin._last_selected_sites == []


def test_successful_empty_sync_removes_last_indexer(monkeypatch, plugin):
    """合法空目录应完成同步并删除旧站点，而不是保留不存在的索引器。"""
    data, _ = configure_runtime(monkeypatch, plugin)
    plugin._indexers = [_indexer()]
    old = SimpleNamespace(id=9, domain=plugin._indexers[0]["domain"], name="Old", url="")
    plugin.site_oper.sites[old.domain] = old
    monkeypatch.setattr(module, "RequestUtils", lambda **kw: SimpleNamespace(get_res=lambda *a, **k: SimpleNamespace(status_code=200, text="<indexers/>")))
    plugin.sync_indexers()
    assert plugin.site_oper.sites == {}
    assert data["indexers"] == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_refresh_keeps_media_type(monkeypatch, plugin, asynchronous):
    """选择音乐浏览时，最终 Torznab 请求只包含音乐分类。"""
    from app.schemas.types import MediaType
    plugin._indexers = [_indexer()]
    site = {"id": 9, "domain": plugin._indexers[0]["domain"], "indexer_id": "aither-api"}
    calls = []
    monkeypatch.setattr(plugin, "_JackettBridge__query_jackett", lambda **kw: calls.append(kw["mtype"]) or [])
    if asynchronous:
        asyncio.run(plugin.async_refresh_torrents(site=site, mtype=MediaType.MUSIC))
    else:
        plugin.refresh_torrents(site=site, mtype=MediaType.MUSIC)
    assert calls == [MediaType.MUSIC]


@pytest.mark.parametrize("content, expected", [("<indexers/>", []), ("<html/>", None), ("<error code='100'/>", None)])
def test_empty_directory_is_distinct_from_invalid_response(plugin, monkeypatch, content, expected):
    """仅合法索引器目录的空结果允许清空站点，HTML或错误响应不允许。"""
    monkeypatch.setattr(module, "RequestUtils", lambda **kw: SimpleNamespace(get_res=lambda *a, **k: SimpleNamespace(status_code=200, text=content)))
    assert plugin._JackettBridge__fetch_indexers_torznab() == expected


@pytest.mark.parametrize("value", ["Mon, 09 Nov 2026 08:00:00 +0000", "2026-11-09T08:00:00+0000", "2026-11-09T08:00:00Z"])
def test_pubdate_converts_to_local_timezone(monkeypatch, value):
    """用固定本地偏移验证日期转换，不依赖测试宿主时区。"""
    class LocalDateTime(datetime):
        """把当前测试的本地时区固定为 UTC+8。"""

        def astimezone(self, tz=None):
            return super().astimezone(tz or timezone(timedelta(hours=8)))

    parsed = LocalDateTime(2026, 11, 9, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(module, "datetime", LocalDateTime)
    if value.startswith("Mon"):
        monkeypatch.setattr(module, "parsedate_to_datetime", lambda text: parsed)
    assert JackettBridge._JackettBridge__parse_pubdate(value) == "2026-11-09 16:00:00"
