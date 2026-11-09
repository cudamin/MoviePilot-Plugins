"""jackettBridge 回归共用的隔离实例夹具。"""

import threading

import pytest

@pytest.fixture
def plugin():
    """
    构造不触发宿主完整启动流程的插件实例。
    """
    from jackettbridge import JackettBridge
    from .test_regressions import _FakeEvents, _FakeSiteOper, _FakeSites

    instance = object.__new__(JackettBridge)
    instance._state_lock = threading.RLock()
    instance._sync_lock = threading.Lock()
    instance._sync_stop_event = threading.Event()
    instance._sync_thread = None
    instance._enabled = True
    instance._onlyonce = False
    instance._host = "http://jackett.local:9117"
    instance._api_key = "test-key"
    instance._password = ""
    instance._cron = "0 0 * * *"
    instance._search_timeout = 30
    instance._search_budget = 15
    instance._result_num = 100
    instance._indexers = []
    instance._indexer_catalog = []
    instance._selected_indexers = []
    instance._indexers_authoritative = False
    instance._last_selected_sites = None
    instance._last_sites_source = "unlimited"
    instance._last_frame_sites = None
    instance.sites_helper = _FakeSites()
    instance.site_oper = _FakeSiteOper()
    instance.eventmanager = _FakeEvents()
    return instance
