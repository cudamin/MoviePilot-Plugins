"""清理范围、辅种身份与残留目录的离线回归测试。"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from app.plugins import spacecleaner as plugin_module

from .test_plugin import _FakeSession, _plugin, _record


@pytest.fixture
def cleanup_plugin(monkeypatch):
    """隔离下载器配置与文件格式设置，不使用真实文件库或下载器。"""
    plugin = _plugin()
    plugin._clean_downloader = ["download"]
    plugin._configured_dirs = list
    monkeypatch.setattr(plugin_module, "settings", SimpleNamespace(
        RMT_MEDIAEXT=[".mkv", ".mp4", ".asf", ".custom"],
        RMT_AUDIOEXT=[".mp3", ".flac"],
    ))
    return plugin


def _owned_record(record_id, owner="download"):
    """构造相同季集但具有明确下载器归属的整理记录。"""
    record = _record(record_id, 1)
    record.downloader = owner
    record.download_hash = f"hash-{record_id}"
    record.status = True
    record.src_storage = "local"
    record.dest_storage = "local"
    return record


def _torrent(torrent_hash, owner="download", name="Watched", path=None):
    """构造完整的清理索引输入，允许共享目录但使用不同实际内容路径。"""
    return SimpleNamespace(hash=torrent_hash, downloader=owner, name=name,
                           size=104857600, save_path=r"C:\Downloads",
                           content_path=path or rf"C:\Downloads\{name}.mkv")


class _Downloader:
    """内存下载器边界，仅记录真实清理方法发出的请求。"""

    def __init__(self, result=True):
        self.calls = []
        self.result = result

    def remove_torrents(self, **kwargs):
        """模拟下载器结果或异常，不接触外部服务。"""
        self.calls.append(kwargs)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.parametrize("owner", ["seed-only", "", None])
def test_collect_units_preserves_unselected_or_unknown_owner(cleanup_plugin, monkeypatch, owner):
    """有限扫描范围不允许未知或未选下载器的记录进入删除队列。"""
    plugin = cleanup_plugin
    plugin._get_playback_pb = lambda: [{"k": "123:S01E13", "p": 100, "t": "2026-01-01"}]
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([_owned_record(1, owner)]))
    assert plugin._collect_delete_units(log_skipped=False) == []


def test_collect_units_keeps_only_selected_records(cleanup_plugin, monkeypatch):
    """同季其它下载器的版本不得因整季归并而被连带删除。"""
    plugin = cleanup_plugin
    plugin._get_playback_pb = lambda: [{"k": "123:S01E13", "p": 100, "t": "2026-01-01"}]
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([
        _owned_record(1), _owned_record(2, "seed-only"),
    ]))
    units = plugin._collect_delete_units(log_skipped=False)
    assert [[record["id"] for record in unit["records"]] for unit in units] == [[1]]


def test_other_versions_preserves_unselected_and_unknown_records(cleanup_plugin, monkeypatch):
    """其它版本查询必须遵循同样的下载器范围。"""
    plugin = cleanup_plugin
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([
        _owned_record(1), _owned_record(2), _owned_record(3, "seed-only"), _owned_record(4, ""),
    ]))
    versions = plugin._find_other_version_records([
        {"id": 1, "tmdbid": 123, "type": "电视剧", "seasons": "S01", "episodes": "E13"},
    ])
    assert [record["id"] for record in versions] == [2]


def test_record_hashes_preserves_unselected_record(cleanup_plugin):
    """脱离候选收集单独调用删种辅助方法时也不能越过范围。"""
    assert cleanup_plugin._collect_record_torrent_hashes([
        {"download_hash": "seed-hash", "downloader": "seed-only", "src": r"C:\Downloads\Watched.mkv"},
    ], {}) == []


@pytest.mark.parametrize("owner", ["seed-only", ""])
def test_actual_delete_rejects_outside_or_unknown_owner(cleanup_plugin, owner):
    """索引外种子不得回退到未选择或默认下载器执行删除。"""
    chain = _Downloader()
    failures = []
    deleted = cleanup_plugin._delete_downloader_torrents(
        chain, "seed-hash", "Watched", {}, owner, delete_file=False, failures=failures,
    )
    assert deleted == 0
    assert chain.calls == []
    assert failures


def test_actual_delete_enforces_owner_from_index(cleanup_plugin):
    """调用者提供选择范围内名称也不能覆盖种子的真实越界归属。"""
    chain = _Downloader()
    failures = []
    index = cleanup_plugin._build_torrent_index([_torrent("main", "seed-only")])
    assert cleanup_plugin._delete_downloader_torrents(
        chain, "main", "Watched", index, "download", failures=failures,
    ) == 0
    assert chain.calls == []
    assert failures


@pytest.mark.parametrize("result", [False, RuntimeError("offline")])
def test_actual_delete_reports_failure_for_unit_retry(cleanup_plugin, result):
    """下载器失败必须保留可供删除单元停止和重试的失败结果。"""
    failures = []
    chain = _Downloader(result)
    index = cleanup_plugin._build_torrent_index([_torrent("main")])
    assert cleanup_plugin._delete_downloader_torrents(
        chain, "main", "Watched", index, failures=failures,
    ) == 0
    assert failures


def test_actual_delete_accepts_selected_owner(cleanup_plugin):
    """明确选中的种子仍按其归属删除，并保留成功计数。"""
    failures = []
    chain = _Downloader()
    index = cleanup_plugin._build_torrent_index([_torrent("main")])
    assert cleanup_plugin._delete_downloader_torrents(
        chain, "main", "Watched", index, delete_file=False, failures=failures,
    ) == 1
    assert chain.calls == [{"hashs": "main", "delete_file": False, "downloader": "download"}]
    assert failures == []


def test_all_downloaders_mode_keeps_unowned_legacy_record(cleanup_plugin):
    """未限制范围时保留无下载器字段的旧记录兼容性。"""
    cleanup_plugin._clean_downloader = []
    assert cleanup_plugin._collect_record_torrent_hashes([
        {"download_hash": "legacy", "downloader": "", "src": ""},
    ], {}) == [("legacy", False)]


@pytest.mark.parametrize("reference", ["src", "dest"])
def test_shared_source_is_protected_by_unselected_reference(cleanup_plugin, monkeypatch, reference):
    """范围外记录的源或成品引用同一源文件时，必须保留该共享文件。"""
    protected = _owned_record(2, "seed-only")
    protected.src = r"C:\Elsewhere\Other.mkv"
    protected.dest = r"C:\Media\Other.mkv"
    setattr(protected, reference, r"C:\Downloads\Watched.mkv")
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([protected]))
    record = {"id": 1, "src": r"C:\Downloads\Watched.mkv", "src_storage": "local", "downloader": "download"}
    assert cleanup_plugin._protected_source_paths([record]) == {
        ("local", str(Path(r"C:\Downloads\Watched.mkv")).lower()),
    }


def test_same_path_on_different_storage_does_not_protect_local_source(cleanup_plugin, monkeypatch):
    """不同存储里的同名路径不是同一文件，不应阻塞本地清理。"""
    other = _owned_record(2, "seed-only")
    other.src = r"C:\Downloads\Watched.mkv"
    other.src_storage = "remote"
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([other]))
    assert cleanup_plugin._protected_source_paths([
        {"id": 1, "src": r"C:\Downloads\Watched.mkv", "src_storage": "local", "downloader": "download"},
    ]) == set()


def test_shared_source_query_checks_real_database(cleanup_plugin):
    """真实隔离数据库中，失败记录及空下载器归属也必须保护共享源。"""
    from app.db import ScopedSession
    from app.db.models.transferhistory import TransferHistory

    session = ScopedSession()
    reference = TransferHistory(src=r"C:\Downloads\Watched.mkv", src_storage="local",
                                downloader=None, status=False)
    try:
        session.add(reference)
        session.commit()
        assert cleanup_plugin._protected_source_paths([
            {"src": r"C:\Downloads\Watched.mkv", "src_storage": "local", "downloader": "download"},
        ])
    finally:
        session.delete(reference)
        session.commit()
        session.close()


def test_shared_destination_is_protected_by_unselected_reference(cleanup_plugin, monkeypatch):
    """本单元成品也在删除范围内，被未选记录引用时必须整单元保留。"""
    protected = _owned_record(2, "seed-only")
    protected.src = r"C:\Media\Watched.mkv"
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([protected]))
    assert cleanup_plugin._protected_source_paths([
        {"id": 1, "src": r"C:\Downloads\Watched.mkv", "dest": r"C:\Media\Watched.mkv",
         "src_storage": "local", "dest_storage": "local", "downloader": "download"},
    ]) == {("local", str(Path(r"C:\Media\Watched.mkv")).lower())}


def test_remote_paths_remain_case_sensitive(cleanup_plugin, monkeypatch):
    """远端存储路径大小写不同，不得因 Windows 本机规范化误认共享文件。"""
    other = _owned_record(2, "seed-only")
    other.src = "/media/watched.mkv"
    other.src_storage = "remote"
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([other]))
    assert cleanup_plugin._protected_source_paths([
        {"src": "/media/Watched.mkv", "src_storage": "remote", "downloader": "download"},
    ]) == set()


def test_scope_preflight_preserves_all_torrents_on_unselected_cross_seed(cleanup_plugin):
    """先发现关联任务越界再停止，不能先删主任务再发现越界。"""
    chain = _Downloader()
    failures = []
    index = cleanup_plugin._build_torrent_index([
        _torrent("main"), _torrent("cross", owner="seed-only"),
    ])
    cleanup_plugin._delete_downloader_torrents(chain, "main", "Watched", index, failures=failures)
    assert chain.calls == []
    assert failures


def test_explicit_empty_snapshot_scans_all_downloaders(cleanup_plugin):
    """传入空范围快照表示扫描全部，不能被后来的有限配置覆盖。"""
    chain = _Downloader()
    failures = []
    index = cleanup_plugin._build_torrent_index([_torrent("main", owner="seed-only")])
    assert cleanup_plugin._delete_downloader_torrents(
        chain, "main", "Watched", index, failures=failures, downloaders=[],
    ) == 1
    assert chain.calls[0]["downloader"] == "seed-only"


def test_candidate_snapshot_preserves_remote_storage(cleanup_plugin, monkeypatch):
    """删除快照保留源和成品存储，避免后续文件删除误落到本地。"""
    record = _owned_record(1)
    record.src_storage = "remote-src"
    record.dest_storage = "remote-dest"
    cleanup_plugin._get_playback_pb = lambda: [{"k": "123:S01E13", "p": 100, "t": "2026-01-01"}]
    monkeypatch.setattr("app.db.ScopedSession", lambda: _FakeSession([record]))
    snapshot = cleanup_plugin._collect_delete_units(log_skipped=False)[0]["records"][0]
    assert (snapshot["src_storage"], snapshot["dest_storage"]) == ("remote-src", "remote-dest")


def test_same_directory_and_size_does_not_remove_unrelated_torrent(cleanup_plugin):
    """保存根目录和体积相同不代表共享内容，不能删除无关任务。"""
    chain = _Downloader()
    index = cleanup_plugin._build_torrent_index([
        _torrent("main"), _torrent("unrelated", name="Unwatched"),
    ])
    cleanup_plugin._delete_downloader_torrents(chain, "main", "Watched", index)
    assert [call["hashs"] for call in chain.calls] == ["main"]


def test_same_content_path_and_size_still_removes_cross_seed(cleanup_plugin):
    """名称不同但内容路径相同的真正辅种仍应移除任务。"""
    chain = _Downloader()
    index = cleanup_plugin._build_torrent_index([
        _torrent("main"), _torrent("cross", name="DifferentName", path=r"C:\Downloads\Watched.mkv"),
    ])
    cleanup_plugin._delete_downloader_torrents(chain, "main", "Watched", index)
    assert {call["hashs"]: call["delete_file"] for call in chain.calls} == {"main": True, "cross": False}


@pytest.mark.parametrize("filename", [
    "Unwatched.asf", "Unwatched.3gp", "Unwatched.tp", "Unwatched.f4v", "Unwatched.mkv",
    "Music.mp3", "Music.flac", "User.custom", "notes.txt", "database.db", "unknown",
])
def test_directory_cleanup_preserves_media_and_unknown_files(cleanup_plugin, tmp_path, filename):
    """仅剩一个未看媒体或未知文件时，也不能当成元数据删除整个目录。"""
    directory = tmp_path / "download"
    directory.mkdir()
    protected_file = directory / filename
    protected_file.write_bytes(b"isolated fixture")
    cleanup_plugin._cleanup_download_dir(directory)
    assert protected_file.exists()


def test_directory_cleanup_removes_only_known_metadata(cleanup_plugin, tmp_path):
    """确定只剩刮削资料、字幕和系统垃圾的资源目录仍可清理。"""
    directory = tmp_path / "download"
    directory.mkdir()
    for filename in ("movie.nfo", "poster.jpg", "subtitle.srt", "Thumbs.db"):
        (directory / filename).write_bytes(b"isolated metadata")
    cleanup_plugin._cleanup_download_dir(directory)
    assert not directory.exists()


def test_custom_media_extension_overrides_metadata_suffix(cleanup_plugin, monkeypatch, tmp_path):
    """用户明确把后缀配置为媒体时，不应按默认元数据后缀删除。"""
    monkeypatch.setattr(plugin_module, "settings", SimpleNamespace(RMT_MEDIAEXT=[".nfo"], RMT_AUDIOEXT=[]))
    directory = tmp_path / "download"
    directory.mkdir()
    media = directory / "CustomVideo.nfo"
    media.write_bytes(b"isolated media")
    cleanup_plugin._cleanup_download_dir(directory)
    assert media.exists()
