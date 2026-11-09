"""删除失败保留重试依据，成功单元才清除记录的离线回归。"""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from app.db import ScopedSession
from app.db.models.transferhistory import TransferHistory
from app.plugins import spacecleaner as plugin_module

from .test_plugin import _plugin


@pytest.fixture
def deletion(monkeypatch, tmp_path):
    """只使用临时 SQLite 记录和临时文件，下载器与存储在外部边界替换。"""
    plugin = _plugin()
    plugin._clean_downloader = []
    plugin._configured_dirs = lambda: [tmp_path]
    plugin._delete_other_versions = False
    plugin._pb = [{"k": "311842:M", "p": 100}]
    plugin.save_data = Mock()
    plugin._add_delete_history = Mock()
    source = tmp_path / "download" / "Movie.mkv"
    target = tmp_path / "library" / "Movie.mkv"
    source.parent.mkdir()
    target.parent.mkdir()
    source.write_bytes(b"source")
    target.write_bytes(b"target")
    session = ScopedSession()
    row = TransferHistory(src=str(source), dest=str(target), type="电影", title="Fixture",
                          media_source="themoviedb", media_id="311842", status=True,
                          download_hash="main", downloader="selected")
    session.add(row)
    session.commit()
    record_id = row.id
    session.close()
    record = {"id": record_id, "title": "Fixture", "type": "电影", "tmdbid": 311842,
              "download_hash": "main", "downloader": "selected", "src": str(source),
              "dest": str(target), "src_fileitem": {}, "dest_fileitem": {}}
    unit = {"records": [record], "hash": "main", "tmdbid": 311842, "season": None,
            "is_tv": False, "display": "Fixture"}
    index = plugin._build_torrent_index([SimpleNamespace(
        hash="main", name="Movie.mkv", size=6, save_path=str(source.parent),
        content_path=str(source), downloader="selected")])
    storage = Mock()
    monkeypatch.setattr(plugin_module, "StorageChain", lambda: storage)

    def remaining():
        check = ScopedSession()
        try:
            return check.query(TransferHistory).filter(TransferHistory.id == record_id).count()
        finally:
            check.close()

    yield plugin, unit, index, source, target, storage, remaining
    cleanup = ScopedSession()
    cleanup.query(TransferHistory).filter(TransferHistory.id == record_id).delete()
    cleanup.commit()
    cleanup.close()


def test_torrent_failure_preserves_files_records_and_playback(deletion):
    """未成功停止保种时，不接着删文件，也不清除整理与观看依据。"""
    plugin, unit, index, source, target, _, remaining = deletion
    chain = SimpleNamespace(remove_torrents=lambda **kwargs: False)
    result = plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index)
    assert result is False
    assert source.exists() and target.exists()
    assert remaining() == 1 and plugin._pb
    assert plugin._add_delete_history.call_args.args[1].startswith("删除失败")


def test_file_failure_keeps_history_until_successful_retry(deletion, monkeypatch):
    """第一轮部分文件失败时保留整单元，第二轮缺失文件视为已完成并重试剩余文件。"""
    plugin, unit, index, source, target, _, remaining = deletion
    chain = SimpleNamespace(remove_torrents=lambda **kwargs: True)
    original = plugin._safe_delete_path
    monkeypatch.setattr(plugin, "_safe_delete_path", lambda path: False if path == target else original(path))
    result = plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index)
    assert result is False
    assert not source.exists() and target.exists()
    assert remaining() == 1 and plugin._pb
    monkeypatch.setattr(plugin, "_safe_delete_path", original)
    result = plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index)
    assert result is True
    assert not target.exists() and remaining() == 0 and not plugin._pb


def test_remote_file_failure_does_not_remove_records(deletion):
    """远端存储返回 False 属于失败，不能把用户记录当成删除成功。"""
    plugin, unit, index, _, _, storage, remaining = deletion
    remote = {"storage": "remote", "path": "/library/Fixture/Movie.mkv", "type": "file", "name": "Movie.mkv"}
    unit["records"][0]["dest_fileitem"] = remote
    storage.delete_file.return_value = False
    storage.delete_media_file.return_value = False
    storage.get_file_item_strict.return_value = plugin_module.schemas.FileItem(**remote)
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False
    assert remaining() == 1 and plugin._pb


def test_remote_lookup_error_is_not_treated_as_already_deleted(deletion):
    """查询网盘失败不能冒充文件不存在并清除整理记录。"""
    plugin, unit, index, _, _, storage, remaining = deletion
    unit["records"][0]["dest_fileitem"] = {
        "storage": "remote", "path": "/library/Fixture/Movie.mkv", "type": "file"}
    storage.get_file_item_strict.side_effect = OSError("offline")
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False and remaining() == 1 and plugin._pb


def test_already_missing_remote_file_allows_retry_to_finish(deletion):
    """严格确认远端文件已不存在时，重试可完成而不重复删除。"""
    plugin, unit, index, _, _, storage, remaining = deletion
    unit["records"][0]["dest_fileitem"] = {
        "storage": "remote", "path": "/library/Fixture/Movie.mkv", "type": "file"}
    storage.get_file_item_strict.return_value = None
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is True and remaining() == 0 and not plugin._pb
    storage.delete_file.assert_not_called()


def test_remote_directory_is_not_removed_as_a_regular_file(deletion):
    """非蓝光目录不能沿远端文件路径递归删除，须保留记录。"""
    plugin, unit, index, _, _, storage, remaining = deletion
    remote = {"storage": "remote", "path": "/library/Fixture", "type": "dir"}
    unit["records"][0]["dest_fileitem"] = remote
    storage.get_file_item_strict.return_value = plugin_module.schemas.FileItem(**remote)
    storage.is_bluray_folder.return_value = False
    storage.delete_file.return_value = True
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False and remaining() == 1 and plugin._pb
    storage.delete_file.assert_not_called()


def test_other_version_outside_scope_keeps_playback_progress(deletion):
    """只清理选中下载器的版本后，保留未选中版本仍需要的观看依据。"""
    plugin, unit, index, source, target, _, remaining = deletion
    plugin._clean_downloader = ["selected"]
    session = ScopedSession()
    other = TransferHistory(src=str(source.with_name("Other.mkv")), dest=str(target.with_name("Other.mkv")),
                            type="电影", title="Fixture", media_source="themoviedb", media_id="311842",
                            status=True, download_hash="keep", downloader="unselected")
    session.add(other)
    session.commit()
    other_id = other.id
    session.close()
    try:
        result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                     {"free_gb": 1, "free_percent": 5}, index)
        assert result is True and remaining() == 0 and plugin._pb
        check = ScopedSession()
        assert check.query(TransferHistory).filter(TransferHistory.id == other_id).count() == 1
        check.close()
    finally:
        cleanup = ScopedSession()
        cleanup.query(TransferHistory).filter(TransferHistory.id == other_id).delete()
        cleanup.commit()
        cleanup.close()


def test_outside_scope_unit_never_reaches_deletion(deletion):
    """执行入口也复核范围，不能依赖候选列表永远正确。"""
    plugin, unit, index, source, target, _, remaining = deletion
    plugin._clean_downloader = ["different"]
    calls = []
    chain = SimpleNamespace(remove_torrents=lambda **kwargs: calls.append(kwargs) or True)
    result = plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index)
    assert result is False and not calls
    assert source.exists() and target.exists() and remaining() == 1 and plugin._pb


def test_shared_source_outside_scope_prevents_whole_unit_deletion(deletion):
    """未选中记录仍引用同一文件时，删种和文件步骤都不开始。"""
    plugin, unit, index, source, target, _, remaining = deletion
    plugin._clean_downloader = ["selected"]
    session = ScopedSession()
    other = TransferHistory(src=str(source.with_name("alternate.mkv")), dest=str(source),
                            type="电影", title="Other", media_source="themoviedb", media_id="999",
                            status=True, downloader="unselected")
    session.add(other)
    session.commit()
    other_id = other.id
    session.close()
    calls = []
    try:
        result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: calls.append(kwargs) or True),
                                     {"free_gb": 1, "free_percent": 5}, index)
        assert result is False and not calls
        assert source.exists() and target.exists() and remaining() == 1 and plugin._pb
    finally:
        cleanup = ScopedSession()
        cleanup.query(TransferHistory).filter(TransferHistory.id == other_id).delete()
        cleanup.commit()
        cleanup.close()


def test_stop_during_torrent_delete_keeps_file_and_database_state(deletion):
    """删除请求返回期间收到停止信号，不再继续文件阶段。"""
    plugin, unit, index, source, target, _, remaining = deletion

    def remove(**kwargs):
        plugin._stop_event.set()
        return True

    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=remove),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False
    assert source.exists() and target.exists() and remaining() == 1 and plugin._pb


def test_stop_after_file_delete_retains_retry_records(deletion, monkeypatch):
    """一个文件删完时收到停止信号，剩余文件及数据库仍保留。"""
    plugin, unit, index, source, target, _, remaining = deletion
    original = plugin._safe_delete_path

    def delete(path):
        result = original(path)
        if path == source:
            plugin._stop_event.set()
        return result

    monkeypatch.setattr(plugin, "_safe_delete_path", delete)
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False and not source.exists() and target.exists()
    assert remaining() == 1 and plugin._pb


def test_cross_seed_failure_retains_main_torrent_for_retry(deletion):
    """辅种失败时保留主种入口，下一轮仍能关联并重试全部未删除任务。"""
    plugin, unit, _, source, target, _, remaining = deletion
    torrents = [SimpleNamespace(hash="main", name="Same", size=6, save_path=str(source.parent),
                               content_path=str(source), downloader="selected"),
                SimpleNamespace(hash="cross", name="Same", size=6, save_path=str(source.parent / "replica"),
                                content_path=str(source.parent / "replica" / "Movie.mkv"), downloader="selected")]
    index = plugin._build_torrent_index(torrents)
    live = {"main", "cross"}
    fail = True

    def remove(hashs, **kwargs):
        if hashs == "cross" and fail:
            return False
        live.discard(hashs)
        return True

    chain = SimpleNamespace(remove_torrents=remove)
    assert plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index) is False
    assert live == {"main", "cross"}
    assert source.exists() and target.exists() and remaining() == 1
    fail = False
    assert plugin._delete_unit(unit, chain, {"free_gb": 1, "free_percent": 5}, index) is True
    assert not live and not source.exists() and not target.exists() and remaining() == 0


def test_stop_during_cross_seed_removal_does_not_remove_main(deletion):
    """辅种请求阻塞期间停用后，不能继续发主种删除请求。"""
    plugin, unit, _, source, target, _, remaining = deletion
    index = plugin._build_torrent_index([
        SimpleNamespace(hash=h, name="Same", size=6, save_path=str(source.parent),
                        content_path=str(source), downloader="selected") for h in ("main", "cross")])
    removed = []

    def remove(hashs, **kwargs):
        removed.append(hashs)
        plugin._stop_event.set()
        return True

    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=remove),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is False and removed == ["cross"]
    assert source.exists() and target.exists() and remaining() == 1


def test_successful_local_fileitem_does_not_remove_unrelated_files(deletion):
    """只精确删除媒体文件，目录内未识别用户文件不交给宿主整目录清理。"""
    plugin, unit, index, source, target, storage, remaining = deletion
    protected = target.parent / "notes.txt"
    protected.write_text("keep", encoding="utf-8")
    for side, path in (("src", source), ("dest", target)):
        unit["records"][0][side + "_fileitem"] = {
            "storage": "local", "path": str(path), "type": "file", "name": path.name}
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is True
    assert protected.exists() and not source.exists() and not target.exists() and remaining() == 0
    storage.delete_media_file.assert_not_called()


def test_clean_count_only_counts_successful_units(monkeypatch):
    """失败单元不占成功数量，但有限数量仍以成功单元达标停止。"""
    plugin = _plugin()
    plugin._clean_downloader = []
    plugin._sync_pb_from_data = Mock()
    plugin._prune_orphan_pb = Mock()
    plugin._get_chain = lambda: None
    plugin._collect_delete_units = lambda **kwargs: [{"display": title} for title in ("failed", "success", "unused")]
    plugin._get_cached_torrents = lambda chain: []
    plugin._get_cached_space_info = lambda: {"free_gb": 1, "free_percent": 1}
    attempts = []

    def delete(unit, *args, **kwargs):
        attempts.append(deepcopy(unit))
        return unit["display"] == "success"

    plugin._delete_unit = delete
    plugin._clean_resources({"free_gb": 1, "free_percent": 1})
    assert [unit["display"] for unit in attempts] == ["failed", "success"]


@pytest.mark.parametrize("stage", ["history", "notification", "playback"])
def test_reporting_failure_does_not_turn_completed_deletion_into_failure(deletion, stage):
    """删除已提交后附属状态或通知故障不回报失败，避免突破本轮删除数量。"""
    plugin, unit, index, source, target, _, remaining = deletion
    if stage == "history":
        plugin._add_delete_history.side_effect = [OSError("history unavailable"), None]
    elif stage == "notification":
        plugin._notify = True
        plugin._get_space_info = lambda: {"free_gb": 1, "free_percent": 5}
        plugin.post_message = Mock(side_effect=OSError("notification unavailable"))
    else:
        plugin._delete_pb_by_tmdbid = Mock(side_effect=OSError("playback persistence unavailable"))
    result = plugin._delete_unit(unit, SimpleNamespace(remove_torrents=lambda **kwargs: True),
                                 {"free_gb": 1, "free_percent": 5}, index)
    assert result is True
    assert not source.exists() and not target.exists() and remaining() == 0
