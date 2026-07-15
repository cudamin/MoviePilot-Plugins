from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse


CHINESE_MARKERS = (
    "chinese",
    "simplified chinese",
    "traditional chinese",
    "mandarin",
    "cantonese",
    "zh-cn",
    "zh-hans",
    "zh-tw",
    "zh-hant",
    "简体中文",
    "繁體中文",
    "繁体中文",
    "简中",
    "簡中",
    "繁中",
    "中文字幕",
    "中字",
)

SHORT_MARKER_RE = re.compile(r"(?<![a-z0-9])(?:zh|zho|chi|chs|cht)(?![a-z0-9])", re.IGNORECASE)
SUBTITLE_EXTENSIONS = {".srt", ".ass", ".ssa", ".sup", ".vtt", ".sub", ".idx"}


@dataclass(frozen=True)
class SiteProfile:
    name: str
    base_url: str
    api_token: str
    match: tuple[str, ...] = field(default_factory=tuple)
    endpoint: str = "/api/torrents/{id}"
    auth_mode: str = "query"
    token_param: str = "api_token"
    token_header: str = "X-API-Key"
    headers: Mapping[str, str] = field(default_factory=dict)

    def matches(self, *values: str | None) -> bool:
        haystack = "\n".join(str(value or "") for value in values).lower()
        needles = self.match or (urlparse(self.base_url).netloc, self.name)
        return any(str(needle).strip().lower() in haystack for needle in needles if str(needle).strip())

    def detail_url(self, torrent_id: str) -> str:
        endpoint = self.endpoint.format(id=torrent_id)
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"


@dataclass(frozen=True)
class SubtitleDecision:
    allowed: bool
    reason: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    api_error: bool = False


@dataclass
class CacheEntry:
    decision: SubtitleDecision
    expires_at: float

    def valid(self) -> bool:
        return time.time() < self.expires_at


def parse_profiles(raw: str | Sequence[Mapping[str, Any]] | None) -> list[SiteProfile]:
    if raw is None or raw == "":
        return []
    data: Any = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(data, Mapping):
        data = [data]
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise ValueError("站点配置必须是 JSON 数组或对象")

    profiles: list[SiteProfile] = []
    for index, item in enumerate(data, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"第 {index} 个站点配置不是对象")
        name = str(item.get("name") or f"UNIT3D-{index}").strip()
        base_url = str(item.get("base_url") or "").strip().rstrip("/")
        token = str(item.get("api_token") or "").strip()
        if not base_url:
            raise ValueError(f"站点 {name} 缺少 base_url")
        if not base_url.startswith(("http://", "https://")):
            raise ValueError(f"站点 {name} 的 base_url 必须以 http:// 或 https:// 开头")
        match_value = item.get("match") or []
        if isinstance(match_value, str):
            match_items = (match_value,)
        elif isinstance(match_value, Sequence):
            match_items = tuple(str(value) for value in match_value if str(value).strip())
        else:
            raise ValueError(f"站点 {name} 的 match 必须是字符串或数组")
        headers = item.get("headers") or {}
        if not isinstance(headers, Mapping):
            raise ValueError(f"站点 {name} 的 headers 必须是对象")
        profiles.append(
            SiteProfile(
                name=name,
                base_url=base_url,
                api_token=token,
                match=match_items,
                endpoint=str(item.get("endpoint") or "/api/torrents/{id}"),
                auth_mode=str(item.get("auth_mode") or "query").strip().lower(),
                token_param=str(item.get("token_param") or "api_token").strip(),
                token_header=str(item.get("token_header") or "X-API-Key").strip(),
                headers={str(k): str(v) for k, v in headers.items()},
            )
        )
    return profiles


def extract_torrent_id(*urls: str | None) -> str | None:
    path_patterns = (
        re.compile(r"/torrents/(?:download/)?(\d+)(?:[/?.]|$)", re.IGNORECASE),
        re.compile(r"/torrent/(?:download/)?(\d+)(?:[/?.]|$)", re.IGNORECASE),
        re.compile(r"/download(?:/torrent)?/(\d+)(?:[/?.]|$)", re.IGNORECASE),
    )
    for raw_url in urls:
        if not raw_url:
            continue
        url = str(raw_url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        for key in ("torrent_id", "torrentid", "id"):
            values = query.get(key)
            if values and str(values[0]).isdigit():
                return str(values[0])
        for pattern in path_patterns:
            match = pattern.search(parsed.path or url)
            if match:
                return match.group(1)
    return None


def normalize_torrent_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    current: Any = payload.get("data", payload)
    if isinstance(current, Mapping) and isinstance(current.get("attributes"), Mapping):
        attrs = dict(current["attributes"])
        attrs.setdefault("id", current.get("id"))
        return attrs
    if isinstance(current, Mapping):
        return dict(current)
    return {}


def _contains_chinese_marker(text: str) -> bool:
    lowered = text.casefold()
    if any(marker.casefold() in lowered for marker in CHINESE_MARKERS):
        return True
    return bool(SHORT_MARKER_RE.search(text))


def _iter_blocks(text: str) -> Iterable[list[str]]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    for raw_block in re.split(r"\n\s*\n", normalized):
        lines = [line.strip() for line in raw_block.splitlines() if line.strip()]
        if lines:
            yield lines


def mediainfo_evidence(text: str | None) -> list[str]:
    """Only accepts Chinese markers inside Text/Subtitle tracks.

    This deliberately ignores Chinese audio tracks to avoid treating Mandarin audio
    as Chinese subtitles.
    """
    if not text:
        return []
    evidence: list[str] = []
    for lines in _iter_blocks(str(text)):
        header = lines[0].casefold()
        is_text_block = bool(
            re.match(r"^(text|subtitle)(?:\s*#?\d+)?(?:\s|$)", header)
            or header.startswith("文本")
            or header.startswith("字幕")
        )
        if not is_text_block:
            continue
        block = "\n".join(lines)
        if _contains_chinese_marker(block):
            evidence.append(f"MediaInfo 字幕轨：{lines[0]}")
    return evidence


def _extract_bdinfo_subtitle_section(text: str) -> str:
    match = re.search(r"(?ims)^[ \t]*(?:subtitles?|字幕)[ \t]*:[ \t]*\n(?P<body>.*?)(?=^[ \t]*[A-Z][A-Z0-9 /_-]{2,}[ \t]*:[ \t]*\n|\Z)", text)
    return match.group("body") if match else ""


def bdinfo_evidence(text: str | None) -> list[str]:
    if not text:
        return []
    section = _extract_bdinfo_subtitle_section(str(text))
    if not section:
        return []
    for line in section.splitlines():
        clean = line.strip()
        if clean and _contains_chinese_marker(clean):
            return [f"BDInfo 字幕表：{clean[:160]}"]
    return []


def file_evidence(files: Any) -> list[str]:
    if not files:
        return []
    if isinstance(files, Mapping):
        iterable: Iterable[Any] = files.values()
    elif isinstance(files, Sequence) and not isinstance(files, (str, bytes)):
        iterable = files
    else:
        iterable = [files]

    evidence: list[str] = []
    for item in iterable:
        if isinstance(item, Mapping):
            name = str(item.get("name") or item.get("path") or item.get("filename") or "")
        else:
            name = str(item)
        if not name:
            continue
        suffix = PurePosixPath(name.replace("\\", "/")).suffix.casefold()
        if suffix in SUBTITLE_EXTENSIONS and _contains_chinese_marker(name):
            evidence.append(f"外挂字幕文件：{name}")
    return evidence


def free_text_evidence(*texts: str | None) -> list[str]:
    evidence: list[str] = []
    release_pattern = re.compile(
        r"(?i)(?<![a-z0-9])(?:chs|cht|zh-cn|zh-tw|zh-hans|zh-hant)(?![a-z0-9])|"
        r"简体中文|繁[體体]中文|简中|簡中|繁中|中文字幕|中字"
    )
    for text in texts:
        if text and release_pattern.search(str(text)):
            evidence.append(f"标题/描述标记：{release_pattern.search(str(text)).group(0)}")
    return evidence


def evaluate_torrent_attributes(
    attrs: Mapping[str, Any],
    *,
    inspect_mediainfo: bool = True,
    inspect_bdinfo: bool = True,
    inspect_files: bool = True,
    inspect_free_text: bool = False,
) -> SubtitleDecision:
    evidence: list[str] = []
    if inspect_mediainfo:
        evidence.extend(mediainfo_evidence(attrs.get("media_info") or attrs.get("mediainfo") or attrs.get("mediaInfo")))
    if inspect_bdinfo:
        evidence.extend(bdinfo_evidence(attrs.get("bd_info") or attrs.get("bdinfo") or attrs.get("bdInfo")))
    if inspect_files:
        evidence.extend(file_evidence(attrs.get("files") or attrs.get("file_list")))
    if inspect_free_text:
        evidence.extend(
            free_text_evidence(
                attrs.get("name") or attrs.get("title"),
                attrs.get("description"),
            )
        )
    if evidence:
        unique = tuple(dict.fromkeys(evidence))
        return SubtitleDecision(True, "检测到中文字幕", unique)
    return SubtitleDecision(False, "API 详情中未检测到中文字幕")
