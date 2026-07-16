from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlparse


# 仅接受“简体中文字幕”的明确标记；不接受泛化 Chinese，也不接受繁体标记。
SIMPLIFIED_MARKERS = (
    "simplified chinese",
    "chinese simplified",
    "simplified mandarin",
    "zh-cn",
    "zh_cn",
    "zh-hans",
    "zh_hans",
    "简体中文",
    "简体字幕",
    "简中",
    "簡中",
)
SIMPLIFIED_SHORT_RE = re.compile(r"(?<![a-z0-9])chs(?![a-z0-9])", re.IGNORECASE)
TRADITIONAL_MARKERS = (
    "traditional chinese",
    "chinese traditional",
    "zh-tw",
    "zh_tw",
    "zh-hant",
    "zh_hant",
    "繁体中文",
    "繁體中文",
    "繁中",
    "cht",
)


@dataclass(frozen=True)
class SiteConfig:
    key: str
    name: str
    rss_url: str
    base_url: str
    api_token: str
    endpoint: str = "/api/torrents/{id}"
    auth_mode: str = "query"
    token_param: str = "api_token"
    token_header: str = "X-API-Key"
    mp_site_id: int | None = None
    proxy: bool = False

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


def extract_torrent_id(*urls: str | None) -> str | None:
    path_patterns = (
        re.compile(r"/torrents/(?:download/)?(\d+)(?:[/?.]|$)", re.IGNORECASE),
        re.compile(r"/torrent/(?:download/)?(\d+)(?:[/?.]|$)", re.IGNORECASE),
        re.compile(r"/download(?:/torrent)?/(\d+)(?:[/?.]|$)", re.IGNORECASE),
    )
    for raw_url in urls:
        if not raw_url:
            continue
        parsed = urlparse(str(raw_url))
        query = parse_qs(parsed.query)
        for key in ("torrent_id", "torrentid", "id"):
            values = query.get(key)
            if values and str(values[0]).isdigit():
                return str(values[0])
        for pattern in path_patterns:
            match = pattern.search(parsed.path or str(raw_url))
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


def _iter_blocks(text: str):
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    for raw_block in re.split(r"\n\s*\n", normalized):
        lines = [line.strip() for line in raw_block.splitlines() if line.strip()]
        if lines:
            yield lines


def _contains_simplified_marker(text: str) -> bool:
    lowered = text.casefold()
    if any(marker in lowered for marker in TRADITIONAL_MARKERS):
        # 同一轨道明确写为繁体时，不因为同时出现泛化内容而误放行。
        if not any(marker in lowered for marker in SIMPLIFIED_MARKERS) and not SIMPLIFIED_SHORT_RE.search(text):
            return False
    return any(marker in lowered for marker in SIMPLIFIED_MARKERS) or bool(SIMPLIFIED_SHORT_RE.search(text))


def mediainfo_simplified_evidence(text: str | None) -> list[str]:
    """只检查 MediaInfo 的 Text/Subtitle 轨，且只接受明确的简体中文标记。"""
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
        if _contains_simplified_marker(block):
            marker_line = next((line for line in lines if _contains_simplified_marker(line)), lines[0])
            evidence.append(f"MediaInfo 字幕轨：{marker_line[:180]}")
    return list(dict.fromkeys(evidence))


def evaluate_mediainfo(attrs: Mapping[str, Any]) -> SubtitleDecision:
    evidence = mediainfo_simplified_evidence(
        attrs.get("media_info") or attrs.get("mediainfo") or attrs.get("mediaInfo")
    )
    if evidence:
        return SubtitleDecision(True, "检测到简体中文字幕", tuple(evidence))
    return SubtitleDecision(False, "MediaInfo 中未检测到明确的简体中文字幕轨")


def split_custom_words(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [line.strip() for line in str(raw).replace("\r", "\n").split("\n") if line.strip()]


def subscription_has_pending_episode(subscribe: Any, episode_list: Sequence[int] | None) -> bool:
    """依据 MoviePilot 的按集事实 note/episode_priority 判断候选是否仍有缺集。"""
    if getattr(subscribe, "state", None) not in (None, "N", "R"):
        return False
    episodes = {int(ep) for ep in (episode_list or []) if str(ep).isdigit()}
    downloaded = set()
    for episode in getattr(subscribe, "note", None) or []:
        try:
            downloaded.add(int(episode))
        except (TypeError, ValueError):
            continue
    priorities = getattr(subscribe, "episode_priority", None) or {}
    for episode, priority in priorities.items():
        try:
            if float(priority) > 0:
                downloaded.add(int(episode))
        except (TypeError, ValueError):
            continue
    if episodes:
        return bool(episodes - downloaded)
    lack_episode = getattr(subscribe, "lack_episode", None)
    return lack_episode is None or int(lack_episode or 0) > 0


def safe_regex_match(pattern: str | None, text: str) -> bool:
    if not pattern:
        return False
    return bool(re.search(r"%s" % pattern, text, re.IGNORECASE))
