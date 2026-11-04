"""
读取原样随包分发的 CC Switch User-Agent 预设。

仅解析固定资源中的 JSON 字符串数组，不执行 TypeScript，也不更改默认 User-Agent。
"""

from __future__ import annotations

import hashlib
import json
import re
from functools import cache
from pathlib import Path

# 源文件来自 CC Switch 提交 7569b27af4f485172950fb6989f5643163842bd1。
USER_AGENT_RESOURCE_SHA256 = "49d62f3cdcc2c96263ae8fd67e381733be7846dc8cbe16fb22ad0af3ffa7f52a"


def _parse_user_agent_presets(source: str) -> tuple[str, ...]:
    """
    仅接受上游导出的静态字符串数组，拒绝表达式与无效 HTTP 头值。

    :param source: 随包 TypeScript 资源的文本
    :return: 保持上游顺序的预设值
    :raises ValueError: 没有匹配数组、数组含表达式或头值不合法
    """
    match = re.search(r"export const USER_AGENT_PRESETS: readonly string\[\] = (\[[\s\S]*?\]);", source)
    if not match:
        raise ValueError("User-Agent 预设资源缺少静态数组")
    try:
        values = json.loads(re.sub(r",\s*\]$", "]", match.group(1)))
    except json.JSONDecodeError as error:
        raise ValueError("User-Agent 预设资源必须是静态字符串数组") from error
    if not values or any(
        not isinstance(value, str) or not value or any(ord(char) < 32 or ord(char) == 127 for char in value)
        for value in values
    ):
        raise ValueError("User-Agent 预设包含无效头值")
    return tuple(values)


@cache
def get_user_agent_presets() -> tuple[str, ...]:
    """
    校验原始资源并读取预设，后续页面渲染复用同一不可变结果。

    :return: 原样复用的上游 User-Agent 预设；用户需显式选择
    :raises ValueError: 资源与记录的上游版本不一致或内容无效
    """
    resource = Path(__file__).parent / "third_party" / "cc-switch" / "userAgentPresets.ts"
    data = resource.read_bytes()
    if hashlib.sha256(data).hexdigest() != USER_AGENT_RESOURCE_SHA256:
        raise ValueError("User-Agent 预设资源与记录的上游版本不一致")
    return _parse_user_agent_presets(data.decode("utf-8"))
