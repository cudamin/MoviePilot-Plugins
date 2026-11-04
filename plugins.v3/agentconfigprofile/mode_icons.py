"""
三模式使用本地矢量图标，避免依赖宿主的远端图标目录。

图标为插件自绘的插头、分支路径和叠层，符号含义与 CC Switch 模式页签一致。
"""

_SHAPES = {
    "direct": '<path d="M9 3v5m6-5v5M7 8h10v5a5 5 0 0 1-5 5v3m-5-8V8m10 5V8"/>',
    "route": '<circle cx="6" cy="5" r="2"/><circle cx="18" cy="5" r="2"/>'
             '<circle cx="6" cy="19" r="2"/><path d="M6 7v10m2-12h4a6 6 0 0 1 0 12H8"/>',
    "stack": '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
}


def mode_svg(mode: str) -> str:
    """
    返回固定的模式图标 SVG，不插入用户输入或外部资源地址。
    """
    return ('<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" '
            'fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" '
            'stroke-linejoin="round" aria-hidden="true">' + _SHAPES[mode] + '</svg>')
