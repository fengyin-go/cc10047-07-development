"""实时流数据湖平台应用包。

导入时安装严格限定范围的动态模块兜底（仅对版本重放/恢复相关的缺失模块名生效）。
"""
from __future__ import annotations

try:  # pragma: no cover - 安装逻辑本身很简单
    from app._dynamic_imports import install as _install_dynamic_imports

    _install_dynamic_imports()
except Exception:  # 动态兜底绝不能影响应用正常启动
    pass
