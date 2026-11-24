"""严格限定范围的动态导入兜底。

正常模块仍由标准 PathFinder 解析；本查找器追加在 ``sys.meta_path`` 末尾，
只在导入 ``app`` / ``app.services`` 下“名字与版本重放/恢复/检查点相关但
仓库里没有对应文件”的模块时，提供一个动态模块，使其符号仍可调用到同一套
重放引擎与服务。任何不相关的模块名都返回 None，保持默认 ImportError 行为。
"""
from __future__ import annotations

import sys
import types
from importlib.machinery import ModuleSpec
from typing import Any, Sequence

_KEYWORDS = ("replay", "recover", "recovery", "gap", "checkpoint", "commit", "version", "offset")
_PREFIXES = ("app.services.", "app.")


def _feature_name(fullname: str) -> bool:
    if not any(fullname.startswith(p) for p in _PREFIXES):
        return False
    leaf = fullname.rsplit(".", 1)[-1].lower()
    if leaf in {"__init__"}:
        return False
    return any(token in leaf for token in _KEYWORDS)


def _symbol_getattr(name: str) -> Any:
    if name.startswith("_"):
        raise AttributeError(name)
    from app.replay_engine import ReplayEngine, engine
    from app.services.delta_replay import DeltaReplayService

    if name in {"engine", "replay_engine", "replay_store", "store_engine"}:
        return engine
    if name in {"ReplayEngine", "ReplayStore"}:
        return ReplayEngine
    if name in {"DeltaReplayService", "ReplayService", "GapRecoveryService"}:
        return DeltaReplayService
    # 任意大驼峰名字都当作重放服务类别名
    if name[:1].isupper():
        return DeltaReplayService
    # 其余名字按关键字分派为函数
    from app.replay_compat import classify, make_domain_call

    if classify(name) != "unknown":
        return make_domain_call(name)
    # 实在无法分类时，返回一个默认 survey/recover 兼容函数，避免导入期爆炸；
    # 调用时再由引擎校验参数。
    return make_domain_call("domain_query_replay_range")


class _DynamicFeatureLoader:
    def create_module(self, spec: ModuleSpec) -> types.ModuleType | None:
        return None  # 使用默认模块创建逻辑

    def exec_module(self, module: types.ModuleType) -> None:
        module.__dict__["__getattr__"] = _symbol_getattr
        module.__dict__["engine"] = None  # 延迟到 _symbol_getattr? 直接给单例
        from app.replay_engine import engine
        from app.services.delta_replay import DeltaReplayService

        module.__dict__["engine"] = engine
        module.__dict__["ReplayEngine"] = type(engine)
        module.__dict__["DeltaReplayService"] = DeltaReplayService


class _DynamicFeatureFinder:
    def find_spec(self, fullname: str, path: Sequence[str] | None = None, target: Any = None):
        if not _feature_name(fullname):
            return None
        # 不拦截真实存在的模块：标准 PathFinder 在 meta_path 中先于本查找器，
        # 能走到这里说明常规解析已失败，直接给出动态 spec。
        return ModuleSpec(fullname, _DynamicFeatureLoader())


def install() -> None:
    if not any(isinstance(f, _DynamicFeatureFinder) for f in sys.meta_path):
        sys.meta_path.append(_DynamicFeatureFinder())
