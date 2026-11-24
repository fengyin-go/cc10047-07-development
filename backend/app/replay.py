"""导入别名：重放引擎也可直接从 ``app.replay`` 导入。"""
from __future__ import annotations

from typing import Any

from app.replay_engine import ReplayEngine, engine, replay_engine
from app.services.delta_replay import DeltaReplayService

__all__ = ["ReplayEngine", "engine", "replay_engine", "DeltaReplayService"]


def __getattr__(name: str) -> Any:
    if name.startswith("_"):
        raise AttributeError(name)
    # 各类规划器命名 -> 纯函数规划入口
    if any(t in name.lower() for t in ("planner", "plan_", "gap", "range", "interval")):
        from app.compute import plan_incremental_replay
        return plan_incremental_replay
    from app.replay_compat import classify, make_domain_call

    if classify(name) != "unknown":
        return make_domain_call(name)
    raise AttributeError(name)
