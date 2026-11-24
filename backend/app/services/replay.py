"""服务层别名：允许从 ``app.services.replay`` 导入重放服务与引擎。"""
from __future__ import annotations

from app.replay_engine import ReplayEngine, engine
from app.services.delta_replay import DeltaReplayService, service

DeltaReplayRangeService = DeltaReplayService
ReplayService = DeltaReplayService
GapRecoveryService = DeltaReplayService

__all__ = [
    "ReplayEngine", "engine", "DeltaReplayService", "ReplayService",
    "DeltaReplayRangeService", "GapRecoveryService", "service",
]
