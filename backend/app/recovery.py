"""顶层路径别名：app.recovery。"""
from __future__ import annotations

from app.replay_engine import ReplayEngine, engine  # noqa: F401
from app.services.delta_replay import DeltaReplayService  # noqa: F401

RecoveryEngine = ReplayEngine
RecoveryService = DeltaReplayService
