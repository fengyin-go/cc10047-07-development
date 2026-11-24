"""湖表版本增量重放 / 缺口恢复领域服务（独立模块入口）。

真实状态与算法都在 ``app.replay_engine``，这里仅按平台的服务骨架暴露
``domain_*`` 方法并注册到服务表，方便按 ``delta_replay`` / ``replay`` 分派。
"""
from __future__ import annotations

from typing import Any

from app.replay_engine import ReplayEngine, engine
from app.services.base import BaseModuleService


class DeltaReplayService(BaseModuleService):
    MODULE = "delta_replay"
    ENTITY = "增量重放"
    LIST_FIELDS = ["目标湖表", "版本范围", "重放区间", "下一读位置"]
    REQUIRED_FIELDS = ["目标湖表"]
    STATUS_ORDER = ["待恢复", "部分确认_缺口保留", "已完成"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def __init__(self, engine_impl: ReplayEngine | None = None) -> None:
        self.engine = engine_impl or engine

    # 检索 / 规划
    def domain_query_replay_range(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.survey(payload or {}, **kw)

    query = domain_query_replay_range
    survey = domain_query_replay_range
    search = domain_query_replay_range

    def domain_search_versions(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.survey(payload or {}, **kw)

    def domain_version_gaps(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.survey(payload or {}, **kw)

    # 恢复 / 确认
    def domain_recover(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.recover(payload or {}, **kw)

    recover = domain_recover
    confirm = domain_recover

    def domain_confirm_recovery(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.recover(payload or {}, **kw)

    # 检查点 / 偏移
    def domain_next_offset(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        p = dict(payload or {})
        p.update(kw)
        scope = p.get("table") or p.get("scope") or p.get("target_table")
        return self.engine.next_offset(scope=scope, checkpoint=p.get("checkpoint") or p.get("offset"))

    def domain_sync_checkpoint(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        p = dict(payload or {})
        p.update(kw)
        scope = p.get("table") or p.get("scope") or p.get("target_table")
        offset = p.get("offset") or p.get("checkpoint") or p.get("confirmed_offset")
        if scope is not None and offset is not None:
            self.engine.set_checkpoint(scope, offset)
        return self.engine.next_offset(scope=scope)

    # 造数 / 运维
    def domain_register_commit(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return {"ok": True, **self.engine.register_commit(payload or {}, **kw)}

    def domain_seed_scenario(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        return self.engine.setup_scenario(payload, **kw)

    def domain_list_recoveries(self, payload: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
        p = dict(payload or {})
        return self.engine.list_recoveries(p.get("table") or p.get("target_table"))

    def domain_reset(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.engine.reset()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        from app.replay_compat import classify, make_domain_call

        if (name.startswith("domain") or not name[0].isupper()) and classify(name) != "unknown":
            return make_domain_call(name)
        raise AttributeError(name)


def __getattr__(name: str):
    if name.startswith("_"):
        raise AttributeError(name)
    from app.replay_compat import classify, make_domain_call

    if classify(name) != "unknown":
        return make_domain_call(name)
    raise AttributeError(name)


SERVICE = DeltaReplayService()
service = SERVICE
