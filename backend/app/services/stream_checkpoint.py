"""流计算检查点领域服务：保存检查点，并同步读取检查点的“实际确认偏移”，
为恢复待办提供下一段真正需要重读的位置。"""
from __future__ import annotations

from typing import Any

from app.replay_engine import engine
from app.services.base import BaseModuleService
from app.store import store


class StreamCheckpointService(BaseModuleService):
    MODULE = "stream_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["检查点标识", "关联管道", "检查点时间", "数据偏移", "状态大小", "保存路径", "保留时长", "检查状态"]
    REQUIRED_FIELDS = ["检查点标识", "关联管道", "检查点时间"]
    STATUS_ORDER = ["保存中", "已保存", "已恢复", "已过期"]
    ACTION_RULES = {"创建检查点": "保存中", "恢复检查点": "已恢复"}
    NEGATIVE_ACTIONS = []

    def domain_save_checkpoint(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行写入，联动关联和回填处理，产出标准化记录。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"检查点 {entry_id} 不存在"}
        import hashlib

        offset = engine._checkpoint_rows_from_store(str(entry.get("关联管道", "default")))
        return {
            "ok": True,
            "checkpoint_id": entry.get("检查点标识"),
            "pipeline": entry.get("关联管道"),
            "checkpoint_time": entry.get("检查点时间"),
            "data_offset": entry.get("数据偏移"),
            "data_offset_int": offset[-1] if offset else None,
            "state_size_kb": int(entry.get("状态大小", 128)),
            "save_path": entry.get("保存路径", "/checkpoints"),
            "retention_hours": 24,
            "checksum": hashlib.sha256(str(entry).encode()).hexdigest()[:16],
        }

    # ------------------------------------------------------- 实际确认 / 下一读
    def domain_read_actual_confirmation(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        p = payload if isinstance(payload, dict) else {}
        scope = (
            kwargs.get("scope") or kwargs.get("pipeline") or kwargs.get("table")
            or p.get("scope") or p.get("pipeline") or p.get("table") or p.get("关联管道")
            or payload if isinstance(payload, str) else None
        )
        value = engine.actual_checkpoint(str(scope) if scope is not None else "default")
        return {"ok": True, "scope": scope, "actual_confirmed_offset": value,
                "checkpoint": value, "confirmed_offset": value}

    def domain_next_offset(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            merged = dict(payload)
            merged.update(kwargs)
            scope = merged.get("scope") or merged.get("pipeline") or merged.get("table")
            checkpoint = merged.get("checkpoint") or merged.get("offset")
            return engine.next_offset(scope=scope, checkpoint=checkpoint)
        return engine.next_offset(scope=payload, **kwargs)

    domain_next_read_offset = domain_next_offset
    domain_next_reread_offset = domain_next_offset
    domain_get_next_offset = domain_next_offset
    domain_sync_confirmation = domain_read_actual_confirmation

    def domain_confirm_offset(self, scope: Any = None, offset: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(scope, dict):
            offset = scope.get("offset") or scope.get("checkpoint")
            scope = scope.get("scope") or scope.get("pipeline") or scope.get("table")
        value = engine.set_checkpoint(scope, offset, **kwargs)
        return engine.next_offset(scope=scope, checkpoint=value)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        from app.replay_compat import classify, make_domain_call

        if name.startswith("domain") and classify(name) in {"next", "checkpoint", "recover", "survey"}:
            return make_domain_call(name)
        raise AttributeError(name)
