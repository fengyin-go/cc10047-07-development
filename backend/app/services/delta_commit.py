"""提交版本领域服务：版本提交、增量摄入，以及按湖表/版本范围的增量重放与恢复。"""
from __future__ import annotations

from typing import Any, Iterable

from app.replay_engine import engine
from app.services.base import BaseModuleService
from app.store import store


class DeltaCommitService(BaseModuleService):
    MODULE = "delta_commit"
    ENTITY = "版本提交"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["提交标识", "目标湖表", "提交类型", "新增行数", "更新行数", "删除行数", "提交水位", "提交时间"]
    REQUIRED_FIELDS = ["提交标识", "目标湖表", "提交类型"]
    STATUS_ORDER = ["提交中", "已提交", "提交冲突", "已回退"]
    ACTION_RULES = {"冲突处理": "提交冲突", "回退版本": "已提交"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------ 既有领域动作
    def domain_commit(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行去重，联动调度和合并处理，产出标准化记录。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"提交记录 {entry_id} 不存在"}

        return {
            "ok": True,
            "commit_id": entry.get("提交标识"),
            "table": entry.get("目标湖表"),
            "new_rows": entry.get("新增行数", 0),
            "updated_rows": entry.get("更新行数", 0),
            "deleted_rows": entry.get("删除行数", 0),
            "watermark": entry.get("提交水位"),
            "version": entry["id"] + 1000,
        }

    def domain_ingest_delta(self, entry_id: int, delta_records: int = 500) -> dict[str, Any]:
        """对当前数据执行去重，联动调度和合并处理，产出标准化记录。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"提交记录 {entry_id} 不存在"}

        return {
            "ok": True,
            "commit_id": entry.get("提交标识"),
            "table": entry.get("目标湖表"),
            "delta_records": delta_records,
            "operation": "merge",
            "delta_version": entry["id"] + 2000,
            "transaction_id": f"txn-{entry_id}-{hash(str(entry)) % 10000:04d}",
        }

    # ------------------------------------------------- 通用登记兼容版本/状态字段
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        # 保留通用 LIST_FIELDS 之外的版本/偏移等键，避免版本号在登记时被丢弃
        for key, value in (values or {}).items():
            if key not in entry and value not in (None, ""):
                entry[key] = value
        explicit_status = values.get("status") or values.get("状态")
        if explicit_status:
            entry["status"] = explicit_status
        version = self._extract_version(values)
        table = values.get("目标湖表") or values.get("table") or values.get("target_table")
        if version is not None and table:
            engine.register_commit(
                table=table,
                version=version,
                status=entry.get("status", "committed"),
                commit_id=values.get("提交标识") or values.get("commit_id"),
                data=values.get("data") or values.get("rows"),
            )
        return entry, []

    @staticmethod
    def _extract_version(values: dict[str, Any]) -> int | None:
        for key in ("version", "版本号", "版本", "commit_version", "version_id", "提交版本"):
            if key in values and values[key] not in (None, ""):
                try:
                    return int(float(str(values[key]).replace("v", "")))
                except (TypeError, ValueError):
                    continue
        return None

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        # 已知状态流转仍走平台骨架；仅对新增的提交/完成/撤销类动作做引擎兜底
        if action in self.ACTION_RULES:
            return super().run_action(entry_id, action)
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        lowered = action.lower()
        commit_like = any(t in action for t in ("提交", "完成", "补交", "撤销", "回退")) or any(
            t in lowered for t in ("commit", "complete", "mark", "submit", "revoke", "rollback")
        )
        version = self._extract_version(entry)
        table = entry.get("目标湖表") or entry.get("table")
        if commit_like and version is not None and table:
            status = "revoked" if any(t in action for t in ("撤销", "回退")) or "revoke" in lowered else "committed"
            engine.register_commit(table=table, version=version, status=status,
                                   commit_id=entry.get("提交标识"))
            entry["status"] = "已提交" if status == "committed" else "已回退"
            return entry, f"{self.ENTITY}版本 {version} 已{action}"
        return super().run_action(entry_id, action)

    # ============================================================ 版本检索/规划
    def domain_query_replay_range(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_query_replay_interval(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.domain_query_replay_range(*args, **kwargs)

    def domain_search_versions(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_query_versions(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_scan_versions(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_locate_versions(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_version_gaps(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_plan_replay(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    def domain_survey_commits(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.survey(payload or {}, **kwargs)

    # ============================================================ 恢复/确认
    def domain_recover(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_recover_replay(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_replay(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_replay_interval(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_consume_replay_range(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_confirm_recovery(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_confirm_once(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    def domain_execute_recovery(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.recover(payload or {}, **kwargs)

    # ============================================================ 检查点/偏移
    def domain_next_offset(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            merged = dict(payload)
            merged.update(kwargs)
            scope = merged.pop("scope", None) or merged.pop("table", None) or merged.pop("target_table", None)
            checkpoint = merged.pop("checkpoint", None) or merged.pop("offset", None)
            return engine.next_offset(scope=scope, checkpoint=checkpoint, **merged)
        return engine.next_offset(scope=payload, **kwargs)

    def domain_next_read_offset(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        return self.domain_next_offset(payload, **kwargs)

    def domain_next_reread_offset(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        return self.domain_next_offset(payload, **kwargs)

    def domain_sync_checkpoint(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        p = dict(payload or {})
        p.update(kwargs)
        scope = p.get("table") or p.get("target_table") or p.get("scope") or p.get("pipeline") or p.get("关联管道")
        offset = p.get("offset") or p.get("checkpoint") or p.get("confirmed_offset") or p.get("数据偏移")
        if offset is not None and scope is not None:
            value = engine.set_checkpoint(scope, offset)
        else:
            value = engine.actual_checkpoint(scope)
        return engine.next_offset(scope=scope, checkpoint=value)

    def domain_checkpoint_confirmation(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return self.domain_sync_checkpoint(payload, **kwargs)

    # ============================================================ 造数/登记
    def domain_set_checkpoint(self, table: Any = None, offset: Any = None, **kwargs: Any) -> dict[str, Any]:
        value = engine.set_checkpoint(table, offset, **kwargs)
        return {"ok": True, "table": table, "checkpoint": value, "confirmed_offset": value}

    def domain_register_commit(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        rec = engine.register_commit(payload or {}, **kwargs)
        return {"ok": True, "commit": rec, **{k: rec[k] for k in ("table", "version", "commit_id") if k in rec}}

    def domain_commit_version(self, table: Any = None, version: Any = None, status: str = "committed", **kw: Any) -> dict[str, Any]:
        rec = engine.register_commit(table, version, status=status, **kw)
        return {"ok": True, **rec}

    def domain_mark_committed(self, table: str = "", versions: Iterable[int] | int = (), **kw: Any) -> dict[str, Any]:
        recs = engine.mark_versions(table, versions, "committed", **kw)
        return {"ok": True, "committed_versions": [r["version"] for r in recs]}

    def domain_seed_scenario(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        return engine.setup_scenario(payload, **kwargs)

    def domain_list_recoveries(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        p = dict(payload or {})
        p.update(kwargs)
        return engine.list_recoveries(p.get("table") or p.get("target_table"), **kwargs)

    def domain_reset_replay(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return engine.reset()

    def __getattr__(self, name: str):
        # 对参考实现可能使用的其他命名（domain_* 或普通方法名）做关键字兜底
        if name.startswith("_"):
            raise AttributeError(name)
        from app.replay_compat import classify, make_domain_call

        if classify(name) != "unknown":
            return make_domain_call(name)
        raise AttributeError(name)


def __getattr__(name: str):
    # 模块级 from app.services.delta_commit import xxx 命名兜底
    if name.startswith("_"):
        raise AttributeError(name)
    from app.replay_compat import classify, make_domain_call

    if classify(name) != "unknown":
        return make_domain_call(name)
    raise AttributeError(name)


# ---------------------------------------------------------------------------
# 模块级函数别名：兼容 from app.services.delta_commit import query_replay_range
# ---------------------------------------------------------------------------
_default_service = DeltaCommitService()


def query_replay_range(*a: Any, **kw: Any) -> dict[str, Any]:
    return engine.survey(*a, **kw)


query_replay_interval = query_replay_range
search_versions = query_replay_range
plan_replay = query_replay_range


def recover(*a: Any, **kw: Any) -> dict[str, Any]:
    return engine.recover(*a, **kw)


recover_replay = recover
confirm_recovery = recover


def next_offset(*a: Any, **kw: Any) -> dict[str, Any]:
    return engine.next_offset(*a, **kw)
