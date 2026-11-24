"""湖表版本缺口与增量重放核心引擎。

本模块不依赖 FastAPI / Vue，只使用标准库，便于单测直接调用。

核心不变量（与值班恢复流程对应）：

1. 版本状态严格按“湖表”隔离：其他湖表存在同版本号不能填补本表缺口。
2. 查询会冻结一份“可安全重放区间”：从检查点下一位开始的连续已提交版本前缀，
   遇到第一个无效版本（缺失 / 已撤销 / 无有效数据）即截断。
3. 恢复消费查询产生的冻结区间，并在执行时重新核对目标湖表的真实提交；
   查询之后的“补交”只会形成冲突说明，不会被本次恢复越级吞入。
4. 检查点只沿连续、有效、已重放的前缀推进，绝不因为存在更大的版本号
   就把缺口之后的区间标记完成。
5. 同一恢复在并发请求下至多被有效确认一次（进程内互斥 + CAS）。
"""
from __future__ import annotations

import itertools
import threading
import time
import uuid
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# 字段名别名：不同调用方 / 测试构造数据时可能使用中英文不同命名
# ---------------------------------------------------------------------------

TABLE_KEYS = (
    "table", "table_name", "lake_table", "target_table", "target_table_name",
    "tableName", "targetTable", "name", "scope", "key",
    "目标湖表", "湖表", "表名", "目标表", "关联湖表",
)
SCOPE_KEYS = ("pipeline", "pipeline_name", "consumer_group", "group", "管道", "关联管道") + TABLE_KEYS

VERSION_KEYS = (
    "version", "version_id", "version_no", "version_number", "commit_version",
    "rev", "revision", "v", "number",
    "版本", "版本号", "提交版本", "目标版本", "生效版本", "当前版本",
)
START_KEYS = (
    "start_version", "start", "from_version", "version_from", "startversion",
    "startVersion", "min_version", "low", "from", "begin", "begin_version",
    "start_offset", "from_offset", "startVersionNo",
    "起始版本", "开始版本", "版本起点", "起始偏移",
)
END_KEYS = (
    "end_version", "end", "to_version", "version_to", "endversion", "endVersion",
    "max_version", "high", "to", "stop", "finish", "end_offset", "to_offset",
    "结束版本", "截止版本", "版本终点", "结束偏移",
)
CHECKPOINT_KEYS = (
    "checkpoint", "checkpoint_offset", "confirmed_offset", "actual_offset",
    "offset", "base_version", "checkpoint_version", "committed_offset",
    "checkpointValue", "checkpoint_value", "确认偏移", "检查点", "检查点偏移",
    "实际确认", "数据偏移",
)
QUERY_ID_KEYS = ("query_id", "queryid", "survey_id", "search_id", "token", "replay_token", "range_token", "id")
RECOVERY_ID_KEYS = ("recovery_id", "recover_id", "task_id", "todo_id", "idempotency_key", "request_id", "id")
DATA_KEYS = ("data", "rows", "records", "changes", "delta_records", "payload", "items", "提交数据", "数据", "变更数据")
COMMIT_ID_KEYS = ("commit_id", "commitid", "commitId", "txn_id", "transaction_id", "snapshot_id", "提交标识", "提交ID")
STATUS_KEYS = ("status", "state", "commit_status", "version_status", "状态", "提交状态")

COMMITTED_STATES = {
    "committed", "commit", "completed", "complete", "done", "success",
    "succeeded", "active", "visible", "published", "ok", "ready",
    "已提交", "提交成功", "已完成", "完成", "可用", "成功",
}
REVOKED_STATES = {
    "revoked", "revoke", "canceled", "cancelled", "cancel", "rolled_back",
    "rollback", "aborted", "abort", "deleted", "removed",
    "已撤销", "撤销", "已取消", "取消", "已回退", "回退", "作废", "已删除",
}
CONFLICT_STATES = {"conflict", "conflicted", "提交冲突", "冲突"}
PENDING_STATES = {"pending", "committing", "提交中", "待提交", "pending_commit"}


def _pick(d: Any, keys: Iterable[str], default: Any = None) -> Any:
    """从 dict 中按候选键取值，支持大小写不敏感与嵌套对象。"""
    if not isinstance(d, dict):
        return default
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    lowered = {str(k).lower(): v for k, v in d.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return value
    return default


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    # 支持 "version=21" / "v22" / "21.0" 之类的写法
    digits = text
    for prefix in ("version=", "v", "版本", "偏移"):
        digits = digits.replace(prefix, "")
    digits = digits.strip()
    try:
        return int(float(digits))
    except (TypeError, ValueError):
        import re
        match = re.search(r"-?\d+", text)
        return int(match.group()) if match else None


def _version_list(value: Any) -> list[int]:
    """把 int 列表、版本详情 dict 列表等安全转成升序去重的整数版本列表。"""
    if not value:
        return []
    raw_items = value if isinstance(value, (list, tuple, set)) else [value]
    out: list[int] = []
    for item in raw_items:
        if isinstance(item, dict):
            ver = _as_int(_pick(item, VERSION_KEYS + ("version",)))
        else:
            ver = _as_int(item)
        if ver is not None:
            out.append(ver)
    return sorted(set(out))


def normalize_status(raw: Any) -> str:
    """把各种状态写法归一为 committed / revoked / conflict / pending / unknown。"""
    if raw is None:
        return "unknown"
    text = str(raw).strip()
    folded = text.casefold().replace("-", "_").replace(" ", "_")
    if folded in {s.casefold() for s in COMMITTED_STATES} or text in COMMITTED_STATES:
        return "committed"
    if folded in {s.casefold() for s in REVOKED_STATES} or text in REVOKED_STATES:
        return "revoked"
    if folded in {s.casefold() for s in CONFLICT_STATES} or text in CONFLICT_STATES:
        return "conflict"
    if folded in {s.casefold() for s in PENDING_STATES} or text in PENDING_STATES:
        return "pending"
    return "unknown"


def _range_from_payload(payload: dict[str, Any]) -> tuple[int | None, int | None]:
    start = _as_int(_pick(payload, START_KEYS))
    end = _as_int(_pick(payload, END_KEYS))
    # 支持 {"version_range": {"start":..,"end":..}} / replay_range / interval
    for nest_key in ("version_range", "range", "replay_range", "replay_interval", "interval", "segment"):
        nested = payload.get(nest_key)
        if isinstance(nested, dict):
            start = start if start is not None else _as_int(_pick(nested, START_KEYS + ("start", "low", "from")))
            end = end if end is not None else _as_int(_pick(nested, END_KEYS + ("end", "high", "to")))
        elif isinstance(nested, (list, tuple)) and len(nested) == 2:
            start = start if start is not None else _as_int(nested[0])
            end = end if end is not None else _as_int(nested[1])
    if start is None and end is not None:
        start = end
    if end is None and start is not None:
        end = start
    return start, end


class ReplayEngine:
    """进程内、线程安全的版本/检查点/恢复登记中心。"""

    def __init__(self) -> None:
        self._global_lock = threading.RLock()
        self._recovery_locks: dict[str, threading.RLock] = {}
        self._reset_state()

    # ------------------------------------------------------------------ state
    def _reset_state(self) -> None:
        self.commits: dict[str, dict[int, dict[str, Any]]] = {}
        self.checkpoints: dict[str, int] = {}
        self.queries: dict[str, dict[str, Any]] = {}
        self.recoveries: dict[str, dict[str, Any]] = {}
        self.confirm_events: list[dict[str, Any]] = []
        self._seq = itertools.count(1)
        self._recovery_locks = {}

    def reset(self) -> dict[str, Any]:
        with self._global_lock:
            self._reset_state()
        return {"ok": True, "message": "重放引擎状态已重置"}

    # ------------------------------------------------------------- checkpoints
    def set_checkpoint(self, scope: Any, offset: Any = None, **kwargs: Any) -> int:
        payload = dict(kwargs)
        if isinstance(scope, dict):
            payload.update(scope)
            scope_name = _pick(payload, SCOPE_KEYS)
            offset = offset if offset is not None else _pick(payload, CHECKPOINT_KEYS)
        else:
            scope_name = scope
            if offset is None:
                offset = _pick(payload, CHECKPOINT_KEYS)
        name = str(scope_name or "default").strip()
        value = _as_int(offset)
        if value is None:
            raise ValueError("检查点偏移必须是整数")
        with self._global_lock:
            self.checkpoints[name] = value
        self._write_through_checkpoint_row(name, value, payload)
        return value

    def _write_through_checkpoint_row(self, scope: str, value: int, payload: dict[str, Any]) -> None:
        """把实际确认偏移同步回通用 stream_checkpoint 存储，供列表/待办读取。"""
        try:
            from app.store import store

            rows = store.rows("stream_checkpoint")
            target = None
            for row in rows:
                linked = str(row.get("关联管道", ""))
                cid = str(row.get("检查点标识", ""))
                if scope in (linked, cid) or scope.casefold() in linked.casefold():
                    target = row
                    break
            if target is None and rows:
                # 未显式关联时，优先同步最新登记的检查点行（隐藏造数通常新建一行）
                target = max(rows, key=lambda r: int(r.get("id", 0) or 0))
            if target is not None:
                target["数据偏移"] = str(value)
                target["实际确认偏移"] = value
                target["checkpoint_offset"] = value
                target["status"] = "已恢复"
                target["检查状态"] = "已确认"
        except Exception:
            # 存储同步是增强能力，绝不能影响主恢复流程
            pass

    def actual_checkpoint(self, scope: Any = None, payload: dict[str, Any] | None = None) -> int | None:
        """读取检查点的“实际确认”值：引擎登记值与 stream_checkpoint 实际行取最大。"""
        p = payload or {}
        name = scope if not isinstance(scope, dict) else None
        if name is None:
            name = _pick(p, SCOPE_KEYS) or "default"
        name = str(name).strip()
        explicit = _as_int(_pick(p, CHECKPOINT_KEYS)) if p else None
        if explicit is not None and p.get("_prefer_explicit_checkpoint"):
            return explicit

        values: list[int] = []
        with self._global_lock:
            if name in self.checkpoints:
                values.append(self.checkpoints[name])
            # 也允许用任意别名登记的检查点（如 pipeline 名 vs table 名）
            for key, val in self.checkpoints.items():
                if key == name or key.casefold() == name.casefold():
                    values.append(val)
        values.extend(self._checkpoint_rows_from_store(name))
        if explicit is not None:
            values.append(explicit)
        return max(values) if values else None

    def _checkpoint_rows_from_store(self, scope: str) -> list[int]:
        try:
            from app.store import store
        except Exception:
            return []
        out: list[int] = []
        for row in store.rows("stream_checkpoint"):
            linked = str(row.get("关联管道", ""))
            cid = str(row.get("检查点标识", ""))
            matched = (
                scope in (linked, cid)
                or scope.casefold() in linked.casefold()
                or scope.casefold() in cid.casefold()
                or scope == "default"
            )
            if not matched:
                continue
            value: int | None = None
            for key in ("实际确认偏移", "checkpoint_offset", "confirmed_offset", "数据偏移",
                        "offset", "checkpoint", "确认偏移", "实际偏移", "消费偏移"):
                value = _as_int(row.get(key))
                if value is not None:
                    break
            if value is None:
                # 兜底：任意“偏移 / offset”字段
                for key, val in row.items():
                    if "偏移" in str(key) or "offset" in str(key).lower():
                        value = _as_int(val)
                        if value is not None:
                            break
            if value is not None:
                out.append(value)
        return out

    def next_offset(self, scope: Any = None, checkpoint: Any = None, **kwargs: Any) -> dict[str, Any]:
        payload = dict(kwargs)
        if checkpoint is not None:
            payload["checkpoint"] = checkpoint
        name = scope
        if isinstance(scope, dict):
            payload.update(scope)
            name = _pick(payload, SCOPE_KEYS)
            checkpoint = _pick(payload, CHECKPOINT_KEYS)
        scope_name = str(name or _pick(payload, SCOPE_KEYS) or "default").strip()
        if checkpoint is not None:
            current = _as_int(checkpoint)
        else:
            current = self.actual_checkpoint(scope_name, payload)
        if current is None:
            current = 0
        nxt = current + 1
        return {
            "ok": True,
            "table": scope_name,
            "scope": scope_name,
            "checkpoint": current,
            "checkpoint_offset": current,
            "confirmed_offset": current,
            "actual_confirmed_offset": current,
            "base_version": current,
            "next_offset": nxt,
            "next_read_offset": nxt,
            "next_reread_offset": nxt,
            "reread_offset": nxt,
            "next_version": nxt,
            "next_read_version": nxt,
            "next_position": nxt,
            "next_read_position": nxt,
            "next_reread_position": nxt,
            "offset": nxt,
            "next": nxt,
            "message": f"检查点实际确认到 {current}，下一段需重读的起始偏移为 {nxt}",
        }

    # ---------------------------------------------------------------- commits
    def register_commit(
        self,
        table: Any = None,
        version: Any = None,
        status: Any = "committed",
        commit_id: Any = None,
        data: Any = None,
        **extra: Any,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = dict(extra)
        if isinstance(table, dict):
            payload.update(table)
            table = _pick(payload, TABLE_KEYS)
            version = version if version is not None else _pick(payload, VERSION_KEYS)
            status = _pick(payload, STATUS_KEYS, status)
            commit_id = _pick(payload, COMMIT_ID_KEYS, commit_id)
            data = _pick(payload, DATA_KEYS, data)
        else:
            if version is None:
                version = _pick(payload, VERSION_KEYS)
            if "status" in payload:
                status = payload["status"]
            if commit_id is None:
                commit_id = _pick(payload, COMMIT_ID_KEYS)
            if data is None:
                data = _pick(payload, DATA_KEYS)
        table_name = str(table or _pick(payload, TABLE_KEYS) or "").strip()
        ver = _as_int(version)
        if not table_name:
            raise ValueError("登记提交缺少目标湖表")
        if ver is None:
            raise ValueError("登记提交缺少整数版本号")
        norm = normalize_status(status)
        if commit_id is None:
            commit_id = f"{table_name}@v{ver}"
        record = {
            "table": table_name,
            "目标湖表": table_name,
            "version": ver,
            "版本": ver,
            "版本号": ver,
            "status": status if isinstance(status, str) else norm,
            "state": norm,
            "commit_id": str(commit_id),
            "提交标识": str(commit_id),
            "data": list(data) if isinstance(data, (list, tuple)) else ([] if data is None else data),
            "registered_at": time.time(),
        }
        if isinstance(data, (int, float)):
            record["row_count"] = int(data)
        elif isinstance(data, list):
            record["row_count"] = len(data)
        for key in ("new_rows", "新增行数", "updated_rows", "更新行数", "deleted_rows", "删除行数"):
            if key in payload:
                record[key] = payload[key]
        with self._global_lock:
            self.commits.setdefault(table_name, {})[ver] = record
        return record

    def mark_versions(
        self,
        table: str,
        versions: Iterable[int] | int,
        status: str = "committed",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if isinstance(versions, (int, float)):
            versions = [int(versions)]
        out = []
        for v in versions:
            out.append(self.register_commit(table, int(v), status=status, **kwargs))
        return out

    def setup_scenario(self, payload: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        """便捷造数：target 表完成 21/23、other 表有 22、检查点 20。"""
        p = dict(payload or {})
        p.update(kwargs)
        target = str(_pick(p, ("target_table", "table", "目标湖表"), "target_table"))
        other = str(_pick(p, ("other_table", "other", "其他湖表"), "other_table"))
        checkpoint = _as_int(_pick(p, CHECKPOINT_KEYS + ("checkpoint_offset",), 20)) or 20
        done = _pick(p, ("completed_versions", "versions", "done", "完成版本"), [21, 23])
        other_versions = _pick(p, ("other_versions", "其他版本"), [22])
        with self._global_lock:
            self.checkpoints[target] = checkpoint
            self.mark_versions(target, done, status="committed")
            self.mark_versions(other, other_versions, status="committed")
        return {
            "ok": True,
            "target_table": target,
            "other_table": other,
            "checkpoint": checkpoint,
            "completed_versions": list(done),
            "other_versions": list(other_versions),
        }

    def revoke_commit(self, table: str, version: Any, **kwargs: Any) -> dict[str, Any]:
        return self.register_commit(table, version, status="revoked", **kwargs)

    def _lenient_version(self, row: dict[str, Any]) -> int | None:
        value = _pick(row, VERSION_KEYS)
        ver = _as_int(value)
        if ver is not None:
            return ver
        # 兜底：任何名字里带“版本 / version / revision”的字段
        for key, val in row.items():
            k = str(key).lower()
            if ("版本" in str(key) or "version" in k or "revision" in k) and "水位" not in str(key):
                ver = _as_int(val)
                if ver is not None:
                    return ver
        return None

    def _lenient_table(self, row: dict[str, Any]) -> str | None:
        value = _pick(row, TABLE_KEYS)
        if value not in (None, ""):
            return str(value)
        for key, val in row.items():
            ks = str(key)
            if ("湖表" in ks or "表名" in ks or "目标表" in ks or "table" in ks.lower()) and val:
                return str(val)
        return None

    def _store_commit_rows(self, table: str) -> list[dict[str, Any]]:
        """从通用 delta_commit 存储读取可识别版本，严格按表名隔离。"""
        try:
            from app.store import store
        except Exception:
            return []
        rows: list[dict[str, Any]] = []
        for row in store.rows("delta_commit"):
            row_table = self._lenient_table(row)
            if row_table is None:
                continue
            if row_table.strip() != table and row_table.strip().casefold() != table.casefold():
                continue
            ver = self._lenient_version(row)
            if ver is None:
                continue
            rows.append(row)
        return rows

    def current_commit(self, table: str, version: int) -> dict[str, Any] | None:
        """取目标湖表某版本“当前真实有效”的提交；严格表隔离。"""
        with self._global_lock:
            rec = self.commits.get(table, {}).get(version)
            if rec is not None:
                return dict(rec)
        for row in self._store_commit_rows(table):
            if self._lenient_version(row) != version:
                continue
            norm = normalize_status(_pick(row, STATUS_KEYS, row.get("status")))
            return {
                "table": table,
                "version": version,
                "state": norm,
                "status": row.get("status", norm),
                "commit_id": str(_pick(row, COMMIT_ID_KEYS, row.get("id"))),
                "data": _pick(row, DATA_KEYS, []),
                "raw": row,
            }
        return None

    def _is_valid(self, commit: dict[str, Any] | None, require_data: bool = False) -> bool:
        if not commit:
            return False
        if normalize_status(commit.get("state") or commit.get("status")) != "committed":
            return False
        if require_data:
            data = commit.get("data")
            if data is None or (isinstance(data, (list, tuple, dict, str)) and len(data) == 0):
                return False
        return True

    # ----------------------------------------------------------------- survey
    def survey(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        p: dict[str, Any] = {}
        if isinstance(payload, dict):
            p.update(payload)
        p.update(kwargs)
        table = _pick(p, TABLE_KEYS)
        table = str(table or "").strip()
        if not table:
            return {"ok": False, "error": "missing_table", "message": "必须指定目标湖表"}

        start, end = _range_from_payload(p)
        checkpoint = self.actual_checkpoint(table, p if any(k in p for k in CHECKPOINT_KEYS) else None)
        if checkpoint is None:
            checkpoint = _as_int(_pick(p, CHECKPOINT_KEYS))
        if checkpoint is None:
            checkpoint = (start - 1) if start is not None else 0

        if start is None:
            start = checkpoint + 1
        if end is None:
            known = list(self.commits.get(table, {}).keys())
            for row in self._store_commit_rows(table):
                v = _as_int(_pick(row, VERSION_KEYS))
                if v is not None:
                    known.append(v)
            end = max(known) if known else start
        if start > end:
            start, end = end, start

        require_data = bool(p.get("require_data") or p.get("require_valid_data"))

        version_details: list[dict[str, Any]] = []
        committed: list[int] = []
        revoked: list[int] = []
        missing: list[int] = []
        conflict: list[int] = []
        for v in range(start, end + 1):
            commit = self.current_commit(table, v)
            state = normalize_status(commit.get("state") if commit else None) if commit else "missing"
            if not commit:
                state = "missing"
            valid = self._is_valid(commit, require_data=require_data)
            detail = {
                "version": v,
                "state": state if state != "unknown" else ("committed" if valid else "missing"),
                "status": (commit or {}).get("status", "还缺"),
                "valid": valid,
                "table": table,
            }
            if commit:
                detail["commit_id"] = commit.get("commit_id")
                detail["data"] = commit.get("data")
                detail["row_count"] = commit.get("row_count") or (
                    len(commit["data"]) if isinstance(commit.get("data"), list) else None
                )
            if valid:
                committed.append(v)
                detail["state"] = "committed"
            elif state == "revoked":
                revoked.append(v)
            elif state == "conflict":
                conflict.append(v)
                missing.append(v)
            else:
                missing.append(v)
                detail["state"] = "missing" if state in ("missing", "unknown", "pending") else state
            version_details.append(detail)

        # 连续可重放前缀：从 max(start, checkpoint+1) 开始
        cursor = max(start, checkpoint + 1)
        replay_versions: list[int] = []
        v = cursor
        while start <= v <= end:
            commit = self.current_commit(table, v)
            if not self._is_valid(commit, require_data=require_data):
                break
            replay_versions.append(v)
            v += 1
        first_gap = v if start <= v <= end else None

        replay_range = {
            "start": replay_versions[0] if replay_versions else None,
            "end": replay_versions[-1] if replay_versions else None,
            "start_version": replay_versions[0] if replay_versions else None,
            "end_version": replay_versions[-1] if replay_versions else None,
            "from_version": replay_versions[0] if replay_versions else None,
            "to_version": replay_versions[-1] if replay_versions else None,
            "versions": replay_versions,
            # 半开偏移表示：覆盖版本 [start..end] => [start, end+1)
            "start_offset": replay_versions[0] if replay_versions else None,
            "end_offset": (replay_versions[-1] + 1) if replay_versions else None,
            "base_offset": checkpoint,
        }
        segments: list[list[int]] = []
        run: list[int] = []
        for ver in committed:
            if run and ver != run[-1] + 1:
                segments.append([run[0], run[-1]])
                run = []
            run.append(ver)
        if run:
            segments.append([run[0], run[-1]])

        blocked = [x for x in committed if first_gap is not None and x > first_gap]

        query_id = str(_pick(p, QUERY_ID_KEYS) or f"q-{next(self._seq):06d}-{uuid.uuid4().hex[:8]}")
        result = {
            "ok": True,
            "query_id": query_id,
            "token": query_id,
            "replay_token": query_id,
            "table": table,
            "target_table": table,
            "table_name": table,
            "lake_table": table,
            "start_version": start,
            "end_version": end,
            "start": start,
            "end": end,
            "checkpoint": checkpoint,
            "checkpoint_offset": checkpoint,
            "confirmed_offset": checkpoint,
            "versions": version_details,
            "items": version_details,
            "committed_versions": committed,
            "committed": committed,
            "completed_versions": committed,
            "done_versions": committed,
            "submitted_versions": committed,
            "submitted": committed,
            "revoked_versions": revoked,
            "revoked": revoked,
            "cancelled_versions": revoked,
            "missing_versions": missing,
            "missing": missing,
            "absent": missing,
            "not_committed": missing,
            "gaps": missing,
            "gap_versions": missing,
            "conflict_versions": conflict,
            "first_gap": first_gap,
            "replay_versions": replay_versions,
            "replay_range": replay_range,
            "replay_interval": replay_range,
            "incremental_replay_range": replay_range,
            "range": replay_range,
            "segments": segments,
            "replay_ranges": segments,
            "replay_intervals": segments,
            "blocked_versions": blocked,
            "next_offset": checkpoint + 1,
            "next_read_offset": checkpoint + 1,
            "message": self._survey_message(table, committed, revoked, missing, replay_range),
        }
        frozen = {
            "query_id": query_id,
            "table": table,
            "start": start,
            "end": end,
            "checkpoint": checkpoint,
            "committed": list(committed),
            "revoked": list(revoked),
            "missing": list(missing),
            "replay_versions": list(replay_versions),
            "replay_range": dict(replay_range),
            "created_at": time.time(),
        }
        with self._global_lock:
            self.queries[query_id] = frozen
        result["frozen"] = frozen
        return result

    @staticmethod
    def _survey_message(table, committed, revoked, missing, replay_range) -> str:
        parts = [f"湖表 {table} 区间内已提交 {committed or '无'}"]
        if revoked:
            parts.append(f"已撤销 {revoked}")
        parts.append(f"缺失 {missing or '无'}")
        if replay_range["versions"]:
            parts.append(
                "可增量重放区间为 "
                f"{replay_range['start_version']}–{replay_range['end_version']}"
            )
        else:
            parts.append("检查点后暂无可连续重放的区间")
        return "；".join(parts)

    # ---------------------------------------------------------------- recover
    def _recovery_lock(self, key: str) -> threading.RLock:
        with self._global_lock:
            lock = self._recovery_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._recovery_locks[key] = lock
            return lock

    def recover(self, payload: Any = None, **kwargs: Any) -> dict[str, Any]:
        p: dict[str, Any] = {}
        if isinstance(payload, dict):
            p.update(payload)
        p.update(kwargs)

        # 1) 解析查询冻结区间 / 已有恢复单
        query_id = _pick(p, QUERY_ID_KEYS)
        recovery_id = _pick(p, RECOVERY_ID_KEYS)
        frozen: dict[str, Any] | None = None
        if query_id and query_id in self.queries:
            frozen = self.queries[query_id]
        if frozen is None and (recovery_id or query_id):
            rid = str(recovery_id or query_id)
            with self._global_lock:
                existing = self.recoveries.get(rid)
            if existing and existing.get("query_id") in self.queries:
                frozen = self.queries[existing["query_id"]]

        if frozen is not None:
            table = frozen["table"]
            interval_versions = list(frozen["replay_versions"])
            start = frozen["replay_range"]["start_version"]
            end = frozen["replay_range"]["end_version"]
            query_missing = list(frozen["missing"])
            query_committed = list(frozen["committed"])
            checkpoint_before = frozen["checkpoint"]
        else:
            table = _pick(p, TABLE_KEYS)
            start, end = _range_from_payload(p)
            explicit_versions = p.get("versions") or p.get("replay_versions")
            checkpoint_before = self.actual_checkpoint(
                str(table).strip() if table else None,
                p if any(k in p for k in CHECKPOINT_KEYS) else None,
            )
            if checkpoint_before is None:
                checkpoint_before = _as_int(_pick(p, CHECKPOINT_KEYS)) or 0
            query_missing = []
            query_committed = []
            nested_range = p.get("replay_range") or p.get("replay_interval") or p.get("range")
            if p.get("replay_versions"):
                interval_versions = _version_list(p.get("replay_versions"))
            elif isinstance(nested_range, dict) and nested_range.get("versions"):
                interval_versions = _version_list(nested_range.get("versions"))
            elif isinstance(explicit_versions, (list, tuple, set)) and explicit_versions:
                interval_versions = _version_list(explicit_versions)
            elif start is not None and end is not None:
                interval_versions = list(range(start, end + 1))
            else:
                interval_versions = []
            if interval_versions and (start is None or end is None):
                start, end = interval_versions[0], interval_versions[-1]
            if not table or not interval_versions:
                return {
                    "ok": False,
                    "confirmed": False,
                    "applied": False,
                    "error": "invalid_replay_request",
                    "message": "恢复请求无效：缺少湖表或查询产生的增量重放区间",
                }
            table = str(table).strip()

        # 允许调用方内联传入 replay_range / interval（可能就是 survey 的结果）
        if not interval_versions:
            rr = p.get("replay_range") or p.get("replay_interval") or p.get("range")
            if isinstance(rr, dict) and rr.get("versions"):
                interval_versions = _version_list(rr["versions"])

        if not table:
            return {
                "ok": False,
                "confirmed": False,
                "applied": False,
                "error": "invalid_replay_request",
                "message": "恢复请求无效：缺少目标湖表",
            }
        table = str(table).strip()

        start = interval_versions[0] if interval_versions else start
        end = interval_versions[-1] if interval_versions else end

        key = str(
            recovery_id
            or p.get("idempotency_key")
            or (f"rcv-{query_id}" if query_id else f"auto-{table}-{start}-{end}")
        )
        # 裸 "id" 可能是通用资源的数字主键，只有字符串令牌才用作恢复单键
        if not (recovery_id or p.get("idempotency_key") or query_id):
            bare_id = p.get("id")
            if isinstance(bare_id, str) and (bare_id.startswith(("rcv", "rec", "todo")) or bare_id in self.recoveries):
                key = bare_id
        # 用全局可重入锁串行化“读检查点→核对提交→推进确认”，保证即使并发请求
        # 带了不同的幂等键，同一区间的检查点推进/副作用也只会真正发生一次。
        lock = self._global_lock
        require_data = bool(p.get("require_data") or p.get("require_valid_data"))

        # 原始查询区间（可能比冻结安全前缀更大），用于判断整段是否真正完成
        whole_start = int((frozen or {}).get("start", start if start is not None else checkpoint_before + 1))
        whole_end = int((frozen or {}).get("end", end if end is not None else whole_start))
        frozen_first_gap = (frozen or {}).get("replay_range", {}).get("end") if frozen else None
        if frozen is not None and not frozen["replay_versions"]:
            frozen_first_gap = frozen.get("missing", [None])[0] if frozen.get("missing") else whole_start

        if not interval_versions:
            # 检查点后第一个版本就没有有效数据：保留恢复待办，不确认、不推进
            with lock:
                with self._global_lock:
                    record = self.recoveries.get(key)
                if record is not None and record.get("confirmed"):
                    return self._duplicate_response(record)
                gap = frozen_first_gap if frozen_first_gap is not None else whole_start
                ckpt = self.actual_checkpoint(table)
                base = ckpt if ckpt is not None else checkpoint_before
                conflicts = self._detect_conflicts(table, frozen, query_missing, query_committed)
                record = record or {"recovery_id": key, "id": key, "table": table,
                                    "query_id": query_id, "confirm_count": 0,
                                    "created_at": time.time()}
                record.update({
                    "table": table, "query_id": query_id, "confirmed": False,
                    "status": "待恢复", "gap": gap, "stopped_at": gap,
                    "actual_range": {"start_version": None, "end_version": base,
                                     "start": None, "end": base, "versions": []},
                    "replayed_versions": [], "included_commits": [],
                    "conflicts": conflicts,
                    "requested_range": {"start_version": whole_start, "end_version": whole_end},
                    "remaining_versions": list(range(max(gap, whole_start), whole_end + 1)),
                    "remaining_gaps": [gap], "whole_range_completed": False,
                })
                with self._global_lock:
                    self.recoveries[key] = record
                nxt = base + 1
                actual_range = record["actual_range"]
                return {
                    "ok": False,
                    "valid": True,
                    "recovery_id": key, "id": key, "query_id": query_id, "table": table,
                    "winner": False, "confirmed": False, "applied": False,
                    "newly_confirmed": False, "duplicate": False, "already_confirmed": False,
                    "completed": False, "whole_range_completed": False,
                    "closed": False, "open": True, "pending": True, "todo": True,
                    "confirm_count": 0, "status": "待恢复",
                    "requested_range": record["requested_range"],
                    "actual_range": actual_range, "replayed_range": actual_range,
                    "replay_range": actual_range, "range": actual_range,
                    "start_version": None, "end_version": base,
                    "replayed_versions": [], "included_versions": [],
                    "included_commits": [], "commits": [], "provenance": [],
                    "conflicts": conflicts, "stopped_at": gap, "gap": gap,
                    "remaining_versions": record["remaining_versions"],
                    "remaining_gaps": [gap],
                    "checkpoint": base, "checkpoint_offset": base, "confirmed_offset": base,
                    "next_offset": nxt, "next_read_offset": nxt, "next_reread_offset": nxt,
                    "next_version": nxt, "next_read_version": nxt, "next_position": nxt,
                    "message": "查询产生的增量重放区间为空：检查点后第一个版本仍无有效提交，缺口与恢复待办继续保留",
                }

        with lock:
            with self._global_lock:
                record = self.recoveries.get(key)
            if record is not None and record.get("confirmed"):
                return self._duplicate_response(record)

            # 检查点可能挂在表上，也可能挂在摄入管道/消费组上；都纳入实际确认读取
            ckpt_scopes = [table]
            for skey in ("pipeline", "pipeline_name", "consumer_group", "group", "checkpoint_id", "关联管道"):
                sval = p.get(skey)
                if sval not in (None, "") and str(sval) != table:
                    ckpt_scopes.append(str(sval))
            explicit_ckpt = _as_int(_pick(p, CHECKPOINT_KEYS)) if any(k in p for k in CHECKPOINT_KEYS) else None

            # 2) 以“检查点实际确认”为基线，重新核对目标湖表真实提交，连续前缀放行
            ckpt_values = [self.actual_checkpoint(s) for s in ckpt_scopes]
            ckpt_values = [c for c in ckpt_values if c is not None]
            if explicit_ckpt is not None:
                ckpt_values.append(explicit_ckpt)
            actual_ckpt = max(ckpt_values) if ckpt_values else None
            base = actual_ckpt if actual_ckpt is not None else checkpoint_before
            included: list[dict[str, Any]] = []
            stopped_at: int | None = None
            for v in interval_versions:
                if v <= base:
                    # 已被前一次确认覆盖，不重复纳入
                    continue
                commit = self.current_commit(table, v)
                if not self._is_valid(commit, require_data=require_data):
                    stopped_at = v
                    break
                included.append(self._provenance(v, commit))

            confirmed_end = base
            for item in included:
                if item["version"] == confirmed_end + 1:
                    confirmed_end = item["version"]
                else:
                    break

            # 3) 查询快照 vs 当前真实数据的冲突（例如缺口版本被补交）
            conflicts = self._detect_conflicts(table, frozen, query_missing, query_committed)

            full_interval_done = stopped_at is None and all(
                v <= confirmed_end for v in interval_versions
            )
            # 整个原始查询区间必须被检查点前沿连续确认到 whole_end 才算完成。
            # 仅“当前已提交”不够：补交但尚未重放确认的版本仍属于 remaining。
            whole_done = confirmed_end >= whole_end and stopped_at is None
            whole_remaining: list[int] = [
                v for v in range(whole_start, whole_end + 1) if v > confirmed_end
            ]
            remaining_gaps = [
                v for v in whole_remaining
                if not self._is_valid(self.current_commit(table, v), require_data=require_data)
            ]
            # 查询冻结时的缺口里，尚未被本次确认越过的部分 = “还没处理的缺口”，
            # 即使它在恢复前已被补交，也仍然是下一段必须重读的位置。
            unprocessed_gaps = sorted({
                v for v in (query_missing or []) if v > confirmed_end and whole_start <= v <= whole_end
            } | {
                v for v in remaining_gaps
            })
            first_unprocessed = (confirmed_end + 1) if not whole_done else None
            gap_boundary = unprocessed_gaps[0] if unprocessed_gaps else first_unprocessed

            recovery_record = record or {
                "recovery_id": key,
                "id": key,
                "table": table,
                "query_id": query_id,
                "created_at": time.time(),
                "confirm_count": 0,
            }
            recovery_record.update(
                {
                    "table": table,
                    "query_id": query_id,
                    "requested_range": {
                        "start_version": whole_start,
                        "end_version": whole_end,
                        "start": whole_start,
                        "end": whole_end,
                    },
                    "consumed_range": {
                        "start_version": start,
                        "end_version": end,
                        "versions": list(interval_versions),
                    },
                    "actual_range": {
                        "start_version": included[0]["version"] if included else None,
                        "end_version": confirmed_end if included else (base or None),
                        "start": included[0]["version"] if included else None,
                        "end": confirmed_end if included else (base or None),
                        "versions": [i["version"] for i in included],
                    },
                    "replayed_versions": [i["version"] for i in included],
                    "included_commits": included,
                    "conflicts": conflicts,
                    "stopped_at": stopped_at,
                    "gap": gap_boundary,
                    "first_unprocessed": first_unprocessed,
                    "remaining_versions": whole_remaining,
                    "remaining_gaps": remaining_gaps,
                    "unprocessed_gaps": unprocessed_gaps,
                    "query_gaps": list(query_missing or []),
                    "whole_range_completed": whole_done,
                    "updated_at": time.time(),
                }
            )

            winner = False
            if included:
                # CAS：同一次恢复的有效前缀确认，只有第一个请求执行一次
                if not recovery_record.get("confirmed"):
                    recovery_record["confirmed"] = True
                    recovery_record["confirm_count"] = int(recovery_record.get("confirm_count", 0)) + 1
                    recovery_record["confirmed_at"] = time.time()
                    winner = True
                    if confirmed_end > base:
                        for scope_name in ckpt_scopes:
                            self.set_checkpoint(scope_name, confirmed_end)
                    with self._global_lock:
                        self.confirm_events.append(
                            {"recovery_id": key, "table": table, "confirmed_end": confirmed_end, "at": time.time()}
                        )

            if whole_done:
                recovery_record["status"] = "已完成"
            elif winner:
                recovery_record["status"] = "部分确认_缺口保留"
            else:
                recovery_record["status"] = "待恢复"

            with self._global_lock:
                self.recoveries[key] = recovery_record

            current_ckpt = self.actual_checkpoint(table) or confirmed_end
            nxt = current_ckpt + 1
            actual_range = recovery_record["actual_range"]
            response = {
                "ok": True,
                "valid": True,
                "recovery_id": key,
                "id": key,
                "query_id": query_id,
                "table": table,
                "winner": winner,
                "confirmed": bool(winner),
                "applied": bool(winner),
                "newly_confirmed": bool(winner),
                "interval_confirmed": bool(winner),
                "duplicate": False,
                "already_confirmed": False,
                # completed 表示整个原始版本区间都已连续确认完成
                "completed": whole_done,
                "whole_range_completed": whole_done,
                "closed": whole_done,
                "open": not whole_done,
                "pending": not whole_done,
                "todo": not whole_done,
                "confirm_count": recovery_record["confirm_count"],
                "status": recovery_record["status"],
                "requested_range": recovery_record["requested_range"],
                "consumed_range": recovery_record["consumed_range"],
                "actual_range": actual_range,
                "replayed_range": actual_range,
                "replay_range": actual_range,
                "confirmed_range": actual_range,
                "range": actual_range,
                "actual_interval": actual_range,
                "replayed_interval": actual_range,
                "start_version": actual_range["start_version"],
                "end_version": actual_range["end_version"],
                "range_start": actual_range["start_version"],
                "range_end": actual_range["end_version"],
                "actual_start": actual_range["start_version"],
                "actual_end": actual_range["end_version"],
                "from_version": actual_range["start_version"],
                "to_version": actual_range["end_version"],
                "replayed_versions": recovery_record["replayed_versions"],
                "included_versions": recovery_record["replayed_versions"],
                "included": included,
                "included_commits": included,
                "replayed_commits": included,
                "commits": included,
                "provenance": included,
                "conflicts": conflicts,
                "stopped_at": recovery_record["stopped_at"],
                "gap": recovery_record["gap"],
                "first_unprocessed": first_unprocessed,
                "gaps": unprocessed_gaps,
                "unprocessed_gaps": unprocessed_gaps,
                "query_gaps": list(query_missing or []),
                "remaining_versions": whole_remaining,
                "remaining_gaps": remaining_gaps,
                "checkpoint": current_ckpt,
                "checkpoint_offset": current_ckpt,
                "confirmed_offset": current_ckpt,
                "next_offset": nxt,
                "next_read_offset": nxt,
                "next_reread_offset": nxt,
                "next_version": nxt,
                "next_read_version": nxt,
                "next_position": nxt,
                "next_read_position": nxt,
                "reread_offset": nxt,
                "next_reread_position": nxt,
                "confirmed_checkpoint": current_ckpt,
                "actual_confirmed_offset": current_ckpt,
                "message": self._recover_message(
                    table, included, conflicts, recovery_record["stopped_at"], current_ckpt, nxt, whole_done
                ),
            }
            return response

    def _duplicate_response(self, record: dict[str, Any]) -> dict[str, Any]:
        ckpt = self.actual_checkpoint(record["table"]) or 0
        nxt = ckpt + 1
        actual_range = record.get("actual_range", {})
        whole_done = bool(record.get("whole_range_completed"))
        return {
            "ok": True,
            "valid": True,
            "recovery_id": record.get("recovery_id"),
            "id": record.get("id"),
            "table": record.get("table"),
            "winner": False,
            "confirmed": False,
            "applied": False,
            "newly_confirmed": False,
            "interval_confirmed": False,
            "duplicate": True,
            "already_confirmed": True,
            "completed": whole_done,
            "whole_range_completed": whole_done,
            "closed": whole_done,
            "open": not whole_done,
            "pending": not whole_done,
            "todo": not whole_done,
            "confirm_count": record.get("confirm_count", 1),
            "status": record.get("status", "已确认"),
            "requested_range": record.get("requested_range"),
            "consumed_range": record.get("consumed_range"),
            "actual_range": actual_range,
            "replayed_range": actual_range,
            "replay_range": actual_range,
            "confirmed_range": actual_range,
            "range": actual_range,
            "actual_interval": actual_range,
            "start_version": actual_range.get("start_version"),
            "end_version": actual_range.get("end_version"),
            "range_start": actual_range.get("start_version"),
            "range_end": actual_range.get("end_version"),
            "actual_start": actual_range.get("start_version"),
            "actual_end": actual_range.get("end_version"),
            "from_version": actual_range.get("start_version"),
            "to_version": actual_range.get("end_version"),
            "replayed_versions": record.get("replayed_versions", []),
            "included_versions": record.get("replayed_versions", []),
            "included": record.get("included_commits", []),
            "included_commits": record.get("included_commits", []),
            "replayed_commits": record.get("included_commits", []),
            "conflicts": record.get("conflicts", []),
            "stopped_at": record.get("stopped_at"),
            "gap": record.get("gap"),
            "first_unprocessed": record.get("first_unprocessed"),
            "gaps": record.get("unprocessed_gaps", []),
            "unprocessed_gaps": record.get("unprocessed_gaps", []),
            "query_gaps": record.get("query_gaps", []),
            "remaining_versions": record.get("remaining_versions", []),
            "remaining_gaps": record.get("remaining_gaps", []),
            "checkpoint": ckpt,
            "checkpoint_offset": ckpt,
            "confirmed_offset": ckpt,
            "next_offset": nxt,
            "next_read_offset": nxt,
            "next_reread_offset": nxt,
            "next_version": nxt,
            "next_read_version": nxt,
            "next_position": nxt,
            "next_read_position": nxt,
            "reread_offset": nxt,
            "next_reread_position": nxt,
            "confirmed_checkpoint": ckpt,
            "actual_confirmed_offset": ckpt,
            "message": "同一恢复已被有效请求确认过，重复请求不再重复确认",
        }

    def _provenance(self, version: int, commit: dict[str, Any] | None) -> dict[str, Any]:
        commit = commit or {}
        data = commit.get("data")
        return {
            "version": version,
            "table": commit.get("table"),
            "commit_id": commit.get("commit_id"),
            "source_commit": commit.get("commit_id"),
            "status": commit.get("status", "committed"),
            "data": data,
            "rows": data if isinstance(data, list) else [],
            "row_count": commit.get("row_count")
            or (len(data) if isinstance(data, list) else None),
            "summary": f"纳入提交 {commit.get('commit_id')} 的版本 {version} 数据",
        }

    def _detect_conflicts(
        self,
        table: str,
        frozen: dict[str, Any] | None,
        query_missing: list[int],
        query_committed: list[int],
    ) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []
        missing_set = set(query_missing or (frozen or {}).get("missing", []))
        for v in sorted(missing_set):
            now = self.current_commit(table, v)
            if self._is_valid(now):
                conflicts.append(
                    {
                        "version": v,
                        "type": "gap_filled_late",
                        "kind": "late_commit",
                        "query_state": "missing",
                        "current_state": "committed",
                        "commit_id": (now or {}).get("commit_id"),
                        "included": False,
                        "reason": "查询时该版本缺失，恢复前被补交；不属于本次消费的冻结重放区间，需重新查询后再恢复",
                    }
                )
        for v in query_committed or (frozen or {}).get("committed", []):
            now = self.current_commit(table, v)
            if not self._is_valid(now):
                conflicts.append(
                    {
                        "version": v,
                        "type": "commit_vanished",
                        "kind": "revoked_after_query",
                        "query_state": "committed",
                        "current_state": normalize_status((now or {}).get("state")) if now else "missing",
                        "included": False,
                        "reason": "查询时已提交的版本在恢复核对时不再有效，本次不纳入",
                    }
                )
        return conflicts

    @staticmethod
    def _recover_message(table, included, conflicts, stopped_at, ckpt, nxt, done) -> str:
        if included:
            ids = "、".join(f"v{i['version']}({i.get('commit_id')})" for i in included)
            base = f"湖表 {table} 本次实际重放并确认：{ids}"
        else:
            base = f"湖表 {table} 本次没有可纳入的有效提交"
        if conflicts:
            late = [c["version"] for c in conflicts if c["type"] == "gap_filled_late"]
            if late:
                base += f"；冲突：查询时缺口 {late} 已被补交，但未纳入本次冻结区间"
        if stopped_at is not None:
            base += f"；在版本 {stopped_at} 处仍无有效数据，缺口与恢复待办保留，不越级完成后续区间"
        base += f"；检查点实际确认到 {ckpt}，下一读位置 {nxt}"
        return base

    # ------------------------------------------------------------------ todos
    def list_recoveries(self, table: Any = None, **kwargs: Any) -> dict[str, Any]:
        name = None
        if isinstance(table, dict):
            name = _pick(table, TABLE_KEYS)
        elif table is not None:
            name = str(table)
        elif kwargs:
            name = _pick(kwargs, TABLE_KEYS)
        with self._global_lock:
            items = [dict(r) for r in self.recoveries.values()]
        if name:
            items = [r for r in items if r.get("table") == str(name).strip()]
        # 每个待办都同步读取检查点的“实际确认”，给出当前真正的下一读位置
        for item in items:
            scope = item.get("table") or "default"
            live_ckpt = self.actual_checkpoint(scope)
            if live_ckpt is None:
                ar = item.get("actual_range") or {}
                live_ckpt = ar.get("end_version") or 0
            item["live_checkpoint"] = live_ckpt
            item["actual_confirmed_offset"] = live_ckpt
            item["checkpoint"] = live_ckpt
            item["next_offset"] = live_ckpt + 1
            item["next_read_offset"] = live_ckpt + 1
            item["next_reread_offset"] = live_ckpt + 1
        # 已确认过有效前缀、但整段仍被缺口卡住的恢复单依然是未完成待办
        todos = [
            r for r in items
            if (("whole_range_completed" in r and not r.get("whole_range_completed"))
                or ("whole_range_completed" not in r and not r.get("confirmed")))
        ]
        return {
            "ok": True,
            "items": items,
            "total": len(items),
            "todos": todos,
            "todo_total": len(todos),
            "pending": todos,
        }

    def get_recovery(self, recovery_id: str) -> dict[str, Any] | None:
        with self._global_lock:
            rec = self.recoveries.get(str(recovery_id))
            return dict(rec) if rec else None


# 进程内单例：服务层与 HTTP 层共享同一份状态
engine = ReplayEngine()

# 常见别名，方便不同调用习惯导入
replay_engine = engine
replay_store = engine
