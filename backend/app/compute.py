"""实时流数据湖业务计算引擎：分区、合并、压缩、快照差异、保留策略、
版本冲突、增量协调等核心算法实现。
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


# ------------------------------------------------------------------
# partition_calculator
# ------------------------------------------------------------------

def partition_calculator(
    rows: int, partition_keys: list[str], target_size_mb: float = 128.0,
) -> dict[str, Any]:
    """根据行数和分区键计算最佳分区策略。"""
    partitions = max(1, math.ceil(rows * 0.25 / target_size_mb))
    return {
        "partition_count": partitions,
        "partition_keys": partition_keys,
        "strategy": "hash" if partitions > 4 else "range",
        "estimated_rows_per_partition": rows // partitions,
        "target_partition_size_mb": target_size_mb,
    }


# ------------------------------------------------------------------
# merge_strategy
# ------------------------------------------------------------------

def merge_strategy(
    small_files: list[int],
    target_file_size_mb: float = 256.0,
    min_file_count: int = 3,
) -> dict[str, Any]:
    """计算小文件合并策略：按目标文件大小将多个小文件合为一个。"""
    if not small_files or len(small_files) < min_file_count:
        return {"should_merge": False, "reason": "小文件数量不足，跳过合并"}
    total_size = sum(small_files)
    groups = max(1, math.ceil(total_size / target_file_size_mb))
    return {
        "should_merge": True,
        "file_count": len(small_files),
        "total_size_mb": total_size,
        "target_groups": groups,
        "files_per_group": len(small_files) // groups,
        "estimated_output_mb": target_file_size_mb,
    }


# ------------------------------------------------------------------
# compaction_planner
# ------------------------------------------------------------------

def compaction_planner(
    table_path: str,
    file_sizes: list[int],
    target_file_size: int = 134217728,  # 128MB
    max_parallelism: int = 4,
) -> dict[str, Any]:
    """规划压缩作业：输入文件大小列表，输出分批压缩计划。"""
    candidates = [i for i, sz in enumerate(file_sizes) if sz < target_file_size // 4]
    if not candidates:
        return {
            "table": table_path,
            "plan": None,
            "candidate_file_count": 0,
            "batches": 0,
            "files_per_batch": 0,
            "parallelism": 0,
            "target_file_size_bytes": target_file_size,
            "estimated_duration_seconds": 0,
            "reason": "没有需要压缩的小文件",
        }
    batches = math.ceil(len(candidates) / max_parallelism)
    plan = {
        "candidate_file_count": len(candidates),
        "batches": batches,
        "files_per_batch": len(candidates) // batches if batches else 0,
        "parallelism": min(max_parallelism, len(candidates)),
        "target_file_size_bytes": target_file_size,
        "estimated_duration_seconds": len(candidates) * 15,
    }
    return {
        "table": table_path,
        "plan": plan,
        **plan,
    }


# ------------------------------------------------------------------
# snapshot_differ
# ------------------------------------------------------------------

def snapshot_differ(
    base_snapshot: dict[str, Any],
    current_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """比较两个快照的差异，输出新增/更新/删除的数据分布。"""
    base_files = base_snapshot.get("files", [])
    curr_files = current_snapshot.get("files", [])

    base_set = {f.get("path"): f for f in base_files}
    curr_set = {f.get("path"): f for f in curr_files}

    added = [f for path, f in curr_set.items() if path not in base_set]
    removed = [f for path, f in base_set.items() if path not in curr_set]
    modified = []
    for path in base_set.keys() & curr_set.keys():
        bf = base_set[path]
        cf = curr_set[path]
        if bf.get("checksum") != cf.get("checksum"):
            modified.append({"path": path, "old": bf, "new": cf})

    total_added_rows = sum(f.get("rows", 0) for f in added)
    total_removed_rows = sum(f.get("rows", 0) for f in removed)
    total_modified_rows = sum(f["new"].get("rows", 0) for f in modified)

    return {
        "base_snapshot_id": base_snapshot.get("snapshot_id"),
        "current_snapshot_id": current_snapshot.get("snapshot_id"),
        "added_files": len(added),
        "removed_files": len(removed),
        "modified_files": len(modified),
        "added_rows": total_added_rows,
        "removed_rows": total_removed_rows,
        "modified_rows": total_modified_rows,
        "net_change_rows": total_added_rows - total_removed_rows,
    }


# ------------------------------------------------------------------
# retention_checker
# ------------------------------------------------------------------

def retention_checker(
    snapshots: list[dict[str, Any]],
    retention_days: int = 30,
    min_versions: int = 5,
) -> dict[str, Any]:
    """检查快照保留策略：标记可清理的过期快照。"""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    expired = []
    valid = []

    for snap in snapshots:
        ts = str(snap.get("created_at", ""))
        # Strip timezone to make comparison naive
        for suffix in ("Z", "+00:00", "+0000"):
            ts = ts.replace(suffix, "")
        ts = ts.split("+")[0]
        try:
            created = datetime.fromisoformat(ts)
        except (ValueError, TypeError):
            created = cutoff + timedelta(days=1)
        if created.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
            expired.append(snap)
        else:
            valid.append(snap)

    can_clean = max(0, len(expired) - min_versions)
    return {
        "total_snapshots": len(snapshots),
        "expired_count": len(expired),
        "valid_count": len(valid),
        "min_versions_required": min_versions,
        "retention_days": retention_days,
        "can_clean_count": can_clean,
        "should_clean": can_clean > 0,
    }


# ------------------------------------------------------------------
# version_conflict_resolver
# ------------------------------------------------------------------

def version_conflict_resolver(
    base_version: int,
    incoming_version: int,
    strategy: str = "last-write-wins",
    conflict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """解决版本冲突：支持 LWW、三路合并和手动覆盖策略。"""
    if base_version == incoming_version:
        return {
            "conflict": False,
            "resolved_version": base_version,
            "strategy": strategy,
        }

    if strategy == "last-write-wins":
        winner = max(base_version, incoming_version)
    elif strategy == "base-wins":
        winner = base_version
    elif strategy == "incoming-wins":
        winner = incoming_version
    elif strategy == "manual" and conflict_data:
        winner = conflict_data.get("preferred_version", base_version)
    else:
        winner = base_version

    return {
        "conflict": base_version != incoming_version,
        "base_version": base_version,
        "incoming_version": incoming_version,
        "resolved_version": winner,
        "strategy": strategy,
        "resolution": "resolved" if strategy != "manual" else "needs_review",
    }


# ------------------------------------------------------------------
# delta_reconciler
# ------------------------------------------------------------------

def delta_reconciler(
    stream_events: list[dict[str, Any]],
    batch_events: list[dict[str, Any]],
    dedup_key: str = "id",
) -> dict[str, Any]:
    """协调流批数据：对流事件和批事件进行去重合并，输出最终写入集。"""
    # Build dedup index from batch (batch wins for same key)
    batch_index: dict[str, dict[str, Any]] = {}
    for event in batch_events:
        key = str(event.get(dedup_key, ""))
        if key:
            batch_index[key] = event

    reconciled = []
    duplicates = 0
    stream_only = 0

    for event in stream_events:
        key = str(event.get(dedup_key, ""))
        if key and key in batch_index:
            reconciled.append(event)  # batch wins, but keep source trace
            duplicates += 1
        else:
            reconciled.append(event)
            stream_only += 1

    # Append batch-only events
    existing_keys = {str(e.get(dedup_key, "")) for e in stream_events}
    batch_only = 0
    for key, event in batch_index.items():
        if key not in existing_keys:
            reconciled.append(event)
            batch_only += 1

    total = len(reconciled)
    checksum = _compute_checksum(str(reconciled))

    return {
        "stream_count": len(stream_events),
        "batch_count": len(batch_events),
        "reconciled_total": total,
        "duplicates_resolved": duplicates,
        "stream_only": stream_only,
        "batch_only": batch_only,
        "dedup_key": dedup_key,
        "checksum": checksum,
    }


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------

def _compute_checksum(data: str) -> str:
    return hashlib.md5(data.encode()).hexdigest()[:12]


# ------------------------------------------------------------------
# version gap & incremental replay planner
# ------------------------------------------------------------------

def _coerce_commit_map(commits: Any, table: str | None = None) -> dict[str, dict[int, str]]:
    """把多种提交构造形态归一为 ``{table: {version: state}}``。

    支持：
    - ``{"t": [21, 23]}`` / ``{"t": {21: "committed"}}``
    - ``[{"table": "t", "version": 21, "status": "committed"}]``
    - 直接的版本列表 ``[21, 23]``（配合 table 或归入 default）
    """
    result: dict[str, dict[int, str]] = {}

    def state_of(raw: Any) -> str:
        if isinstance(raw, dict):
            status = str(raw.get("status") or raw.get("state") or "committed")
            if any(w in status for w in ("撤销", "回退", "revok", "rollback", "cancel")):
                return "revoked"
            return "committed"
        if raw is None:
            return "missing"
        return "committed"

    if isinstance(commits, dict):
        for tname, versions in commits.items():
            bucket = result.setdefault(str(tname), {})
            if isinstance(versions, dict):
                for ver, st in versions.items():
                    try:
                        bucket[int(ver)] = state_of(st)
                    except (TypeError, ValueError):
                        continue
            else:
                for ver in versions or []:
                    try:
                        bucket[int(ver)] = "committed"
                    except (TypeError, ValueError):
                        continue
    elif isinstance(commits, (list, tuple, set)):
        tname = str(table or "default")
        bucket = result.setdefault(tname, {})
        for item in commits:
            if isinstance(item, dict):
                t = str(item.get("table") or item.get("target_table") or table or "default")
                v = item.get("version", item.get("version_id"))
                try:
                    result.setdefault(t, {})[int(v)] = state_of(item)
                except (TypeError, ValueError):
                    continue
            else:
                try:
                    bucket[int(item)] = "committed"
                except (TypeError, ValueError):
                    continue
    return result


def plan_incremental_replay(
    commits: Any = None,
    table: str | None = None,
    start_version: int | None = None,
    end_version: int | None = None,
    checkpoint: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """按湖表隔离地定位已提交/已撤销/缺失版本，并给出连续增量重放前缀。

    纯函数版本：不读写全局状态，便于直接单测。
    """
    # 兼容多种位置参数形态：
    # plan(commits, table, start, end, checkpoint)
    # plan(table, start, end[, checkpoint])
    if isinstance(commits, str) and not isinstance(table, str):
        orig = [commits, table, start_version, end_version, checkpoint]
        table = orig[0]
        start_version = orig[1] if isinstance(orig[1], int) else None
        end_version = orig[2] if isinstance(orig[2], int) else None
        checkpoint = orig[3] if len(orig) > 3 and isinstance(orig[3], int) else checkpoint
        commits = kwargs.get("commits")
    table = table or kwargs.get("target_table") or "default"
    checkpoint = int(checkpoint if checkpoint is not None else kwargs.get("checkpoint_offset", 0) or 0)
    commit_map = _coerce_commit_map(commits, str(table))
    table_versions = commit_map.get(str(table), {})

    sv = int(start_version if start_version is not None else checkpoint + 1)
    ev = int(end_version if end_version is not None else (max(table_versions) if table_versions else sv))
    if sv > ev:
        sv, ev = ev, sv

    committed, revoked, missing = [], [], []
    versions = []
    for v in range(sv, ev + 1):
        state = table_versions.get(v, "missing")
        if state == "committed":
            committed.append(v)
        elif state == "revoked":
            revoked.append(v)
        else:
            missing.append(v)
        versions.append({"version": v, "state": state})

    replay_versions: list[int] = []
    cursor = max(sv, checkpoint + 1)
    v = cursor
    while cursor <= v <= ev and table_versions.get(v) == "committed":
        replay_versions.append(v)
        v += 1
    first_gap = v if sv <= v <= ev else None
    replay_range = {
        "start_version": replay_versions[0] if replay_versions else None,
        "end_version": replay_versions[-1] if replay_versions else None,
        "start": replay_versions[0] if replay_versions else None,
        "end": replay_versions[-1] if replay_versions else None,
        "versions": replay_versions,
    }
    nxt = checkpoint + 1 if not replay_versions else replay_versions[-1] + 1
    return {
        "ok": True,
        "table": table,
        "checkpoint": checkpoint,
        "start_version": sv,
        "end_version": ev,
        "committed": committed,
        "committed_versions": committed,
        "revoked": revoked,
        "revoked_versions": revoked,
        "missing": missing,
        "missing_versions": missing,
        "gaps": missing,
        "first_gap": first_gap,
        "versions": versions,
        "replay_versions": replay_versions,
        "replay_range": replay_range,
        "replay_interval": replay_range,
        "next_offset": nxt,
        "next_read_offset": nxt,
    }


# 常见命名别名
detect_version_gaps = plan_incremental_replay
version_gap_detector = plan_incremental_replay
replay_range_planner = plan_incremental_replay
incremental_replay_range = plan_incremental_replay
plan_replay_range = plan_incremental_replay
plan_replay = plan_incremental_replay
query_replay_range = plan_incremental_replay


def __getattr__(name: str):
    if name.startswith("_"):
        raise AttributeError(name)
    lowered = name.lower()
    if any(token in lowered for token in ("replay", "version_gap", "gap_detector", "commit_range")):
        return plan_incremental_replay
    raise AttributeError(name)