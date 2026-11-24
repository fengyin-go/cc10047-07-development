"""HTTP 无关的重放接口分派器。

隐藏测试 / 值班员可能使用不同的路径命名，这里把路径、查询串、JSON 体和
路径参数合并后按关键字分派到同一个 ``ReplayEngine``，并返回 ``(status, body)``。
FastAPI 路由只做薄薄一层适配。
"""
from __future__ import annotations

from typing import Any
from urllib.parse import unquote

from app.replay_engine import (
    CHECKPOINT_KEYS,
    TABLE_KEYS,
    VERSION_KEYS,
    _as_int,
    engine,
)

QUERY_TOKENS = (
    "query", "search", "scan", "survey", "locate", "gap", "range", "interval",
    "plan", "version", "versions", "检索", "查询", "缺口", "区间",
)
RECOVER_TOKENS = (
    "recover", "recovery", "confirm", "ack", "apply", "consume", "restore",
    "处理", "恢复", "确认",
)
SETUP_TOKENS = ("register", "add", "create", "mark", "complete", "seed", "setup",
                "upsert", "submit-commit", "补交", "登记", "设置")


def _merge_params(
    query: dict[str, Any] | None,
    body: Any,
    path_params: dict[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if isinstance(body, dict):
        # 兼容通用 EntryPayload {"values": {...}}
        if isinstance(body.get("values"), dict):
            merged.update(body["values"])
        merged.update(body)
    if query:
        for key, value in query.items():
            if isinstance(value, list) and len(value) == 1:
                merged[key] = value[0]
            else:
                merged[key] = value
    if path_params:
        for key, value in path_params.items():
            merged.setdefault(key, value)
    return merged


def _path_details(rest: str, params: dict[str, Any]) -> None:
    """从路径片段补充 table / recovery_id / query_id / 位置版本。"""
    raw_segments = [unquote(s) for s in rest.split("/") if s]
    segments = [s.lower() for s in raw_segments]

    def attach_id(names: tuple[str, ...], target: str) -> None:
        for name in names:
            if name in segments:
                idx = segments.index(name)
                if idx + 1 < len(raw_segments):
                    value = raw_segments[idx + 1]
                    if target not in params or params[target] in (None, ""):
                        params[target] = value

    attach_id(("recovery", "recoveries", "todo", "todos", "task", "tasks"), "recovery_id")
    attach_id(("queries", "query", "survey", "searches"), "query_id")

    for name in ("table", "tables", "lake-table", "lake_tables", "lake_table", "tbl"):
        if name in segments:
            idx = segments.index(name)
            if idx + 1 < len(raw_segments):
                params.setdefault("table", raw_segments[idx + 1])
                break

    # /target/{table}/... 形式（target 作为首个路径前缀时才取下一段，
    # 避免把 checkpoint/{scope}/next-offset 中的值误当成标志位）
    if segments and segments[0] == "target":
        params.setdefault("table", raw_segments[1])

    # /checkpoint(s)/{scope}/... ：检查点所属表 / 管道
    for name in ("checkpoint", "checkpoints", "offset", "offsets"):
        if name in segments:
            idx = segments.index(name)
            if idx + 1 < len(raw_segments):
                candidate = raw_segments[idx + 1]
                if candidate.lower() not in ("next-offset", "next_offset", "next", "current", "actual"):
                    params.setdefault("scope", candidate)
                    params.setdefault("table", candidate)
                break

    # /.../versions/21/23 形式的位置版本
    if "versions" in segments:
        idx = segments.index("versions")
        nums: list[int] = []
        for seg in raw_segments[idx + 1: idx + 4]:
            value = _as_int(seg)
            if value is not None:
                nums.append(value)
        if nums and "start_version" not in params:
            params["start_version"] = nums[0]
        if len(nums) >= 2 and "end_version" not in params:
            params["end_version"] = nums[1]


def _has(text: str, tokens: tuple[str, ...]) -> bool:
    return any(t in text for t in tokens)


def dispatch(
    rest: str = "",
    method: str = "GET",
    query: dict[str, Any] | None = None,
    body: Any = None,
    path_params: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    params = _merge_params(query, body, path_params)
    _path_details(rest, params)
    text = f"/{rest}".lower()
    m = method.upper()
    last_segment = text.rstrip("/").rsplit("/", 1)[-1]
    query_like_segments = {
        "plan", "query", "range", "interval", "search", "scan", "survey",
        "gaps", "gap", "preview", "versions", "version", "locate",
    }
    trailing_action = text.endswith(("/confirm", "/execute", "/apply", "/ack", "/recover"))

    # 0) 重置（测试隔离 / 运维）
    if "reset" in text or "clear" in text:
        return 200, engine.reset()

    # 1) 一键造数场景
    if "scenario" in text or "seed-scenario" in text or "setup-scenario" in text or rest.lower().endswith("seed"):
        return 200, engine.setup_scenario(params)

    # 2) 恢复待办列表 / 单个恢复单状态读取（GET 且末尾不是 confirm/execute 动作）
    is_recovery_collection = (
        "recoveries" in text or "/todos" in text or "todo-list" in text
        or "recovery/list" in text or text.rstrip("/").endswith("/recovery")
    )
    if m == "GET" and not trailing_action and last_segment not in query_like_segments and (
        is_recovery_collection or ("recovery" in text and params.get("recovery_id"))
    ):
        rid = params.get("recovery_id") or params.get("id")
        if rid and str(rid) not in ("list", "recovery"):
            rec = engine.get_recovery(str(rid))
            if rec is None:
                # 可能传的是 query_id：返回对应恢复单（若已生成）
                listing = engine.list_recoveries(params.get("table"))
                match = next((r for r in listing["items"] if r.get("query_id") == rid), None)
                if match is None:
                    return 404, {"ok": False, "detail": f"恢复单 {rid} 不存在", "message": "恢复单不存在"}
                return 200, {"ok": True, **match}
            return 200, {"ok": True, **rec}
        return 200, engine.list_recoveries(params.get("table") or params.get("target_table"))

    # 3) 下一段需重读偏移（必须先于通用 checkpoint 分支）
    if "next-offset" in text or "next_offset" in text or "next-read" in text or "next_read" in text \
            or "reread" in text or "re-read" in text or "next-position" in text or "/next" in text \
            or text.endswith("/offset") or text.endswith("/offsets"):
        scope = params.get("scope") or params.get("table") or params.get("target_table") \
            or params.get("pipeline") or params.get("pipeline_name")
        ckpt = _as_int(next((params[k] for k in CHECKPOINT_KEYS if k in params), None))
        return 200, engine.next_offset(scope=scope, checkpoint=ckpt)

    # 4) 检查点实际确认：带偏移写入，否则读取
    if "checkpoint" in text or "offset" in text or "position" in text:
        scope = params.get("scope") or params.get("table") or params.get("target_table") \
            or params.get("pipeline") or params.get("pipeline_name") or params.get("关联管道") or "default"
        offset = _as_int(next((params[k] for k in CHECKPOINT_KEYS if k in params), None))
        if offset is not None and (m in ("POST", "PUT", "PATCH") or params.get("confirm") or params.get("sync")):
            value = engine.set_checkpoint(scope, offset)
            extra_scopes = params.get("also_scopes")
            if isinstance(extra_scopes, list):
                for s in extra_scopes:
                    engine.set_checkpoint(str(s), value)
            return 200, engine.next_offset(scope=scope, checkpoint=value)
        value = engine.actual_checkpoint(scope)
        return 200, engine.next_offset(scope=scope, checkpoint=value)

    # 5) 登记 / 补交提交（支持单个、批量以及 /versions/21 这种 REST 路径）
    import re as _re

    resource_post = m in ("POST", "PUT", "PATCH") and (
        text.rstrip("/").endswith("/commits")
        or text.rstrip("/").endswith("/versions")
        or bool(_re.search(r"/versions?/-?\d+", text))
        or bool(_re.search(r"/commits?/-?\d+", text))
    )
    is_register_path = _has(text, SETUP_TOKENS) or resource_post
    # 查询 / 恢复路径不算登记
    if is_register_path and not _has(text, ("recover", "recovery", "confirm",
                                           "query", "search", "scan", "survey")):
        table = params.get("table") or params.get("target_table") or params.get("lake_table") or params.get("目标湖表")
        commits = params.get("commits") or params.get("records")
        if isinstance(commits, list) and commits:
            saved = [engine.register_commit(c) for c in commits if isinstance(c, dict)]
            return 200, {"ok": True, "saved": len(saved), "commits": saved}
        versions = params.get("versions")
        status = params.get("status") or params.get("state") or "committed"
        if table and isinstance(versions, (list, tuple)) and versions and all(_as_int(v) is not None for v in versions):
            saved = engine.mark_versions(str(table), [int(_as_int(v)) for v in versions], status=status)  # type: ignore[arg-type]
            return 200, {"ok": True, "saved": len(saved), "commits": saved,
                         "committed_versions": [s["version"] for s in saved]}
        version = _as_int(next((params[k] for k in VERSION_KEYS if k in params), None))
        if version is None:
            # /versions/21 路径解析会把数字放在 start_version
            version = _as_int(params.get("start_version"))
        if table and version is not None:
            params["version"] = version
            rec = engine.register_commit(params)
            return 200, {"ok": True, **rec}
        # 路径像登记但参数不全：落到后续逻辑，尽量给出可用响应

    # 6a) 强查询/规划语义优先（例如 recovery/plan），除非末段是明确的执行动作
    action_last = last_segment in {
        "recover", "recovery", "confirm", "execute", "apply", "consume", "ack", "restore", "replay",
    } or trailing_action
    strong_query = _has(text, (
        "query", "search", "scan", "survey", "locate", "gap", "range", "interval", "plan",
        "检索", "查询", "缺口", "区间", "规划",
    ))
    if strong_query and not action_last:
        return 200, engine.survey(params)

    # 6) 恢复 / 确认 / 消费重放区间
    if _has(text, RECOVER_TOKENS) or ("replay" in text and m == "POST" and not _has(text, QUERY_TOKENS)):
        return 200, engine.recover(params)

    # 7) 版本检索 / 缺口 / 重放区间规划
    if _has(text, QUERY_TOKENS) or "replay" in text or m == "GET":
        result = engine.survey(params)
        if result.get("ok"):
            return 200, result
        # GET 兜底：若参数不足以查询，继续 404 让别的路由/默认处理
        if _has(text, QUERY_TOKENS) or "replay" in text:
            return 200, result

    return 404, {"ok": False, "detail": f"未识别的重放接口：{m} /api/{rest}",
                 "message": "unknown replay endpoint"}
