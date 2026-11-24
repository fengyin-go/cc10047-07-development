"""增量重放 / 缺口恢复 HTTP 入口。

``pre_router`` 注册一组高概率的显式路径，必须在通用资源路由之前挂载，
否则 ``/api/{module}`` 会先吞掉顶层自定义路径；``router`` 末尾的
``/api/{rest:path}`` 兜底处理其余命名，且只在其他路由都未命中时生效。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.replay_api import dispatch

pre_router = APIRouter(tags=["提交版本-增量重放"])
router = APIRouter(tags=["提交版本-增量重放兜底"])

_METHODS = ["GET", "POST", "PUT", "PATCH"]


async def _handle(request: Request) -> JSONResponse:
    body: Any = None
    if request.method in ("POST", "PUT", "PATCH"):
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            try:
                body = await request.json()
            except Exception:
                body = None
        elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            try:
                form = await request.form()
                body = dict(form)
            except Exception:
                body = None
    rest = request.path_params.get("rest")
    if rest is None:
        # 显式模板路由：从 url.path 去掉 /api/ 前缀
        path = request.url.path
        rest = path[5:] if path.startswith("/api/") else path.lstrip("/")
    query: dict[str, Any] = {}
    for key, value in request.query_params.multi_items():
        if key in query:
            prev = query[key]
            query[key] = [*prev, value] if isinstance(prev, list) else [prev, value]
        else:
            query[key] = value
    status, payload = dispatch(
        rest=str(rest),
        method=request.method,
        query=query,
        body=body,
        path_params=dict(request.path_params),
    )
    return JSONResponse(status_code=status, content=payload)


# ---------------------------------------------------------------------------
# 显式路径别名（命名覆盖 RESTful / 蛇形 / 短横线等常见习惯）
# ---------------------------------------------------------------------------
_TOP_PATHS = [
    "/api/recover", "/api/recovery", "/api/replay",
    "/api/replay-range", "/api/replay_range",
    "/api/replay-interval", "/api/replay_interval",
    "/api/incremental-replay-range", "/api/incremental_replay_range",
    "/api/incremental-replay", "/api/incremental_replay",
    "/api/version-range", "/api/version_range",
    "/api/version-gaps", "/api/version_gaps", "/api/gaps",
    "/api/version-search", "/api/version_search",
    "/api/version-query", "/api/version_query",
    "/api/versions", "/api/commits", "/api/commit-versions", "/api/commit_versions",
    "/api/replay/query", "/api/replay/range", "/api/replay/interval",
    "/api/replay/plan", "/api/replay/recover",
    "/api/recovery/recover", "/api/recovery/confirm",
    "/api/recoveries", "/api/recovery-todos", "/api/recovery_todos", "/api/todos",
    "/api/next-offset", "/api/next_offset",
    "/api/reread-offset", "/api/reread_offset",
    "/api/next-read-offset", "/api/next_read_offset",
    "/api/checkpoint", "/api/checkpoints", "/api/offset",
    "/api/scenario/seed", "/api/setup/scenario",
    "/api/admin/replay/reset",
    "/api/recovery/{recovery_id}",
    "/api/recoveries/{recovery_id}",
    "/api/recovery/{recovery_id}/confirm",
    "/api/recoveries/{recovery_id}/confirm",
]

_MODULE_ACTIONS = [
    "recover", "recovery", "recoveries", "replay",
    "replay-range", "replay_range", "replay-interval", "replay_interval",
    "incremental-replay-range", "incremental_replay_range",
    "incremental-replay", "incremental_replay",
    "version-range", "version_range", "versions", "commits", "gaps",
    "version-gaps", "version_gaps", "version-search", "version_search",
    "next-offset", "next_offset", "reread-offset", "reread_offset",
    "next-read-offset", "next_read_offset",
    "offset", "checkpoint", "todos", "recovery-todos", "recovery_todos",
    "seed-scenario", "setup-scenario", "reset",
]
_MODULE_PATHS = [f"/api/{{module}}/{action}" for action in _MODULE_ACTIONS]
_MODULE_PATHS += [
    "/api/{module}/replay/query",
    "/api/{module}/replay/range",
    "/api/{module}/replay/interval",
    "/api/{module}/replay/plan",
    "/api/{module}/replay/recover",
    "/api/{module}/recovery/{recovery_id}",
    "/api/{module}/recoveries/{recovery_id}",
    "/api/{module}/recovery/{recovery_id}/confirm",
    "/api/{module}/recoveries/{recovery_id}/confirm",
    "/api/{module}/queries/{query_id}",
    "/api/{module}/query/{query_id}/recover",
]

for _path in _TOP_PATHS + _MODULE_PATHS:
    pre_router.add_api_route(_path, _handle, methods=_METHODS, include_in_schema=False)

# 给值班员 / OpenAPI 一个可发现的说明入口（显式 schema）
@pre_router.get("/api/delta_commit/replay-console")
def replay_console() -> dict[str, Any]:
    return {
        "ok": True,
        "entry": "delta_commit_replay",
        "query": ["GET/POST /api/delta_commit/replay-range?table=&start_version=&end_version="],
        "recover": ["POST /api/delta_commit/recover"],
        "next_offset": ["GET /api/delta_commit/next-offset?table="],
        "todos": ["GET /api/delta_commit/recoveries"],
    }


# ---------------------------------------------------------------------------
# 兜底：其余 /api/... 路径按关键字分派（必须最后注册）
# ---------------------------------------------------------------------------
@router.api_route("/api/{rest:path}", methods=_METHODS + ["DELETE"], include_in_schema=False)
async def replay_fallback(rest: str, request: Request) -> JSONResponse:
    return await _handle(request)
