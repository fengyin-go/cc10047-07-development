"""实时流数据湖构建与治理平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8011
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import chains, replay, resource
from app.store import store

app = FastAPI(title="实时流数据湖构建与治理平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# 路由顺序很关键：
# 1) replay.pre_router：增量重放的显式顶层/模块别名，必须先于 /api/{module}；
# 2) resource / chains：平台既有通用资源与业务链路；
# 3) replay.router：/api/{rest:path} 关键字兜底，必须最后注册。
app.include_router(replay.pre_router)
app.include_router(resource.router)
app.include_router(chains.router)
app.include_router(replay.router)
