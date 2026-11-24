# 实时流数据湖构建与治理平台

面向实时流数据接入、Delta湖表构建、流批一体转换、分区合并和数据治理的流式数据湖平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8011/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5175/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8011`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源 | `stream_source` | 流数据源 | 来源标识、接入协议、主题名称 |
| 湖表定义 | `lake_table` | 湖表定义 | 表标识、表名称、主键列 |
| 流式摄入 | `stream_ingest` | 摄入管道 | 管道标识、来源流、目标湖表 |
| 提交版本 | `delta_commit` | 版本提交 | 提交标识、目标湖表、提交类型 |
| 分区合并 | `partition_merge` | 合并任务 | 任务标识、目标湖表、源分区 |
| 流Schema演进 | `stream_schema` | Schema变更 | 变更标识、目标湖表、变更类型 |
| 快照管理 | `snapshot_mgr` | 表快照 | 快照标识、目标湖表、快照时间 |
| 流质量校验 | `stream_quality` | 质量检查 | 检查标识、目标管道、校验字段 |
| 历史清理 | `vacuum_job` | 清理作业 | 作业标识、目标湖表、保留版本 |
| 批流合并 | `batch_merge` | 合并管道 | 管道标识、流来源、批来源 |
| 湖表血缘 | `lake_lineage` | 血缘链路 | 链路标识、上游表、下游表 |
| 保留策略 | `retention_policy` | 保留规则 | 规则标识、目标湖表、保留天数 |
| 压缩任务 | `compaction_task` | 压缩任务 | 任务标识、目标湖表、压缩算法 |
| 数据回填 | `backfill_pipeline` | 回填任务 | 任务标识、目标湖表、回填范围 |
| 流处理指标 | `stream_metric` | 处理指标 | 指标标识、关联管道、指标类型 |
| 格式迁移 | `iceberg_migrate` | 迁移计划 | 计划标识、源格式、目标格式 |
| 表目录同步 | `table_catalog` | 同步记录 | 记录标识、来源Catalog、目标Catalog |
| 湖仓审计 | `lake_audit` | 审计报告 | 报告标识、审计范围、审计时间 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖

## 提交版本：缺口检索与增量重放恢复

「提交版本」工作台顶部新增 **版本缺口检索与增量重放恢复** 面板，后端核心在
`backend/app/replay_engine.py`，HTTP 入口在 `backend/app/routers/replay.py`
（同时注册了 `/api/delta_commit/...` 与 `/api/replay...` 等常见命名别名）。

典型流程（对应验收场景：检查点 20，目标表完成 21、23，另一张表也有 22）：

```bash
# 造数
curl -X POST localhost:8011/api/delta_commit/seed-scenario \
  -H 'Content-Type: application/json' -d '{"target_table":"target_table"}'

# 1) 按湖表 + 版本范围检索：已提交 [21,23]、已撤销 []、还缺 [22]，
#    查询产生的连续增量重放区间为 [21,21]（他表的 22 不会填补本表缺口）
curl 'localhost:8011/api/delta_commit/replay-range?table=target_table&start_version=21&end_version=23'

# 2) 恢复前补交目标表 22
curl -X POST localhost:8011/api/delta_commit/commits -H 'Content-Type: application/json' \
  -d '{"table":"target_table","version":22,"commit_id":"c22"}'

# 3) 恢复消费查询产生的区间：第一次实际范围仍是 21–21，下一读位置 22；
#    补交的 22 只作为冲突说明，不被本次冻结区间越级吞入
curl -X POST localhost:8011/api/delta_commit/recover -H 'Content-Type: application/json' \
  -d '{"query_id":"<上一步 query_id>"}'

# 4) 同步读取检查点的实际确认，得到下一段真正需重读的偏移（checkpoint=21,next=22）
curl 'localhost:8011/api/delta_commit/next-offset?table=target_table'
```

不变量：严格按湖表隔离版本；检查点只沿连续有效前缀推进，绝不因更大版本号越过
缺口；同一恢复在并发下只有一个有效请求确认一次，缺口未补齐时恢复待办继续保留。

