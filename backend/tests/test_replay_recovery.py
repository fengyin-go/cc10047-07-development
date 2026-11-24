"""增量重放区间 / 缺口恢复的验收测试，对应值班员版本检索与并发恢复场景。"""
from __future__ import annotations

import sys
import threading

sys.path.insert(0, ".")

import pytest

from app.main import app
from app.replay_engine import engine
from app.services import service_for

try:  # HTTP 用例依赖 httpx（与既有 test_chains 相同）；缺失时仅跳过该用例
    from fastapi.testclient import TestClient
except Exception:  # pragma: no cover
    TestClient = None  # type: ignore[assignment]


@pytest.fixture(autouse=True)
def _reset_engine():
    engine.reset()
    yield
    engine.reset()


def _seed_prompt_scenario():
    # 检查点在 20；目标表完成 21、23；另一张表也有 22
    engine.set_checkpoint("target_table", 20)
    engine.mark_versions("target_table", [21, 23], status="committed")
    engine.mark_versions("other_table", [22], status="committed")


class TestVersionSurvey:
    def test_locates_committed_revoked_missing(self):
        engine.set_checkpoint("t", 20)
        engine.mark_versions("t", [21, 23])
        engine.register_commit("t", 24, status="revoked")
        result = engine.survey(table="t", start_version=21, end_version=24)
        assert result["committed"] == [21, 23]
        assert result["revoked"] == [24]
        assert result["missing"] == [22]
        assert result["replay_range"]["versions"] == [21]

    def test_other_table_same_version_does_not_fill_gap(self):
        _seed_prompt_scenario()
        result = engine.survey(table="target_table", start_version=21, end_version=23)
        assert result["missing"] == [22]
        assert 22 not in result["committed"]


class TestPromptScenario:
    def test_first_recovery_range_and_next_position_after_late_commit(self):
        _seed_prompt_scenario()
        queried = engine.survey(table="target_table", start_version=21, end_version=23)
        assert queried["missing"] == [22]
        assert queried["replay_range"]["versions"] == [21]

        # 恢复前补交目标表 22
        engine.register_commit("target_table", 22, status="committed", commit_id="commit-22-late")

        first = engine.recover(query_id=queried["query_id"])
        # 第一次恢复只消费查询产生的冻结区间 [21]，不被更大版本带过缺口
        assert first["replayed_versions"] == [21]
        assert first["actual_range"]["start_version"] == 21
        assert first["actual_range"]["end_version"] == 21
        assert first["checkpoint"] == 21
        assert first["next_offset"] == 22
        assert first["next_read_version"] == 22
        # 补交的 22 被说明为冲突且未纳入
        assert any(c["version"] == 22 and c["included"] is False for c in first["conflicts"])
        assert first["unprocessed_gaps"] == [22]

        # 检查点的实际确认决定下一段重读位置
        nxt = engine.next_offset(scope="target_table")
        assert nxt["checkpoint"] == 21
        assert nxt["next_offset"] == 22

    def test_cannot_leapfrog_gap_even_with_larger_version(self):
        engine.set_checkpoint("g", 20)
        engine.mark_versions("g", [21, 23])
        result = engine.recover(table="g", start_version=21, end_version=23)
        assert result["replayed_versions"] == [21]
        assert result["gap"] == 22
        assert result["completed"] is False
        assert result["next_offset"] == 22
        assert engine.actual_checkpoint("g") == 21


class TestConcurrentConfirm:
    def test_only_one_valid_confirmation(self):
        engine.set_checkpoint("c", 20)
        engine.mark_versions("c", [21])
        queried = engine.survey(table="c", start_version=21, end_version=21)

        barrier = threading.Barrier(8)
        outcomes: list[dict] = []

        def worker():
            barrier.wait()
            outcomes.append(engine.recover(query_id=queried["query_id"]))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(1 for r in outcomes if r["confirmed"]) == 1
        assert sum(1 for r in outcomes if r["already_confirmed"]) == 7
        record = engine.recoveries[f"rcv-{queried['query_id']}"]
        assert record["confirm_count"] == 1
        assert engine.actual_checkpoint("c") == 21

    def test_gap_recovery_keeps_todo_under_concurrency(self):
        engine.set_checkpoint("gc", 20)
        engine.mark_versions("gc", [21, 23])
        queried = engine.survey(table="gc", start_version=21, end_version=23)

        barrier = threading.Barrier(5)
        outcomes: list[dict] = []

        def worker():
            barrier.wait()
            outcomes.append(engine.recover(query_id=queried["query_id"]))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(1 for r in outcomes if r["confirmed"]) == 1
        assert engine.actual_checkpoint("gc") == 21
        assert all(r["next_offset"] == 22 for r in outcomes)
        todos = engine.list_recoveries(table="gc")
        assert todos["todo_total"] == 1


class TestServiceAndHttp:
    def test_delta_commit_service_aliases(self):
        svc = service_for("delta_commit")
        svc.domain_set_checkpoint("svc_t", 20)
        svc.domain_mark_committed(table="svc_t", versions=[21, 23])
        queried = svc.domain_query_replay_range(table="svc_t", start_version=21, end_version=23)
        assert queried["missing"] == [22]
        recovered = svc.domain_recover(query_id=queried["query_id"])
        assert recovered["next_offset"] == 22

    def test_checkpoint_service_next_offset(self):
        engine.set_checkpoint("pipe-1", 20)
        svc = service_for("stream_checkpoint")
        result = svc.domain_next_offset({"pipeline": "pipe-1"})
        assert result["checkpoint"] == 20
        assert result["next_offset"] == 21

    def test_http_prompt_scenario(self):
        if TestClient is None:
            pytest.skip("当前环境缺少 httpx，跳过 TestClient 用例")
        client = TestClient(app)
        client.post("/api/admin/replay/reset")
        client.post("/api/delta_commit/checkpoint", json={"table": "http_t", "offset": 20})
        client.post("/api/delta_commit/versions",
                    json={"table": "http_t", "versions": [21, 23]})
        client.post("/api/commits", json={"table": "other_http", "version": 22})

        queried = client.get("/api/delta_commit/replay-range",
                              params={"table": "http_t", "start_version": 21, "end_version": 23}).json()
        assert queried["missing"] == [22]

        client.post("/api/delta_commit/commits",
                    json={"table": "http_t", "version": 22, "commit_id": "c22"})
        first = client.post("/api/delta_commit/recover",
                            json={"query_id": queried["query_id"]}).json()
        assert first["actual_range"]["end_version"] == 21
        assert first["next_offset"] == 22

        nxt = client.get("/api/delta_commit/next-offset", params={"table": "http_t"}).json()
        assert nxt["next_offset"] == 22

        duplicate = client.post("/api/recover", json={"query_id": queried["query_id"]}).json()
        assert duplicate["already_confirmed"] is True
