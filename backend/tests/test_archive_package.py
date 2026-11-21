"""可重放归档包测试：构建、传输损坏、部分核验、幂等接收与局部/整包重放。

覆盖需求点：
- 归档包逐分片关联批次、原始行号和内容校验，可下载；
- 归档包跨多个批次，尾部未核验通过时只能做局部重放，并给出缺损原文区间；
- 上传与核验并发时，通过的批次与缺损区间同时标出；
- 接收中断后重试不会重复登记已确认日志，原批次编号仍可回查；
- 损坏后段后重放只执行可核验部分，修复后再接收得到两批完整结果。
"""
from __future__ import annotations

import json

import pytest

from app.services import archive_service
from app.services.archive_package import archive_registry, parse_package_text
from app.store import store


def _seed_two_batches(pipeline):
    pipeline.register_source("arc-src-a", "http", "default")
    pipeline.validate_source("arc-src-a")
    logs_a = [{"body": f"2026-01-15T10:3{i}:00|INFO|alpha-{i}"} for i in range(4)]
    batch_a = pipeline.start_ingest_batch("arc-src-a", "2026-01-15", logs_a)["batch"]["批次编号"]

    pipeline.register_source("arc-src-b", "http", "default")
    pipeline.validate_source("arc-src-b")
    logs_b = [{"body": f"2026-01-16T11:0{i}:00|WARN|beta-{i}"} for i in range(3)]
    batch_b = pipeline.start_ingest_batch("arc-src-b", "2026-01-16", logs_b)["batch"]["批次编号"]
    return batch_a, batch_b


@pytest.fixture(autouse=True)
def reset_registry():
    archive_registry.reset()
    yield
    archive_registry.reset()


class TestPackageBuild:
    def test_package_links_batch_line_and_checksum(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        result = archive_service.build_package([batch_a, batch_b], archive_name="two-batch")

        assert result["ok"] is True
        package = result["package"]
        assert package["batch_nos"] == [batch_a, batch_b]
        assert package["shard_count"] == 7
        assert package["record_count"] == 7

        text, _ = archive_service.package_text(package["package_id"])
        parsed = parse_package_text(text)
        assert parsed["manifest"]["batches"][0]["batch_no"] == batch_a
        for shard in parsed["slots"]:
            item = shard["item"]
            assert item["batch_no"] in (batch_a, batch_b)
            assert item["content_checksum"]
            assert item["body_checksum"]
            assert item["byte_length"] == len(item["body"].encode("utf-8"))

    def test_package_download_text_is_ndjson(self, pipeline):
        batch_a, _ = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a])["package"]["package_id"]
        text, name = archive_service.package_text(package_id)
        lines = [json.loads(line) for line in text.splitlines() if line.strip()]
        assert lines[0]["kind"] == "manifest"
        assert lines[-1]["kind"] == "footer"
        assert all(item["kind"] == "shard" for item in lines[1:-1])
        assert name

    def test_unknown_batch_rejected(self, pipeline):
        result = archive_service.build_package(["NO-SUCH-BATCH"])
        assert result["ok"] is False
        assert "不存在" in result["message"]

    def test_archive_strategy_action_builds_package(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        from app.services import service_for

        archive = service_for("data_archive")
        entry, message = archive.run_action(1, "执行归档")
        assert entry is not None
        assert entry["status"] == "已执行"
        package_id = entry["最后归档包"]
        assert package_id
        detail = archive_service.package_detail(package_id)
        archived_batches = {batch["batch_no"] for batch in detail["manifest"]["batches"]}
        assert archived_batches == {batch_a, batch_b}
        assert "归档包" in message


class TestPartialReceive:
    def test_truncated_tail_only_readable_prefix(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)

        lines = text.splitlines()
        corrupted = "\n".join(lines[:-3]) + "\n"  # 丢掉 footer 和最后两个分片
        result = archive_service.receive_package(package_id, corrupted)

        verification = result["verification"]
        assert verification["state"] == "部分通过"
        assert verification["complete_package"] is False
        assert verification["readable_batches"] == [batch_a, batch_b]
        assert verification["passed_batches"] == [batch_a]
        assert verification["missing_ranges"] == {batch_b: ["2-3"]}
        assert verification["package_integrity"].startswith("截断")

    def test_corrupt_body_detected_by_checksum(self, pipeline):
        batch_a, _ = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        lines = text.splitlines()
        shard = json.loads(lines[2])
        shard["body"] = shard["body"] + "-tampered"
        lines[2] = json.dumps(shard, ensure_ascii=False)
        result = archive_service.receive_package(package_id, "\n".join(lines) + "\n")

        verification = result["verification"]
        assert verification["complete_package"] is False
        assert verification["missing_ranges"] == {batch_a: ["2"]}
        assert any("校验" in item["reason"] for item in verification["invalid"])

    def test_line_reference_mismatch_detected(self, pipeline):
        batch_a, _ = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        lines = text.splitlines()
        shard = json.loads(lines[1])
        shard["line_no"] = "99"
        lines[1] = json.dumps(shard, ensure_ascii=False)
        result = archive_service.receive_package(package_id, "\n".join(lines) + "\n")

        verification = result["verification"]
        assert verification["state"] == "部分通过"
        assert verification["missing_ranges"] == {batch_a: ["1"]}
        assert any("清单" in item["reason"] or "校验" in item["reason"] for item in verification["invalid"])

    def test_receive_reports_passed_batches_and_missing_ranges_together(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        corrupted = "\n".join(text.splitlines()[:-3]) + "\n"
        result = archive_service.receive_package(package_id, corrupted)

        verification = result["verification"]
        assert batch_a in verification["passed_batches"]
        assert verification["missing_ranges"].get(batch_b)
        assert "缺损区间" in result["message"]


class TestIdempotentRegistration:
    def test_retry_does_not_duplicate_confirmed_records(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)

        first = archive_service.receive_package(package_id, text)
        before = len(store.rows("raw_records"))
        assert first["registration"]["new_records"] == 0  # 构建时已登记

        corrupted = "\n".join(text.splitlines()[:-3]) + "\n"
        archive_service.receive_package(package_id, corrupted)
        retry = archive_service.receive_package(package_id, text)

        assert retry["registration"]["new_records"] == 0
        assert retry["registration"]["existing_records"] == 7
        assert len(store.rows("raw_records")) == before

        # 原批次编号仍可回查，且原始行号和正文没有变化
        rows = [
            row for row in store.rows("raw_records")
            if row.get("批次编号") == batch_b
        ]
        assert sorted(row["原始行号"] for row in rows) == ["1", "2", "3"]
        assert rows[0]["原始正文"].endswith("beta-0")
        batch_row = next(
            row for row in store.rows("ingest_batch") if row.get("批次编号") == batch_b
        )
        assert batch_row["批次编号"] == batch_b

    def test_existing_row_with_different_body_reported_as_conflict(self, pipeline):
        batch_a, _ = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        target = next(
            row for row in store.rows("raw_records") if row.get("批次编号") == batch_a
        )
        target["原始正文"] = "被本地改写的正文"

        result = archive_service.receive_package(package_id, text)
        assert result["registration"]["conflicts"]
        conflict = result["registration"]["conflicts"][0]
        assert conflict["batch_no"] == batch_a
        assert conflict["raw_record_id"] == target["id"]
        # 冲突不覆盖本地已确认记录
        assert target["原始正文"] == "被本地改写的正文"


class TestArchiveReplay:
    def test_partial_package_cannot_masquerade_as_full_replay(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        corrupted = "\n".join(text.splitlines()[:-3]) + "\n"
        archive_service.receive_package(package_id, corrupted)

        result = archive_service.replay_package(package_id, "field-rule-v2")
        assert result["ok"] is True
        assert result["scope"] == "局部重放"
        assert result["complete_replay"] is False
        assert result["skipped_batches"] == []
        assert result["missing_ranges"] == {batch_b: ["2-3"]}
        assert result["replayed_count"] == 5
        assert batch_b in result["executed_batches"]

        task = result["replay"]
        assert task["status"] == "部分失败"
        assert task["complete_replay"] is False
        assert task["missing_ranges"] == {batch_b: ["2-3"]}
        assert task["package_revision"]

        journal = [row for row in store.rows("parse_journal") if row.get("操作类型") == "归档重放"]
        assert journal[-1]["scope"] == "局部重放"
        assert journal[-1]["missing_ranges"] == {batch_b: ["2-3"]}

    def test_replay_reads_verified_body_and_original_line(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        archive_service.receive_package(package_id, text)

        result = archive_service.replay_package(package_id, "field-rule-v3")
        assert result["scope"] == "整包重放"
        assert result["complete_replay"] is True
        assert result["replayed_count"] == 7

        refs = result["used_sources"]
        assert {ref["batch_no"] for ref in refs} == {batch_a, batch_b}
        assert {ref["line_no"] for ref in refs if ref["batch_no"] == batch_b} == {"1", "2", "3"}
        for ref in refs:
            raw = store.find("raw_records", ref["raw_record_id"])
            assert raw is not None
            assert raw["原始正文"] == ref["body_preview"] or raw["原始正文"].startswith(ref["body_preview"])
            assert raw["content_checksum"] == ref["content_checksum"]

        detail = archive_service.replay_task_sources(result["replay"]["id"])
        assert detail["total"] == 7
        assert all(item["原始正文"] for item in detail["items"])
        assert all(item["标准化字段集"] for item in detail["items"])

    def test_replay_after_repair_marks_both_batches_complete(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)

        archive_service.receive_package(package_id, "\n".join(text.splitlines()[:-3]) + "\n")
        partial = archive_service.replay_package(package_id, "field-rule-v4")
        assert partial["scope"] == "局部重放"

        # 修复后重新接收完整包，再做整包重放
        repair = archive_service.receive_package(package_id, text)
        assert repair["verification"]["complete_package"] is True
        assert repair["verification"]["passed_batches"] == [batch_a, batch_b]

        replay = archive_service.replay_package(package_id, "field-rule-v4", validate_entities=True)
        assert replay["scope"] == "整包重放"
        assert replay["complete_replay"] is True
        assert replay["executed_batches"] == [batch_a, batch_b]

        for batch_no in (batch_a, batch_b):
            entities = [
                row for row in store.rows("parsed_entity")
                if row.get("来源批次") == batch_no
                and row.get("archive_package_id") == package_id
            ]
            assert len(entities) == 4 if batch_no == batch_a else 3
            assert all(entity["status"] == "校验通过" for entity in entities)
            raw = [
                row for row in store.rows("raw_records") if row.get("批次编号") == batch_no
            ]
            assert all(row["status"] == "已解析" for row in raw)
            assert all(row["原始正文"] for row in raw)

    def test_replay_fails_without_verified_body(self, pipeline):
        batch_a, _ = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a])["package"]["package_id"]
        result = archive_service.receive_package(package_id, "{not-json}\n")
        assert result["verification"]["state"] == "核验失败"
        replay = archive_service.replay_package(package_id, "field-rule-v5")
        assert replay["ok"] is False
        assert "没有通过核验" in replay["message"]


class TestUsableFileList:
    def test_file_list_tracks_partial_and_repaired_state(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        archive_service.receive_package(package_id, "\n".join(text.splitlines()[:-3]) + "\n")

        items = archive_service.usable_file_list()["items"]
        by_batch = {item["batch_no"]: item for item in items}
        assert by_batch[batch_a]["file_status"] == "可用"
        assert by_batch[batch_b]["file_status"] == "部分可用"
        assert by_batch[batch_b]["missing_ranges"] == ["2-3"]
        assert by_batch[batch_b]["registered_lines"] == 1
        assert by_batch[batch_b]["raw_record_ids"]

        archive_service.receive_package(package_id, text)
        items = archive_service.usable_file_list()["items"]
        by_batch = {item["batch_no"]: item for item in items}
        assert by_batch[batch_b]["file_status"] == "可用"
        assert by_batch[batch_b]["missing_ranges"] == []

    def test_readable_only_filter(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        # 丢掉全部 batch_b 分片
        lines = text.splitlines()
        keep = [lines[0]]
        for line in lines[1:]:
            item = json.loads(line)
            if item.get("kind") != "shard" or item.get("batch_no") == batch_a:
                keep.append(line)
        archive_service.receive_package(package_id, "\n".join(keep) + "\n")

        all_items = archive_service.usable_file_list()["items"]
        readable = archive_service.usable_file_list(readable_only=True)["items"]
        assert {item["batch_no"] for item in all_items} == {batch_a, batch_b}
        assert {item["batch_no"] for item in readable} == {batch_a}
        assert all(not item["readable"] for item in all_items if item["batch_no"] == batch_b)


class TestUploadDuringVerification:
    def test_second_upload_while_first_unverified_keeps_both_states(self, pipeline):
        """模拟值班员在第一批正文已可用、尾部尚未核验时发起重放。"""
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        truncated = "\n".join(text.splitlines()[:-3]) + "\n"

        first = archive_service.receive_package(package_id, truncated)
        assert first["verification"]["state"] == "部分通过"

        # 上传尚未完成时，重放引擎请求第一批正文
        engine_view = archive_service.replay_package(package_id, "field-rule-race", batch_nos=[batch_a])
        assert engine_view["scope"] == "局部重放"
        assert engine_view["complete_replay"] is False
        assert engine_view["executed_batches"] == [batch_a]
        assert engine_view["missing_ranges"] == {batch_b: ["2-3"]}

        # 紧接着完整上传：通过批次和此前缺损区间都要能同时看到
        second = archive_service.receive_package(package_id, text)
        verification = second["verification"]
        assert verification["passed_batches"] == [batch_a, batch_b]
        assert verification["complete_package"] is True
        assert verification["verified_shards"] == 7
        # 两次接收回执都保留，可追溯哪一次只核到前缀
        receipts = second["package"]["receipts"]
        assert len(receipts) >= 2
        assert receipts[-1]["complete_package"] is True

    def test_replay_task_records_partial_scope_per_batch(self, pipeline):
        batch_a, batch_b = _seed_two_batches(pipeline)
        package_id = archive_service.build_package([batch_a, batch_b])["package"]["package_id"]
        text, _ = archive_service.package_text(package_id)
        archive_service.receive_package(package_id, "\n".join(text.splitlines()[:-3]) + "\n")
        archive_service.replay_package(package_id, "field-rule-race", batch_nos=[batch_b])

        tasks = archive_service.list_replay_tasks()["items"]
        assert tasks[0]["archive_package_id"] == package_id
        assert tasks[0]["scope"] == "局部重放"
        assert tasks[0]["complete_replay"] is False
        assert tasks[0]["missing_ranges"] == {batch_b: ["2-3"]}
