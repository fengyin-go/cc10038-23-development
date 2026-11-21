"""可重放归档包的构建、核验、接收登记与归档重放。

数据流：

1. `build_package` 从 raw_records 抽取原始日志，生成带批次/原始行/内容校验的分片文件；
2. `receive_package` 接收文件，逐分片核验，按“批次 + 原始行号”幂等登记 raw_records；
3. `replay_package` 只读取已通过核验的分片，产出标准化明细并标记本次重放范围；
4. 核验结果同时写入可用文件清单，供值班人员追溯使用过的原文。

关键约束：局部核验通过只能产生“局部重放”，任何缺少分片或校验失败的批次都会
在结果里给出缺损的原文区间，不能被当成整包重放。
"""
from __future__ import annotations

import copy
import json
from typing import Any

from app.services.archive_package import (
    archive_registry,
    body_checksum,
    content_checksum,
    empty_verification,
    make_footer,
    make_manifest,
    make_shard,
    manifest_checksum,
    natural_key,
    new_package_record,
    now_iso,
    number_ranges,
    package_checksum,
    parse_package_text,
    public_package,
    serialize_package,
    sha256_text,
)
from app.services.pipeline import pipeline
from app.store import store


# ---------------------------------------------------------------------------
# 构建归档包
# ---------------------------------------------------------------------------

def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def _find_batch(batch_no: str) -> dict[str, Any] | None:
    for row in store.rows("ingest_batch"):
        if str(row.get("批次编号")) == str(batch_no):
            return row
    return None


def _batch_raw_records(batch_no: str) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows("raw_records")
        if str(row.get("批次编号")) == str(batch_no)
    ]
    return sorted(rows, key=lambda row: natural_key(row.get("原始行号", row.get("id"))))


def build_package(
    batch_nos: list[str],
    archive_name: str | None = None,
    strategy_id: int | None = None,
) -> dict[str, Any]:
    """把若干批次的原始日志打成可下载、可重放的归档包。"""
    if not batch_nos:
        return {"ok": False, "message": "至少选择一个采集批次"}

    with archive_registry.lock:
        unique_batches: list[str] = []
        for batch_no in batch_nos:
            text = str(batch_no).strip()
            if text and text not in unique_batches:
                unique_batches.append(text)

        package_id = archive_registry.next_package_id()
        manifest_batches: list[dict[str, Any]] = []
        shards: list[dict[str, Any]] = []
        sequence = 0

        for batch_no in unique_batches:
            batch = _find_batch(batch_no)
            if batch is None:
                return {"ok": False, "message": f"采集批次 {batch_no} 不存在，无法归档"}
            raw_rows = _batch_raw_records(batch_no)
            if not raw_rows:
                return {"ok": False, "message": f"采集批次 {batch_no} 没有原始日志，无法归档"}

            line_nos: list[str] = []
            for raw in raw_rows:
                line_no = str(raw.get("原始行号") or raw.get("id"))
                body = str(raw.get("原始正文", ""))
                sequence += 1
                shard = make_shard(
                    seq=sequence,
                    shard_id=f"{package_id}-S{sequence:04d}",
                    batch_no=batch_no,
                    source_id=str(batch.get("所属数据源", raw.get("source_id", "archive"))),
                    line_no=line_no,
                    body=body,
                    collected_at=str(raw.get("采集时间戳") or now_iso()),
                )
                shard["raw_record_id"] = raw.get("id")
                shards.append(shard)
                line_nos.append(line_no)

            manifest_batches.append({
                "batch_no": batch_no,
                "source_id": str(batch.get("所属数据源", "archive")),
                "batch_status": str(batch.get("status", "已完成")),
                "time_window": str(batch.get("采集时间窗口", "")),
                "line_count": len(line_nos),
                "line_nos": line_nos,
            })

        name = (archive_name or f"archive-{package_id}").strip()
        manifest = make_manifest(package_id, name, manifest_batches, len(shards))

        # 下载文件里的分片不带 raw_record_id 这类库内字段，保持文件自解释。
        file_manifest = copy.deepcopy(manifest)
        file_shards = [
            {key: value for key, value in shard.items() if key != "raw_record_id"}
            for shard in shards
        ]
        package_text = serialize_package(file_manifest, file_shards)
        record = new_package_record(package_id, archive_name=name)
        record.update({
            "origin": "构建",
            "status": "可下载",
            "manifest": file_manifest,
            "shard_count": len(file_shards),
            "package_checksum": make_footer(file_shards)["package_checksum"],
            "package_text": package_text,
        })
        analysis = analyze_package_text(package_text, fallback_manifest=file_manifest)
        record["verification"] = analysis["public"]
        record["revision"] = sha256_text(package_text)[:16]
        archive_registry.save(record)

        if strategy_id is not None:
            strategy = store.find("data_archive", int(strategy_id))
            if strategy is not None:
                strategy["status"] = "已执行"
                strategy["pending"] = False
                strategy["最后归档包"] = package_id
                strategy["已归档批次"] = ",".join(unique_batches)
                strategy["归档时间"] = now_iso()

        return {
            "ok": True,
            "message": f"归档包 {package_id} 已生成，包含 {len(unique_batches)} 个批次、{len(shards)} 条原始日志",
            "package": public_package(record),
            "download_url": f"/api/archive/packages/{package_id}/download",
            "manifest": file_manifest,
        }


# ---------------------------------------------------------------------------
# 解析与逐分片核验
# ---------------------------------------------------------------------------

def _expected_items(manifest: dict[str, Any] | None) -> list[tuple[str, str]]:
    """按清单顺序展开“批次 + 原始行号”，用于给损坏行定位缺损区间。"""
    if not manifest:
        return []
    items: list[tuple[str, str]] = []
    for batch in manifest.get("batches") or []:
        batch_no = str(batch.get("batch_no", ""))
        line_nos = batch.get("line_nos")
        if isinstance(line_nos, list) and line_nos:
            for line_no in line_nos:
                items.append((batch_no, str(line_no)))
            continue
        count = int(batch.get("line_count", 0) or 0)
        for index in range(1, count + 1):
            items.append((batch_no, str(index)))
    return items


def _shard_slots(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    """把清单与 footer 之间的每一行都当作一个分片槽位，坏行也占位。"""
    return parsed.get("slots") or []


def analyze_package_text(
    text: str,
    fallback_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """核验归档文本，返回公开核验结果和内部通过核验的分片。"""
    parsed = parse_package_text(text)
    manifest = parsed.get("manifest") or fallback_manifest
    manifest_source = "包内清单"
    if parsed.get("manifest") is None and fallback_manifest is not None:
        manifest_source = "登记清单"
    manifest_ok = False
    manifest_detail = "缺少清单"
    if manifest:
        checksum = str(manifest.get("manifest_checksum") or "")
        if checksum:
            manifest_ok = checksum == manifest_checksum(manifest)
            manifest_detail = "校验通过" if manifest_ok else "校验失败"
        else:
            # 兼容没有清单校验字段的旧包：内容核验仍然有效，但不能算整包通过。
            manifest_ok = True
            manifest_detail = "未提供校验"

    expected = _expected_items(manifest)
    expected_set = set(expected)
    slots = _shard_slots(parsed)

    valid_shards: dict[tuple[str, str], dict[str, Any]] = {}
    invalid_shards: list[dict[str, Any]] = []
    missing: set[tuple[str, str]] = set()
    shard_ids: set[str] = set()

    for slot in slots:
        ordinal = int(slot.get("ordinal", 0))
        expected_slot = expected[ordinal - 1] if 0 < ordinal <= len(expected) else None
        item = slot.get("item")
        if item is None:
            if expected_slot:
                missing.add(expected_slot)
            invalid_shards.append({
                "ordinal": ordinal,
                "expected_batch": expected_slot[0] if expected_slot else "",
                "expected_line": expected_slot[1] if expected_slot else "",
                "reason": slot.get("reason", "坏行"),
            })
            continue

        batch_no = str(item.get("batch_no", ""))
        line_no = str(item.get("line_no", ""))
        body = str(item.get("body", ""))
        issues: list[str] = []

        required = ("shard_id", "batch_no", "line_no", "body")
        absent = [field for field in required if not str(item.get(field, "")).strip()]
        if absent:
            issues.append(f"缺少字段：{'、'.join(absent)}")
        if expected_set and (batch_no, line_no) not in expected_set:
            issues.append("分片声明的批次或原始行不在清单中")
        elif not expected_set and not manifest:
            issues.append("缺少工作清单，无法确认原始行引用")
        if str(item.get("body_checksum", "")) != body_checksum(body):
            issues.append("正文摘要不匹配")
        if str(item.get("content_checksum", "")) != content_checksum(batch_no, line_no, body):
            issues.append("批次/行号与正文的联合校验不匹配")
        byte_length = item.get("byte_length")
        if byte_length is not None and int(byte_length) != len(body.encode("utf-8")):
            issues.append("正文长度不匹配")
        shard_id = str(item.get("shard_id", ""))
        if shard_id and shard_id in shard_ids:
            issues.append("分片编号重复")
        if (batch_no, line_no) in valid_shards:
            issues.append("原始行重复出现")
        if int(item.get("seq", ordinal) or ordinal) != ordinal:
            # 顺序异常不影响内容校验，但会标注出来供值班员排查。
            item["_sequence_warning"] = f"分片序号 {item.get('seq')} 与文件位置 {ordinal} 不一致"

        if issues:
            if expected_slot:
                missing.add(expected_slot)
            invalid_shards.append({
                "ordinal": ordinal,
                "shard_id": shard_id,
                "batch_no": batch_no,
                "line_no": line_no,
                "expected_batch": expected_slot[0] if expected_slot else "",
                "expected_line": expected_slot[1] if expected_slot else "",
                "reason": "；".join(issues),
            })
            continue

        shard_ids.add(shard_id)
        valid_shards[(batch_no, line_no)] = item

    verified_set = set(valid_shards)
    batches: list[dict[str, Any]] = []
    for batch in (manifest or {}).get("batches") or []:
        batch_no = str(batch.get("batch_no", ""))
        line_nos = batch.get("line_nos")
        if isinstance(line_nos, list) and line_nos:
            expected_lines = [str(line) for line in line_nos]
        else:
            expected_lines = [str(i) for i in range(1, int(batch.get("line_count", 0) or 0) + 1)]
        verified_lines = [line for line in expected_lines if (batch_no, line) in verified_set]
        missing_lines = [line for line in expected_lines if (batch_no, line) not in verified_set]
        if not expected_lines:
            batch_status = "未核验"
        elif not missing_lines:
            batch_status = "通过"
        elif verified_lines:
            batch_status = "部分通过"
        else:
            batch_status = "缺损"
        shard_details = [
            {
                "shard_id": valid_shards[(batch_no, line)].get("shard_id"),
                "line_no": line,
                "raw_record_id": valid_shards[(batch_no, line)].get("raw_record_id"),
                "body_checksum": valid_shards[(batch_no, line)].get("body_checksum"),
                "content_checksum": valid_shards[(batch_no, line)].get("content_checksum"),
            }
            for line in verified_lines
        ]
        batches.append({
            "batch_no": batch_no,
            "source_id": str(batch.get("source_id", "archive")),
            "status": batch_status,
            "readable": bool(verified_lines),
            "expected_line_count": len(expected_lines),
            "verified_line_count": len(verified_lines),
            "missing_line_nos": missing_lines,
            "missing_ranges": number_ranges(missing_lines),
            "verified_lines": verified_lines,
            "shards": shard_details,
        })

    # 清单里没出现、但文件里多出来的分片单独列出来，避免被静默忽略。
    extraneous = [
        {"batch_no": batch_no, "line_no": line_no}
        for (batch_no, line_no) in valid_shards
        if expected_set and (batch_no, line_no) not in expected_set
    ]

    all_expected = set(expected)
    all_verified = set(valid_shards) & all_expected
    all_missing = all_expected - all_verified
    missing_by_batch: dict[str, list[str]] = {}
    for batch_no, line_no in sorted(all_missing, key=lambda item: (natural_key(item[0]), natural_key(item[1]))):
        missing_by_batch.setdefault(batch_no, []).append(line_no)

    footer = parsed.get("footer")
    footer_ok = bool(
        footer
        and str(footer.get("package_checksum", ""))
        == package_checksum([slot["item"] for slot in slots if slot.get("item")])
        and int(footer.get("shard_count", -1)) == len([slot for slot in slots])
    )
    malformed_slots = [slot for slot in slots if slot.get("item") is None]
    package_integrity = "通过"
    if not footer:
        package_integrity = "截断：缺少文件尾"
    elif not footer_ok:
        package_integrity = "校验失败：整包摘要或分片数量不一致"
    elif malformed_slots:
        package_integrity = "存在无法解析的分片"

    complete_package = bool(
        manifest_ok
        and manifest
        and footer_ok
        and not malformed_slots
        and not invalid_shards
        and not all_missing
        and not extraneous
        and all_expected
    )

    if complete_package:
        state = "核验通过"
    elif valid_shards:
        state = "部分通过"
    else:
        state = "核验失败"

    readable_batches = [batch["batch_no"] for batch in batches if batch["readable"]]
    passed_batches = [batch["batch_no"] for batch in batches if batch["status"] == "通过"]
    public = {
        "state": state,
        "manifest_source": manifest_source,
        "manifest_integrity": manifest_detail,
        "package_integrity": package_integrity,
        "complete_package": complete_package,
        "verified_shards": len(valid_shards),
        "total_shards": int((manifest or {}).get("shard_count", len(slots)) or len(slots)),
        "valid_shards": len(valid_shards),
        "invalid_shards": len(invalid_shards),
        "processed_shards": len(slots),
        "readable_batches": readable_batches,
        "passed_batches": passed_batches,
        "missing_ranges": {
            batch_no: number_ranges(lines) for batch_no, lines in missing_by_batch.items()
        },
        "missing_lines": missing_by_batch,
        "batches": batches,
        "invalid": [
            {key: value for key, value in item.items() if key != "item"}
            for item in invalid_shards
        ],
        "extraneous_shards": extraneous,
        "malformed_lines": parsed.get("malformed", []),
        "verified_at": now_iso(),
        "revision": sha256_text(str(text))[:16],
    }
    return {"public": public, "valid_shards": valid_shards, "manifest": manifest}


# ---------------------------------------------------------------------------
# 接收与幂等登记
# ---------------------------------------------------------------------------

def _ensure_batch_row(
    manifest_batch: dict[str, Any],
    batch_result: dict[str, Any],
    package_id: str,
) -> dict[str, Any]:
    batch_no = str(manifest_batch.get("batch_no", ""))
    source_id = str(manifest_batch.get("source_id") or "archive")
    row = _find_batch(batch_no)
    now = now_iso()
    if row is None:
        row = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": str(manifest_batch.get("time_window") or now[:10]),
            "记录总数": "0",
            "批次状态": "归档接收",
            "status": "已完成" if batch_result.get("status") == "通过" else (
                "部分失败" if batch_result.get("readable") else "待采集"
            ),
            "pending": not bool(batch_result.get("status") == "通过"),
            "abnormal": bool(batch_result.get("missing_line_nos")),
            "from_archive": True,
            "archive_package_ids": [package_id],
            "archive_readable": bool(batch_result.get("readable")),
            "archive_missing_ranges": batch_result.get("missing_ranges", []),
            "registered_at": now,
        }
        store.rows("ingest_batch").append(row)
    else:
        ids = row.setdefault("archive_package_ids", [])
        if package_id not in ids:
            ids.append(package_id)
        row["from_archive"] = True
        row["archive_readable"] = bool(batch_result.get("readable"))
        row["archive_missing_ranges"] = batch_result.get("missing_ranges", [])
        row["archive_registered_at"] = now
        # 归档接收只补充来源信息，不覆盖已有采集状态。
        if batch_result.get("status") == "通过" and row.get("status") in ("待采集", "采集中"):
            row["status"] = "已完成"
            row["pending"] = False
    return row


def register_verified_shards(
    record: dict[str, Any],
    analysis: dict[str, Any],
) -> dict[str, Any]:
    """把通过核验的分片按“批次 + 原始行号”幂等登记到 raw_records。

    已存在的同一行不会重复生成记录；正文不一致时保留原记录并报告冲突。
    """
    package_id = str(record["package_id"])
    manifest = analysis.get("manifest") or record.get("manifest") or {}
    manifest_batches = {
        str(batch.get("batch_no")): batch for batch in manifest.get("batches") or []
    }
    valid_shards: dict[tuple[str, str], dict[str, Any]] = analysis["valid_shards"]
    verification = analysis["public"]

    raw_rows = store.rows("raw_records")
    registered = record.setdefault("registered", {})
    raw_record_ids = record.setdefault("raw_record_ids", {})
    registered_batches = record.setdefault("registered_batches", [])

    new_count = 0
    existing_count = 0
    conflicts: list[dict[str, Any]] = []
    per_batch: dict[str, dict[str, Any]] = {}

    for batch_result in verification.get("batches", []):
        batch_no = str(batch_result.get("batch_no"))
        if not batch_result.get("readable"):
            continue
        manifest_batch = manifest_batches.get(batch_no, {
            "batch_no": batch_no,
            "source_id": batch_result.get("source_id", "archive"),
        })
        batch_row = _ensure_batch_row(manifest_batch, batch_result, package_id)
        if batch_no not in registered_batches:
            registered_batches.append(batch_no)
        batch_registered = registered.setdefault(batch_no, {})
        batch_ids = raw_record_ids.setdefault(batch_no, {})
        per_batch[batch_no] = {
            "batch_id": batch_row.get("id"),
            "source_id": batch_row.get("所属数据源"),
            "new_records": 0,
            "existing_records": 0,
            "conflicts": 0,
        }

        for line_no in batch_result.get("verified_lines", []):
            shard = valid_shards.get((batch_no, str(line_no)))
            if shard is None:
                continue
            body = str(shard.get("body", ""))
            body_digest = str(shard.get("body_checksum", ""))
            content_digest = str(shard.get("content_checksum", ""))
            existing = next(
                (
                    row for row in raw_rows
                    if str(row.get("批次编号")) == batch_no
                    and str(row.get("原始行号")) == str(line_no)
                ),
                None,
            )
            if existing is not None:
                same_body = str(existing.get("原始正文", "")) == body
                if not same_body:
                    conflicts.append({
                        "batch_no": batch_no,
                        "line_no": str(line_no),
                        "raw_record_id": existing.get("id"),
                        "existing_checksum": existing.get("content_checksum")
                        or body_checksum(str(existing.get("原始正文", ""))),
                        "archive_checksum": content_digest,
                        "reason": "同一批次行号已存在不同正文，保留原记录",
                    })
                    batch_registered[str(line_no)] = {
                        "status": "冲突",
                        "raw_record_id": existing.get("id"),
                        "shard_id": shard.get("shard_id"),
                    }
                    per_batch[batch_no]["conflicts"] += 1
                    continue

                batch_ids[str(line_no)] = int(existing.get("id", 0))
                ids = existing.setdefault("archive_package_ids", [])
                if package_id not in ids:
                    ids.append(package_id)
                existing.setdefault("body_checksum", body_digest)
                existing.setdefault("content_checksum", content_digest)
                existing.setdefault("archive_shard_id", shard.get("shard_id"))
                existing["from_archive"] = True
                batch_registered[str(line_no)] = {
                    "status": "已确认",
                    "raw_record_id": existing.get("id"),
                    "shard_id": shard.get("shard_id"),
                }
                existing_count += 1
                per_batch[batch_no]["existing_records"] += 1
                continue

            raw_id = _next_id("raw_records")
            row = {
                "id": raw_id,
                "批次编号": batch_no,
                "原始行号": str(line_no),
                "采集时间戳": str(shard.get("collected_at") or now_iso()),
                "原始正文": body,
                "解析状态": "未解析",
                "status": "待解析",
                "pending": True,
                "abnormal": False,
                "source_id": batch_row.get("所属数据源", "archive"),
                "from_archive": True,
                "archive_package_id": package_id,
                "archive_shard_id": shard.get("shard_id"),
                "body_checksum": body_digest,
                "content_checksum": content_digest,
                "ingested_at": now_iso(),
            }
            raw_rows.append(row)
            batch_ids[str(line_no)] = raw_id
            batch_registered[str(line_no)] = {
                "status": "新登记",
                "raw_record_id": raw_id,
                "shard_id": shard.get("shard_id"),
            }
            new_count += 1
            per_batch[batch_no]["new_records"] += 1

        total = sum(
            1 for row in raw_rows if str(row.get("批次编号")) == batch_no
        )
        batch_row["记录总数"] = str(total)
        batch_row["from_archive"] = True
        if batch_result.get("status") == "通过" and batch_row.get("status") in ("待采集", "采集中", "部分失败"):
            batch_row["status"] = "已完成"
            batch_row["pending"] = False
            batch_row["abnormal"] = False
        elif batch_result.get("missing_ranges"):
            batch_row["abnormal"] = True
            batch_row["archive_missing_ranges"] = batch_result.get("missing_ranges")

    receipt = {
        "receipt_id": f"RCP-{len(record.get('receipts', [])) + 1:04d}",
        "received_at": now_iso(),
        "package_id": package_id,
        "revision": verification.get("revision"),
        "result": verification.get("state"),
        "complete_package": verification.get("complete_package"),
        "readable_batches": verification.get("readable_batches", []),
        "passed_batches": verification.get("passed_batches", []),
        "missing_ranges": verification.get("missing_ranges", {}),
        "new_records": new_count,
        "existing_records": existing_count,
        "conflicts": len(conflicts),
    }
    receipts = record.setdefault("receipts", [])
    receipts.append(receipt)
    record["registered"] = registered
    record["raw_record_ids"] = raw_record_ids
    record["registered_batches"] = registered_batches
    record["status"] = (
        "已接收" if verification.get("complete_package")
        else "部分接收" if verification.get("verified_shards")
        else "核验失败"
    )
    record["registered_at"] = receipt["received_at"]

    return {
        "receipt": receipt,
        "per_batch": per_batch,
        "new_records": new_count,
        "existing_records": existing_count,
        "conflicts": conflicts,
    }


def receive_package(package_id: str, archive_text: str) -> dict[str, Any]:
    """接收上传的归档文本：核验、登记、写可用文件清单，整个过程幂等。"""
    text = str(archive_text or "")
    if not text.strip():
        return {"ok": False, "message": "归档文件为空，无法接收"}

    with archive_registry.lock:
        record = archive_registry.ensure(package_id)
        previous_manifest = record.get("manifest")
        analysis = analyze_package_text(text, fallback_manifest=previous_manifest)
        verification = analysis["public"]
        manifest = analysis.get("manifest") or previous_manifest

        record["package_text"] = text
        record["format_version"] = str((manifest or {}).get("format_version", "1"))
        record["manifest"] = manifest
        record["shard_count"] = int((manifest or {}).get("shard_count", verification["total_shards"]))
        record["package_checksum"] = sha256_text(text)
        record["revision"] = verification["revision"]
        record["verification"] = verification
        record["origin"] = record.get("origin") or "接收"
        record["received_at"] = now_iso()

        registration = register_verified_shards(record, analysis)
        record["verification"]["registration"] = registration["receipt"]
        record["verification"]["registered_batches"] = list(record.get("registered_batches", []))

        message = (
            f"归档包 {package_id} 已接收：{verification['state']}，"
            f"可读批次 {len(verification['readable_batches'])} 个，"
            f"新登记 {registration['new_records']} 条，已确认 {registration['existing_records']} 条"
        )
        if verification.get("missing_ranges"):
            ranges = "；".join(
                f"{batch} 缺损 {','.join(ranges)}"
                for batch, ranges in verification["missing_ranges"].items()
            )
            message += f"；缺损区间：{ranges}"

        return {
            "ok": verification["state"] != "核验失败",
            "message": message,
            "package": public_package(record),
            "verification": verification,
            "registration": registration,
            "file_list": file_entries_for_package(record),
        }


def verify_package(package_id: str, archive_text: str | None = None) -> dict[str, Any]:
    """重新核验归档包；不传文本时使用上次接收/构建的内容。"""
    with archive_registry.lock:
        record = archive_registry.get(package_id)
        if record is None:
            return {"ok": False, "message": f"归档包 {package_id} 不存在"}
        text = archive_text if archive_text is not None else record.get("package_text", "")
        analysis = analyze_package_text(text, fallback_manifest=record.get("manifest"))
        record["verification"] = analysis["public"]
        if archive_text is not None:
            record["package_text"] = str(archive_text)
            record["revision"] = analysis["public"]["revision"]
        return {
            "ok": True,
            "message": f"归档包 {package_id} 核验完成：{analysis['public']['state']}",
            "package": public_package(record),
            "verification": analysis["public"],
        }


# ---------------------------------------------------------------------------
# 可用文件清单
# ---------------------------------------------------------------------------

def file_entries_for_package(record: dict[str, Any]) -> list[dict[str, Any]]:
    verification = record.get("verification") or empty_verification()
    raw_ids = record.get("raw_record_ids") or {}
    entries: list[dict[str, Any]] = []
    for batch in verification.get("batches", []):
        batch_no = str(batch.get("batch_no"))
        batch_ids = raw_ids.get(batch_no) or {}
        entries.append({
            "package_id": record.get("package_id"),
            "archive_name": record.get("archive_name"),
            "batch_no": batch_no,
            "source_id": batch.get("source_id"),
            "file_status": (
                "可用" if batch.get("status") == "通过"
                else "部分可用" if batch.get("readable")
                else "缺损"
            ),
            "readable": bool(batch.get("readable")),
            "expected_line_count": batch.get("expected_line_count", 0),
            "verified_line_count": batch.get("verified_line_count", 0),
            "missing_ranges": batch.get("missing_ranges", []),
            "raw_record_ids": list(batch_ids.values()),
            "registered_lines": len(batch_ids),
            "complete_package": bool(verification.get("complete_package")),
            "package_revision": record.get("revision", ""),
            "checked_at": verification.get("verified_at", ""),
            "replay_task_ids": list(record.get("replay_task_ids") or []),
        })
    return entries


def usable_file_list(readable_only: bool = False) -> dict[str, Any]:
    """可用文件清单只收录已接收过的归档包：构建完成但未接收的包还没有登记结果。"""
    with archive_registry.lock:
        entries: list[dict[str, Any]] = []
        for record in archive_registry.list():
            if record.get("status") == "可下载":
                continue
            for entry in file_entries_for_package(record):
                if readable_only and not entry["readable"]:
                    continue
                entry["package_status"] = record.get("status")
                entries.append(entry)
    return {"items": entries, "total": len(entries)}


def list_packages() -> dict[str, Any]:
    with archive_registry.lock:
        items = archive_registry.list_public()
    for item in items:
        item["download_url"] = f"/api/archive/packages/{item['package_id']}/download"
    return {"items": items, "total": len(items)}


def package_detail(package_id: str) -> dict[str, Any] | None:
    with archive_registry.lock:
        record = archive_registry.get(package_id)
        if record is None:
            return None
        detail = public_package(record)
        detail["manifest"] = record.get("manifest")
        detail["file_list"] = file_entries_for_package(record)
        detail["registered"] = record.get("registered", {})
        detail["download_url"] = f"/api/archive/packages/{package_id}/download"
        return detail


def package_text(package_id: str) -> tuple[str, str] | None:
    with archive_registry.lock:
        record = archive_registry.get(package_id)
        if record is None:
            return None
        return str(record.get("package_text", "")), str(record.get("archive_name") or package_id)


# ---------------------------------------------------------------------------
# 归档重放
# ---------------------------------------------------------------------------

def _resolve_parser_rule(rule_name: str) -> dict[str, Any]:
    rules = store.rows("field_parser")
    for rule in rules:
        if str(rule.get("规则名称")) == rule_name:
            return rule
    for rule in rules:
        if str(rule.get("解析规则版本")) == rule_name:
            return rule
    for rule in rules:
        if rule.get("status") == "已发布":
            return rule
    return {
        "规则名称": rule_name,
        "输出字段列表": "time,level,message",
        "status": "已发布",
        "_synthetic": True,
    }


def replay_package(
    package_id: str,
    new_parser_rule: str,
    batch_nos: list[str] | None = None,
    validate_entities: bool = False,
) -> dict[str, Any]:
    """只使用通过核验的正文和原始编号执行重放，并明确标注重放范围。"""
    with archive_registry.lock:
        record = archive_registry.get(package_id)
        if record is None:
            return {"ok": False, "message": f"归档包 {package_id} 不存在，请先接收归档文件"}
        text = record.get("package_text", "")
        if not text:
            return {"ok": False, "message": f"归档包 {package_id} 还没有可用的归档文件"}

        analysis = analyze_package_text(text, fallback_manifest=record.get("manifest"))
        verification = analysis["public"]
        record["verification"] = verification
        if not analysis["valid_shards"]:
            return {
                "ok": False,
                "message": f"归档包 {package_id} 没有通过核验的正文，不能重放",
                "verification": verification,
            }

        manifest = analysis.get("manifest") or record.get("manifest") or {}
        manifest_batches = [str(item.get("batch_no")) for item in manifest.get("batches") or []]
        requested = [str(item) for item in (batch_nos or manifest_batches) if str(item)]
        requested = list(dict.fromkeys(requested))

        batch_results = {str(item["batch_no"]): item for item in verification.get("batches", [])}
        executable = [
            batch_no for batch_no in requested
            if batch_results.get(batch_no, {}).get("readable")
        ]
        skipped = [batch_no for batch_no in requested if batch_no not in executable]
        # 缺损区间按整包清单给出，不只按本次请求的批次：
        # 即使值班员只请求了第一批正文，也必须同时看到他没请求到的批次缺了什么。
        verified_missing = verification.get("missing_lines") or {}
        missing_ranges = {
            str(batch_no): number_ranges(lines)
            for batch_no, lines in verified_missing.items()
            if lines
        }

        rule = _resolve_parser_rule(new_parser_rule)
        rule_version = str(rule.get("规则名称") or new_parser_rule)
        schema_ver = next(
            (row for row in store.rows("schema_version") if row.get("status") == "已发布"),
            None,
        )

        replay_id = _next_id("replay_engine")
        now = now_iso()
        source_refs: list[dict[str, Any]] = []
        created = 0
        updated = 0
        parsed_rows = store.rows("parsed_entity")
        target_refs: list[tuple[str, str, dict[str, Any]]] = []

        for batch_no in executable:
            batch_result = batch_results.get(batch_no, {})
            for line_no in batch_result.get("verified_lines", []):
                shard = analysis["valid_shards"].get((batch_no, str(line_no)))
                if shard is not None:
                    target_refs.append((batch_no, str(line_no), shard))

        for batch_no, line_no, shard in target_refs:
            body = str(shard.get("body", ""))
            raw_id = None
            batch_ids = (record.get("raw_record_ids") or {}).get(batch_no) or {}
            if str(line_no) in batch_ids:
                raw_id = int(batch_ids[str(line_no)])
            if raw_id is None:
                # 尚未登记时按批次行号兜底查找，绝不按正文模糊匹配。
                existing_raw = next(
                    (
                        row for row in store.rows("raw_records")
                        if str(row.get("批次编号")) == batch_no
                        and str(row.get("原始行号")) == str(line_no)
                    ),
                    None,
                )
                raw_id = int(existing_raw["id"]) if existing_raw else None
            if raw_id is None:
                continue

            fields = pipeline._parse_body(body, rule)
            fields_json = json.dumps(fields, ensure_ascii=False)
            content_digest = str(shard.get("content_checksum") or content_checksum(batch_no, line_no, body))
            existing_entity = next(
                (
                    row for row in parsed_rows
                    if str(row.get("来源批次")) == batch_no
                    and str(row.get("原始记录ID")) == str(raw_id)
                    and str(row.get("解析规则版本")) == rule_version
                    and str(row.get("archive_package_id", "")) == package_id
                ),
                None,
            )
            if existing_entity is not None:
                entity = existing_entity
                entity["标准化字段集"] = fields_json
                entity["校验状态"] = ""
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["abnormal"] = False
                entity["replayed_at"] = now
                entity["content_digest"] = sha256_text(fields_json)[:16]
                replay_ids = entity.setdefault("replay_task_ids", [])
                if str(replay_id) not in replay_ids:
                    replay_ids.append(str(replay_id))
                updated += 1
            else:
                entity = {
                    "id": _next_id("parsed_entity"),
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule_version,
                    "标准化字段集": fields_json,
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": now,
                    "content_digest": sha256_text(fields_json)[:16],
                    "archive_package_id": package_id,
                    "archive_shard_id": shard.get("shard_id"),
                    "原始行号": line_no,
                    "原始正文摘要": shard.get("body_checksum"),
                    "content_checksum": content_digest,
                    "replayed_from_archive": True,
                    "replay_task_ids": [str(replay_id)],
                }
                parsed_rows.append(entity)
                created += 1

            raw_row = store.find("raw_records", raw_id)
            if raw_row is not None:
                raw_row["status"] = "已解析"
                raw_row["解析状态"] = "已解析"
                raw_row["pending"] = False
                raw_row["parsed_entity_id"] = entity["id"]

            source_refs.append({
                "batch_no": batch_no,
                "line_no": line_no,
                "raw_record_id": raw_id,
                "shard_id": shard.get("shard_id"),
                "body_checksum": shard.get("body_checksum"),
                "content_checksum": content_digest,
                "body_preview": body[:120],
                "parsed_entity_id": entity["id"],
            })

        validated = None
        if validate_entities and target_refs:
            # 只校验本次重放产出的明细，避免把其他来源的待校验数据一起改掉。
            entity_ids = {int(ref["parsed_entity_id"]) for ref in source_refs}
            targeted = [
                row for row in store.rows("parsed_entity")
                if int(row.get("id", 0)) in entity_ids
            ]
            for row in targeted:
                row["status"] = "待校验"
            passed = failed = 0
            for row in targeted:
                try:
                    fields_data = json.loads(row.get("标准化字段集", "{}"))
                except (json.JSONDecodeError, TypeError):
                    fields_data = {}
                if fields_data and all(str(value).strip() for value in fields_data.values()):
                    row["status"] = "校验通过"
                    row["校验状态"] = "校验通过"
                    row["pending"] = False
                    row["validated_at"] = now
                    passed += 1
                else:
                    row["status"] = "校验失败"
                    row["校验状态"] = "校验失败"
                    row["abnormal"] = True
                    failed += 1
            validated = {"passed": passed, "failed": failed, "targeted": len(targeted)}

        full_scope = bool(
            verification.get("complete_package")
            and set(executable) == set(manifest_batches)
            and not skipped
            and not missing_ranges
        )
        executed_batches = list(dict.fromkeys(batch_no for batch_no, _, _ in target_refs))
        scope = "整包重放" if full_scope else "局部重放"
        status = "已完成" if full_scope else "部分失败"
        produced = len(target_refs)

        if not target_refs:
            status = "部分失败"
            scope = "局部重放"

        missing_text = "；".join(
            f"{batch} 缺损 {','.join(ranges)}" for batch, ranges in missing_ranges.items()
        )
        scope_note = (
            "已覆盖整包全部通过核验的分片"
            if full_scope
            else (
                f"仅重放已通过核验的 {len(executed_batches)} 个可读批次、{produced} 行"
                + (f"；未执行批次：{','.join(skipped)}" if skipped else "")
                + (f"；缺损区间：{missing_text}" if missing_text else "")
            )
        )

        task = {
            "id": replay_id,
            "历史批次范围": ",".join(executed_batches) if executed_batches else ",".join(requested),
            "新解析规则版本": rule_version,
            "重放状态": status,
            "产出记录数": str(produced),
            "执行时间": now,
            "status": status,
            "pending": False,
            "abnormal": not full_scope,
            "archive_package_id": package_id,
            "requested_batches": requested,
            "executed_batches": executed_batches,
            "skipped_batches": skipped,
            "missing_ranges": missing_ranges,
            "scope": scope,
            "complete_replay": full_scope,
            "scope_note": scope_note,
            "used_line_count": produced,
            "created_entities": created,
            "updated_entities": updated,
            "source_refs": source_refs,
            "validated": validated,
            "package_revision": verification.get("revision"),
            "package_complete": verification.get("complete_package"),
        }
        store.rows("replay_engine").append(task)

        task_ids = record.setdefault("replay_task_ids", [])
        if str(replay_id) not in task_ids:
            task_ids.append(str(replay_id))
        record["last_replay_id"] = replay_id
        record["last_replay_scope"] = scope

        journal = {
            "id": _next_id("parse_journal"),
            "操作类型": "归档重放",
            "批次编号": ",".join(executed_batches) if executed_batches else "",
            "执行时间": now,
            "影响的记录数": str(produced),
            "执行结果": "成功" if full_scope else "部分成功",
            "status": "成功" if full_scope else "部分成功",
            "pending": not full_scope,
            "abnormal": not full_scope,
            "archive_package_id": package_id,
            "replay_id": str(replay_id),
            "scope": scope,
            "missing_ranges": missing_ranges,
        }
        store.rows("parse_journal").append(journal)

        for batch_no in executed_batches:
            store.rows("data_lineage").append({
                "id": _next_id("data_lineage"),
                "来源批次": batch_no,
                "解析规则版本": rule_version,
                "标准化版本": schema_ver.get("版本号") if schema_ver else "v1.0",
                "下游消费标识": f"replay-{replay_id}",
                "记录时间": now,
                "status": "已记录",
                "pending": False,
                "abnormal": not full_scope,
                "archive_package_id": package_id,
                "replay_id": str(replay_id),
                "scope": scope,
            })

        message = (
            f"归档包 {package_id} {scope}完成：执行 {len(executed_batches)} 个批次、"
            f"{produced} 行原始日志，新增标准化明细 {created} 条、更新 {updated} 条"
        )
        if not full_scope:
            message += f"；{scope_note}"

        return {
            "ok": True,
            "message": message,
            "replay": _public_task(task),
            "replayed_count": produced,
            "created": created,
            "updated": updated,
            "scope": scope,
            "complete_replay": full_scope,
            "executed_batches": executed_batches,
            "skipped_batches": skipped,
            "missing_ranges": missing_ranges,
            "used_sources": source_refs,
            "validation": validated,
        }


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in task.items() if key != "source_refs"}


def list_replay_tasks() -> dict[str, Any]:
    rows = [
        _public_task(row) for row in store.rows("replay_engine")
        if row.get("archive_package_id")
    ]
    rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
    return {"items": rows, "total": len(rows)}


def replay_task_detail(task_id: int) -> dict[str, Any] | None:
    for row in store.rows("replay_engine"):
        if int(row.get("id", 0)) == int(task_id):
            detail = _public_task(row)
            detail["package_id"] = row.get("archive_package_id")
            return detail
    return None


def replay_task_sources(task_id: int) -> dict[str, Any] | None:
    for row in store.rows("replay_engine"):
        if int(row.get("id", 0)) != int(task_id):
            continue
        sources: list[dict[str, Any]] = []
        for ref in row.get("source_refs") or []:
            raw = store.find("raw_records", int(ref.get("raw_record_id", 0)))
            entity = store.find("parsed_entity", int(ref.get("parsed_entity_id", 0)))
            sources.append({
                **ref,
                "原始正文": raw.get("原始正文") if raw else "",
                "标准化字段集": entity.get("标准化字段集") if entity else "",
                "校验状态": entity.get("status") if entity else "",
            })
        return {
            "replay_id": task_id,
            "scope": row.get("scope"),
            "complete_replay": row.get("complete_replay"),
            "missing_ranges": row.get("missing_ranges", {}),
            "skipped_batches": row.get("skipped_batches", []),
            "items": sources,
            "total": len(sources),
        }
    return None


def archive_strategy_batches(strategy_entry: dict[str, Any]) -> list[str]:
    """执行归档策略时的默认范围：有原始日志且尚未归档的批次。"""
    archived: set[str] = set()
    with archive_registry.lock:
        for record in archive_registry.list():
            archived.update(record.get("registered_batches") or [])
            for batch in (record.get("manifest") or {}).get("batches") or []:
                if record.get("origin") == "构建":
                    archived.add(str(batch.get("batch_no")))
    batch_nos: list[str] = []
    for batch in store.rows("ingest_batch"):
        batch_no = str(batch.get("批次编号", ""))
        if not batch_no or batch_no in archived:
            continue
        if _batch_raw_records(batch_no):
            batch_nos.append(batch_no)
    return batch_nos
