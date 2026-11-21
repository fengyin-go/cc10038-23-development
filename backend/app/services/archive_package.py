"""可重放归档包：文件格式、内容校验与进程内包清单。

归档包是一份按行分片的 NDJSON 文件：

1. 第一行是 manifest，声明包内有哪些批次、每个批次应包含哪些原始行号；
2. 之后每个分片一行，逐条绑定“批次 + 原始行号 + 原始正文 + 内容校验”；
3. 最后一行是 footer，记录分片总数和整包校验和。

文件可以安全地被截断或改坏后重新上传：解析器会保留能够独立核验的前缀分片，
把尾部缺失或校验失败的部分归到对应批次的“缺损原文区间”，而不是把局部当成整包。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from typing import Any, Iterable

FORMAT_VERSION = "1"
MANIFEST_KIND = "manifest"
SHARD_KIND = "shard"
FOOTER_KIND = "footer"

# 参与整包校验的分片字段；不依赖 JSON 文本的空白和键顺序，便于跨环境传输。
SHARD_FINGERPRINT_FIELDS = (
    "kind",
    "seq",
    "shard_id",
    "batch_no",
    "source_id",
    "line_no",
    "collected_at",
    "body",
    "body_checksum",
    "content_checksum",
    "byte_length",
)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def body_checksum(body: str) -> str:
    """原始正文自身的摘要。"""
    return sha256_text(str(body))


def content_checksum(batch_no: str, line_no: str, body: str) -> str:
    """正文 + 原始行引用的联合摘要。

    只改正文、只改批次号、只改行号，都会让校验失败，从而保证文件和原始行引用对得上。
    """
    payload = canonical_json({
        "batch_no": str(batch_no),
        "line_no": str(line_no),
        "body": str(body),
    })
    return sha256_text(payload)


def shard_fingerprint(shard: dict[str, Any]) -> str:
    return canonical_json({field: shard.get(field) for field in SHARD_FINGERPRINT_FIELDS})


def package_checksum(shards: Iterable[dict[str, Any]]) -> str:
    return sha256_text("\n".join(shard_fingerprint(shard) for shard in shards))


def manifest_checksum(manifest: dict[str, Any]) -> str:
    payload = {key: value for key, value in manifest.items() if key != "manifest_checksum"}
    return sha256_text(canonical_json(payload))


def natural_key(value: Any) -> tuple[int, Any]:
    text = str(value)
    return (0, int(text)) if text.isdigit() else (1, text)


def number_ranges(values: Iterable[Any]) -> list[str]:
    """把行号集合压成区间文本，例如 {7,8,9,12} -> ["7-9", "12"]。"""
    numbers = sorted({int(value) for value in values if str(value).isdigit()})
    others = sorted({str(value) for value in values if not str(value).isdigit()})
    ranges: list[str] = []
    start = previous = None
    for value in numbers:
        if start is None:
            start = previous = value
            continue
        if value == previous + 1:
            previous = value
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    if start is not None:
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ranges + others


def make_manifest(
    package_id: str,
    archive_name: str,
    batches: list[dict[str, Any]],
    shard_count: int,
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "kind": MANIFEST_KIND,
        "format_version": FORMAT_VERSION,
        "package_id": package_id,
        "archive_name": archive_name,
        "created_at": now_iso(),
        "batches": batches,
        "shard_count": shard_count,
        "record_count": sum(int(item.get("line_count", 0)) for item in batches),
    }
    manifest["manifest_checksum"] = manifest_checksum(manifest)
    return manifest


def make_shard(
    *,
    seq: int,
    shard_id: str,
    batch_no: str,
    source_id: str,
    line_no: str,
    body: str,
    collected_at: str | None = None,
) -> dict[str, Any]:
    body_text = str(body)
    return {
        "kind": SHARD_KIND,
        "seq": seq,
        "shard_id": shard_id,
        "batch_no": str(batch_no),
        "source_id": str(source_id),
        "line_no": str(line_no),
        "collected_at": collected_at or now_iso(),
        "body": body_text,
        "body_checksum": body_checksum(body_text),
        "content_checksum": content_checksum(str(batch_no), str(line_no), body_text),
        "byte_length": len(body_text.encode("utf-8")),
    }


def make_footer(shards: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": FOOTER_KIND,
        "shard_count": len(shards),
        "package_checksum": package_checksum(shards),
    }


def serialize_package(manifest: dict[str, Any], shards: list[dict[str, Any]]) -> str:
    lines = [canonical_json(manifest)]
    lines.extend(canonical_json(shard) for shard in shards)
    lines.append(canonical_json(make_footer(shards)))
    return "\n".join(lines) + "\n"


def parse_package_text(text: str) -> dict[str, Any]:
    """解析归档文本，坏行不会中断整份文件，交给核验阶段归因到批次。

    每个非清单/非文件尾的有效行都是一个“分片槽位”，序号按文件中的物理顺序分配；
    无法解析的行也占一个槽位，这样缺失内容才能对应回清单里的原始行号。
    """
    raw_lines = str(text or "").lstrip("﻿").splitlines()
    slots: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    manifest: dict[str, Any] | None = None
    footer: dict[str, Any] | None = None

    for index, raw_line in enumerate(raw_lines, start=1):
        if not raw_line.strip():
            continue
        try:
            item = json.loads(raw_line)
        except (json.JSONDecodeError, TypeError) as exc:
            reason = f"JSON 无法解析: {exc}"
            slots.append({"ordinal": len(slots) + 1, "line": index, "item": None, "reason": reason})
            malformed.append({"line": index, "reason": reason})
            continue
        if not isinstance(item, dict):
            reason = "分片不是 JSON 对象"
            slots.append({"ordinal": len(slots) + 1, "line": index, "item": None, "reason": reason})
            malformed.append({"line": index, "reason": reason})
            continue
        kind = item.get("kind")
        if kind == MANIFEST_KIND and manifest is None:
            manifest = item
        elif kind == SHARD_KIND:
            slots.append({"ordinal": len(slots) + 1, "line": index, "item": item})
        elif kind == FOOTER_KIND:
            footer = item
        else:
            reason = f"未知行类型: {kind!r}"
            slots.append({"ordinal": len(slots) + 1, "line": index, "item": None, "reason": reason})
            malformed.append({"line": index, "reason": reason})

    return {
        "manifest": manifest,
        "slots": slots,
        "footer": footer,
        "malformed": malformed,
        "line_count": len([line for line in raw_lines if line.strip()]),
    }


class ArchivePackageRegistry:
    """保存构建出来的包和接收侧核验结果，供“可用文件清单”和重放任务共享。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._packages: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._counter = 0

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def reset(self) -> None:
        with self._lock:
            self._packages.clear()
            self._order.clear()
            self._counter = 0

    def next_package_id(self) -> str:
        with self._lock:
            while True:
                self._counter += 1
                candidate = f"ARC-{time.strftime('%Y%m%d')}-{self._counter:04d}"
                if candidate not in self._packages:
                    return candidate

    def save(self, record: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            package_id = str(record["package_id"])
            if package_id not in self._packages:
                self._order.append(package_id)
            self._packages[package_id] = record
            return record

    def get(self, package_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._packages.get(package_id)

    def ensure(self, package_id: str) -> dict[str, Any]:
        with self._lock:
            record = self._packages.get(package_id)
            if record is None:
                record = new_package_record(package_id)
                self._order.append(package_id)
                self._packages[package_id] = record
            return record

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._packages[package_id] for package_id in self._order]

    def list_public(self) -> list[dict[str, Any]]:
        with self._lock:
            return [public_package(self._packages[key]) for key in self._order]


def new_package_record(package_id: str, *, archive_name: str = "") -> dict[str, Any]:
    return {
        "package_id": package_id,
        "archive_name": archive_name or package_id,
        "created_at": now_iso(),
        "format_version": FORMAT_VERSION,
        "manifest": None,
        "shard_count": 0,
        "package_checksum": "",
        "package_text": "",
        "origin": "接收",
        "status": "已接收",
        "verification": empty_verification(),
        "registered": {},
        "registered_batches": [],
        "raw_record_ids": {},
        "replay_task_ids": [],
        "receipts": [],
        "revision": "",
    }


def empty_verification() -> dict[str, Any]:
    return {
        "state": "待核验",
        "verified_shards": 0,
        "total_shards": 0,
        "valid_shards": 0,
        "invalid_shards": 0,
        "processed_shards": 0,
        "malformed_lines": [],
        "package_integrity": "未核验",
        "complete_package": False,
        "batches": [],
        "readable_batches": [],
        "missing_ranges": {},
        "verified_at": "",
    }


def public_package(record: dict[str, Any]) -> dict[str, Any]:
    """对外只暴露可 JSON 序列化的字段，去掉原始包文本方便列表接口。"""
    verification = record.get("verification") or empty_verification()
    manifest = record.get("manifest") or {}
    batch_nos = [item.get("batch_no") for item in (manifest.get("batches") or [])]
    return {
        "package_id": record.get("package_id"),
        "archive_name": record.get("archive_name"),
        "created_at": record.get("created_at"),
        "format_version": record.get("format_version"),
        "origin": record.get("origin"),
        "status": record.get("status"),
        "shard_count": record.get("shard_count", 0),
        "record_count": manifest.get("record_count", record.get("shard_count", 0)),
        "package_checksum": record.get("package_checksum", ""),
        "batch_nos": batch_nos,
        "verification": verification,
        "readable_batches": list(verification.get("readable_batches") or []),
        "missing_ranges": dict(verification.get("missing_ranges") or {}),
        "complete_package": bool(verification.get("complete_package")),
        "raw_record_ids": dict(record.get("raw_record_ids") or {}),
        "replay_task_ids": list(record.get("replay_task_ids") or []),
        "receipts": list(record.get("receipts") or [])[-5:],
        "revision": record.get("revision", ""),
    }


archive_registry = ArchivePackageRegistry()
