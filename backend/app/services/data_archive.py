"""归档策略领域服务：除状态流转外，执行归档会真正生成可重放归档包。"""
from __future__ import annotations

from typing import Any

from app.services import archive_service
from app.services.base import BaseModuleService
from app.store import store


class DataArchiveService(BaseModuleService):
    MODULE = "data_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["策略名称", "归档周期", "保留时长", "归档格式", "目标存储"]
    REQUIRED_FIELDS = ["策略名称", "归档周期", "保留时长"]
    STATUS_ORDER = ["待执行", "已执行", "已暂停"]
    ACTION_RULES = {"执行归档": "已执行", "暂停归档": "已暂停"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "执行归档":
            return self._execute_archive(entry)
        return super().run_action(entry_id, action)

    def _execute_archive(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        batch_nos = self._batch_scope(entry)
        if not batch_nos:
            entry["status"] = "已执行"
            entry["pending"] = False
            return entry, "没有待归档的采集批次，归档策略已执行"

        result = archive_service.build_package(
            batch_nos,
            archive_name=f"{entry.get('策略名称', 'archive')}-{entry.get('id')}",
            strategy_id=int(entry["id"]),
        )
        if not result.get("ok"):
            return entry, str(result.get("message", "归档失败"))

        package = result["package"]
        entry["status"] = "已执行"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["已归档批次"] = ",".join(package.get("batch_nos") or [])
        entry["最后归档包"] = package.get("package_id")
        entry["归档格式"] = entry.get("归档格式") or "ndjson"
        entry["归档时间"] = package.get("created_at")
        return entry, (
            f"归档策略已执行，生成归档包 {package.get('package_id')}，"
            f"覆盖 {len(package.get('batch_nos') or [])} 个批次"
        )

    def _batch_scope(self, entry: dict[str, Any]) -> list[str]:
        raw = str(entry.get("归档范围") or "").strip()
        if raw:
            return [item.strip() for item in raw.replace("，", ",").split(",") if item.strip()]
        return archive_service.archive_strategy_batches(entry)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["archived_packages"] = [
            item["package_id"] for item in archive_service.list_packages()["items"]
        ]
        return snapshot
