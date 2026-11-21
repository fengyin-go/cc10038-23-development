"""归档交付路由：构建可重放归档包、接收核验、可用文件清单与归档重放。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from app.services import archive_service

router = APIRouter(prefix="/api/archive", tags=["归档交付"])


class BuildPackagePayload(BaseModel):
    batch_nos: list[str] = Field(default_factory=list)
    archive_name: str | None = None
    strategy_id: int | None = None


class ReceivePayload(BaseModel):
    archive_text: str = Field(..., min_length=1)
    archive_name: str | None = None


class VerifyPayload(BaseModel):
    archive_text: str | None = None


class ReplayPackagePayload(BaseModel):
    new_parser_rule: str = Field(..., min_length=1)
    batch_nos: list[str] | None = None
    validate_entities: bool = False


@router.get("/packages")
def list_packages() -> dict[str, Any]:
    return archive_service.list_packages()


@router.post("/packages")
def build_package(payload: BuildPackagePayload) -> dict[str, Any]:
    return archive_service.build_package(
        payload.batch_nos, payload.archive_name, payload.strategy_id)


@router.get("/files")
def usable_files(readable_only: bool = Query(default=False)) -> dict[str, Any]:
    return archive_service.usable_file_list(readable_only=readable_only)


@router.get("/replays")
def list_replays() -> dict[str, Any]:
    return archive_service.list_replay_tasks()


@router.get("/replays/{task_id}")
def replay_detail(task_id: int) -> dict[str, Any]:
    detail = archive_service.replay_task_detail(task_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"重放任务 {task_id} 不存在")
    return detail


@router.get("/replays/{task_id}/sources")
def replay_sources(task_id: int) -> dict[str, Any]:
    detail = archive_service.replay_task_sources(task_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"重放任务 {task_id} 不存在")
    return detail


@router.get("/packages/{package_id}")
def package_detail(package_id: str) -> dict[str, Any]:
    detail = archive_service.package_detail(package_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"归档包 {package_id} 不存在")
    return detail


@router.get("/packages/{package_id}/download", response_class=PlainTextResponse)
def download_package(package_id: str) -> PlainTextResponse:
    found = archive_service.package_text(package_id)
    if found is None:
        raise HTTPException(status_code=404, detail=f"归档包 {package_id} 不存在")
    text, name = found
    if not text:
        raise HTTPException(status_code=409, detail=f"归档包 {package_id} 还没有可下载内容")
    return PlainTextResponse(
        text,
        media_type="application/x-ndjson; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{name}.ndjson"'},
    )


@router.post("/packages/{package_id}/receive")
def receive_package(package_id: str, payload: ReceivePayload) -> dict[str, Any]:
    result = archive_service.receive_package(package_id, payload.archive_text)
    if payload.archive_name:
        record = archive_service.archive_registry.get(package_id)
        if record is not None:
            record["archive_name"] = payload.archive_name
    return result


@router.post("/packages/{package_id}/verify")
def verify_package(package_id: str, payload: VerifyPayload | None = None) -> dict[str, Any]:
    text = payload.archive_text if payload else None
    return archive_service.verify_package(package_id, text)


@router.post("/packages/{package_id}/replay")
def replay_package(package_id: str, payload: ReplayPackagePayload) -> dict[str, Any]:
    return archive_service.replay_package(
        package_id, payload.new_parser_rule, payload.batch_nos, payload.validate_entities)
