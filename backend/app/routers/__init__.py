"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import archive_router, resource, pipeline_router

# 固定前缀（/api/archive、/api/pipeline）必须先于通用资源路由注册，
# 否则会被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [archive_router, pipeline_router, resource]
