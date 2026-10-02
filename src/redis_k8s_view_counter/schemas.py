"""Pydantic request/response models shared across the API."""

from datetime import datetime

from pydantic import BaseModel


class EventView(BaseModel):
    """Request body for POST /views."""

    resource_id: str


class ViewBucket(BaseModel):
    """One per-minute (or per-hour, depending on config) count."""

    bucket_start: datetime
    count: int


class ViewsResponse(BaseModel):
    """Response body for GET /views/{resource_id}."""

    resource_id: str
    since: datetime
    total: int
    buckets: list[ViewBucket]
