"""Pydantic request/response models shared across the API."""

from pydantic import BaseModel


class EventView(BaseModel):
    """Request body for POST /views."""

    resource_id: str
