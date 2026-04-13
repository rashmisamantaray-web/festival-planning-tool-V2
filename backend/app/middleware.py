"""
Request correlation middleware.

Layer: API / Middleware
Extracts X-Request-ID from incoming requests and stores it in request.state
for propagation through the pipeline. Generates one if not provided.
"""

from __future__ import annotations

import logging
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
REQUEST_ID_STATE_KEY = "request_id"


def get_request_id(request: Request) -> str:
    """Get the correlation ID for the current request."""
    return getattr(request.state, REQUEST_ID_STATE_KEY, "")


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Adds request_id to request.state and response headers."""

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get(REQUEST_ID_HEADER) or f"req_{uuid.uuid4().hex[:16]}"
        request.state.request_id = rid
        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = rid
        return response
