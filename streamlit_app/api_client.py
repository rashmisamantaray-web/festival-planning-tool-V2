"""HTTP client wrapping all /festivals/* backend endpoints."""

from __future__ import annotations

import uuid
from typing import Any

import requests

from constants import API_BASE

TIMEOUT = 600  # 10 min — compute can be slow on first run


def _rid() -> str:
    return f"st_{uuid.uuid4().hex[:12]}"


def compute(
    current_date: str,
    reference_dates: list[str],
    include_minor: bool = False,
) -> dict[str, Any]:
    resp = requests.post(
        f"{API_BASE}/compute",
        json={
            "current_date": current_date,
            "reference_dates": reference_dates,
            "include_minor": include_minor,
        },
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_trends(reference_dates: list[str]) -> dict[str, Any]:
    resp = requests.post(
        f"{API_BASE}/trends",
        json={"reference_dates": reference_dates},
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def update_city_overrides(
    store_key: str, overrides: dict
) -> dict[str, Any]:
    resp = requests.put(
        f"{API_BASE}/city/overrides",
        json={"store_key": store_key, "overrides": overrides},
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def update_l2_finals(
    store_key: str, finals: dict[str, float]
) -> dict[str, Any]:
    resp = requests.put(
        f"{API_BASE}/city-subcat/finals",
        json={"store_key": store_key, "finals": finals},
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def update_l3_finals(
    store_key: str, finals: dict[str, float]
) -> dict[str, Any]:
    resp = requests.put(
        f"{API_BASE}/city-subcat-cut/finals",
        json={"store_key": store_key, "finals": finals},
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def update_l4_finals(
    store_key: str, finals: dict[str, float]
) -> dict[str, Any]:
    resp = requests.put(
        f"{API_BASE}/city-hub/finals",
        json={"store_key": store_key, "finals": finals},
        headers={"X-Request-ID": _rid()},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_export_bytes(store_key: str) -> bytes:
    resp = requests.get(
        f"{API_BASE}/export",
        params={"store_key": store_key},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.content
