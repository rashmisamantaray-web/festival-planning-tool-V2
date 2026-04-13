"""Shared utilities for the Streamlit frontend."""

from __future__ import annotations

import streamlit as st

import api_client


def key_label(key: str) -> str:
    """Human-readable label for a date key like 'current', 'ref1', etc."""
    labels = st.session_state.get("date_labels", {})
    if labels.get(key):
        return labels[key]
    if key == "current":
        return "Current"
    return key.replace("ref", "Ref ")


def pct(v) -> str:
    """Format a number as a percentage string, or empty if None."""
    if v is None:
        return ""
    return f"{v:.2f}%"


def merge_response(resp: dict):
    """Merge a partial API response into session_state.data."""
    data = st.session_state.data
    if data is None:
        return
    for level_key in ("city", "subcat", "subcat_cut", "hub", "hub_cut"):
        if level_key in resp:
            data[level_key] = resp[level_key]
    if "store_key" in resp:
        st.session_state.store_key = resp["store_key"]


def resolve_formula(col_id: str, multiplier: float, offset: float, columns: list[dict]) -> float:
    """Resolve a formula spec (col × multiplier + offset) against available columns."""
    col = next((c for c in columns if c["id"] == col_id), None)
    if col is None:
        return offset
    return multiplier * col["value"] + offset


def toggle_minor():
    """Toggle minor cities visibility; re-compute with include_minor on first show."""
    if st.session_state.show_minor:
        st.session_state.show_minor = False
        return

    if st.session_state.get("minor_data_loaded"):
        st.session_state.show_minor = True
        return

    if not st.session_state.get("last_current"):
        return
    try:
        result = api_client.compute(
            st.session_state.last_current,
            st.session_state.last_refs,
            include_minor=True,
        )
        st.session_state.data = result
        st.session_state.store_key = result.get("store_key", "")
        st.session_state.show_minor = True
        st.session_state.minor_data_loaded = True
    except Exception as e:
        st.session_state.error = f"Failed to load minor cities: {e}"


def build_formula_columns(years: dict, hist_keys: list[str]) -> list[dict]:
    """Build the list of selectable formula columns from a record's year data."""
    cols = []
    for k in hist_keys:
        yd = years.get(k, {})
        label = key_label(k)
        cols.append({
            "id": f"P_{k}",
            "label": f"Pristine {label}",
            "value": yd.get("pristine_drop_pct", 0) or 0,
        })
        if yd.get("base_corrected_drop_pct") is not None:
            cols.append({
                "id": f"BC_{k}",
                "label": f"BC {label}",
                "value": yd.get("base_corrected_drop_pct", 0) or 0,
            })
    return cols
