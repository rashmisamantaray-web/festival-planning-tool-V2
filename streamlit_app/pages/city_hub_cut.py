"""Level 5 — City-Hub-SubCat-CutClass (read-only derived level)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from constants import MAJOR_CITIES


def _pct(v) -> str:
    if v is None:
        return ""
    return f"{v:.2f}%"


def _is_unmapped(rec: dict) -> bool:
    hub = rec.get("hub_name", "")
    return isinstance(hub, str) and hub.startswith("[Unmapped]")


def _build_l5_df(records: list[dict]) -> pd.DataFrame:
    rows = []
    for rec in records:
        rows.append({
            "City": rec.get("city_name", ""),
            "Hub": rec.get("hub_name", ""),
            "Sub Category": rec.get("sub_category", ""),
            "Cut Class": rec.get("cut_class", ""),
            "Baseline": round(rec.get("baseline", 0) or 0, 2),
            "Hub Drop %": _pct(rec.get("hub_drop_pct")),
            "Drop w/ Current": _pct(rec.get("drop_with_current_pct")),
            "Target SubCat-Cut %": _pct(rec.get("target_subcat_cut_drop_pct")),
            "After Indexing %": _pct(rec.get("final_after_indexing_pct")),
            "Final Rev": round(rec.get("final_rev", 0) or 0, 2),
        })
    return pd.DataFrame(rows)


def render_hub_cut(level_data: dict):
    records = level_data.get("data", [])

    st.subheader("Level 5 — City-Hub-SubCat-CutClass (Derived)")

    mapped = [r for r in records if not _is_unmapped(r)]
    unmapped = [r for r in records if _is_unmapped(r)]

    major = [r for r in mapped if r.get("city_name") in MAJOR_CITIES]
    minor = [r for r in mapped if r.get("city_name") not in MAJOR_CITIES]

    st.markdown("**Major Cities**")
    if major:
        st.dataframe(_build_l5_df(major), use_container_width=True, hide_index=True)

    if minor:
        st.markdown("**Minor Cities**")
        st.dataframe(_build_l5_df(minor), use_container_width=True, hide_index=True)

    if unmapped:
        show = st.checkbox(
            f"Show Unmapped Hubs ({len(unmapped)} rows)",
            value=st.session_state.get("show_unmapped", False),
            key="unmapped_l5",
        )
        if show:
            st.markdown("**Unmapped Hubs**")
            st.dataframe(_build_l5_df(unmapped), use_container_width=True, hide_index=True)
