"""Level 5 — City-Hub-SubCat-CutClass (read-only derived level).

Matches the React HubCutLevel component: major/minor/unmapped sections,
toggle buttons in header.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from constants import MAJOR_CITIES
from helpers import pct


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
            "Hub Drop %": pct(rec.get("hub_drop_pct")),
            "Drop w/ Current": pct(rec.get("drop_with_current_pct")),
            "Target SubCat-Cut %": pct(rec.get("target_subcat_cut_drop_pct")),
            "After Indexing %": pct(rec.get("final_after_indexing_pct")),
            "Final Rev": round(rec.get("final_rev", 0) or 0, 2),
        })
    return pd.DataFrame(rows)


def render_hub_cut(level_data: dict):
    records = level_data.get("data", [])

    mapped = [r for r in records if not _is_unmapped(r)]
    unmapped = [r for r in records if _is_unmapped(r)]
    major = [r for r in mapped if r.get("city_name") in MAJOR_CITIES]
    minor = [r for r in mapped if r.get("city_name") not in MAJOR_CITIES]

    # ── Header: title + action buttons ───────────────────────────────
    hdr_l, hdr_r = st.columns([6, 4])
    with hdr_l:
        st.subheader("Level 5 — City-Hub-SubCat-CutClass (Derived)")
    with hdr_r:
        btn_cols = st.columns(3)
        with btn_cols[0]:
            if unmapped:
                show_u = st.session_state.show_unmapped
                u_label = "Hide Unmapped Hubs" if show_u else f"Show Unmapped Hubs ({len(unmapped)})"
                if st.button(u_label, key="unmapped_btn_l5"):
                    st.session_state.show_unmapped = not show_u
                    st.rerun()
        with btn_cols[2]:
            label = "Hide Minor Cities" if st.session_state.show_minor else "Show Minor Cities"
            from app import _toggle_minor
            st.button(label, key="toggle_minor_l5", on_click=_toggle_minor)

    # ── Tables ───────────────────────────────────────────────────────
    st.markdown("**Major Cities**")
    if major:
        st.dataframe(_build_l5_df(major), use_container_width=True, hide_index=True)

    if st.session_state.show_minor and minor:
        st.markdown("**Minor Cities**")
        st.dataframe(_build_l5_df(minor), use_container_width=True, hide_index=True)

    if st.session_state.show_unmapped and unmapped:
        st.markdown("**Unmapped Hubs**")
        st.dataframe(_build_l5_df(unmapped), use_container_width=True, hide_index=True)
