"""Reusable indexed level renderer for L2, L3, L4 tabs.

Matches the React IndexedLevel component: per-row formula editing,
minor cities toggle button, unmapped hubs toggle button.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

import api_client
from constants import MAJOR_CITIES
from helpers import key_label, pct, merge_response, build_formula_columns, resolve_formula, toggle_minor


def _is_unmapped(rec: dict) -> bool:
    hub = rec.get("hub_name", "")
    return isinstance(hub, str) and hub.startswith("[Unmapped]")


def _build_indexed_df(
    records: list[dict],
    group_fields: list[tuple[str, str]],
    hist_keys: list[str],
    cur_key: str,
    parent_drop_label: str,
) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: dict = {}
        for field_key, field_label in group_fields:
            row[field_label] = rec.get(field_key, "")
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = key_label(k)
            row[f"Base Rev (L) {lbl}"] = round(yd.get("baseline", 0) or 0, 2)
            row[f"Fest Rev (L) {lbl}"] = round(yd.get("actual", 0) or 0, 2)
            row[f"Drop {lbl}"] = pct(yd.get("pristine_drop_pct"))
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = key_label(k)
            row[f"Base Corr {lbl}"] = pct(yd.get("base_corrected_drop_pct"))
        row[f"Base Rev (L) {key_label(cur_key)}"] = round(
            rec.get("years", {}).get(cur_key, {}).get("baseline", 0) or 0, 2
        )
        row["Final %"] = pct(rec.get("final_pct", 0))
        row["Drop w/ Current"] = pct(rec.get("drop_with_current_pct"))
        parent_drop = rec.get("city_drop_pct") or rec.get("subcat_drop_pct") or 0
        row[parent_drop_label] = pct(parent_drop)
        row["After Indexing %"] = pct(rec.get("final_after_indexing_pct"))
        rows.append(row)
    return pd.DataFrame(rows)


def _make_key(rec: dict, group_fields: list[tuple[str, str]]) -> str:
    return "||".join(str(rec.get(fk, "")) for fk, _ in group_fields)


def _make_label(rec: dict, group_fields: list[tuple[str, str]]) -> str:
    return " | ".join(str(rec.get(fk, "")) for fk, _ in group_fields)


def render_indexed_level(
    title: str,
    level_data: dict,
    group_fields: list[tuple[str, str]],
    parent_drop_label: str,
    level_key: str,
    has_hub_field: bool = False,
):
    hist_keys = level_data.get("historical_keys", [])
    cur_key = level_data.get("current_key", "current")
    records = level_data.get("data", [])

    # Split mapped vs unmapped (hub levels only)
    if has_hub_field:
        mapped_records = [r for r in records if not _is_unmapped(r)]
        unmapped_records = [r for r in records if _is_unmapped(r)]
    else:
        mapped_records = records
        unmapped_records = []

    major = [r for r in mapped_records if r.get("city_name") in MAJOR_CITIES]
    minor = [r for r in mapped_records if r.get("city_name") not in MAJOR_CITIES]

    # ── Header: title + action buttons ───────────────────────────────
    hdr_l, hdr_r = st.columns([6, 4])
    with hdr_l:
        st.subheader(title)
    with hdr_r:
        btn_cols = st.columns(3)
        with btn_cols[0]:
            if has_hub_field and unmapped_records:
                show_u = st.session_state.show_unmapped
                u_label = "Hide Unmapped Hubs" if show_u else f"Show Unmapped Hubs ({len(unmapped_records)})"
                if st.button(u_label, key=f"unmapped_btn_{level_key}"):
                    st.session_state.show_unmapped = not show_u
                    st.rerun()
        with btn_cols[2]:
            label = "Hide Minor Cities" if st.session_state.show_minor else "Show Minor Cities"
            st.button(label, key=f"toggle_minor_{level_key}", on_click=toggle_minor)

    # ── Tables ───────────────────────────────────────────────────────
    st.markdown("**Major Cities**")
    if major:
        df = _build_indexed_df(major, group_fields, hist_keys, cur_key, parent_drop_label)
        st.dataframe(df, use_container_width=True, hide_index=True)

    if st.session_state.show_minor and minor:
        st.markdown("**Minor Cities**")
        df = _build_indexed_df(minor, group_fields, hist_keys, cur_key, parent_drop_label)
        st.dataframe(df, use_container_width=True, hide_index=True)

    if st.session_state.show_unmapped and has_hub_field and unmapped_records:
        st.markdown("**Unmapped Hubs**")
        df = _build_indexed_df(unmapped_records, group_fields, hist_keys, cur_key, parent_drop_label)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Per-row formula editing (matching React FormulaCell) ─────────
    update_fn = {
        "l2": api_client.update_l2_finals,
        "l3": api_client.update_l3_finals,
        "l4": api_client.update_l4_finals,
    }.get(level_key)
    if update_fn is None or not mapped_records:
        return

    st.subheader(f"Edit Final % — {title}")
    st.caption("Select a row, set formula (column × multiplier + offset), then Apply.")

    row_options = {
        _make_key(r, group_fields): _make_label(r, group_fields)
        for r in mapped_records
    }
    if not row_options:
        return

    selected_key = st.selectbox(
        "Row",
        list(row_options.keys()),
        format_func=lambda k: row_options[k],
        key=f"sel_row_{level_key}",
    )
    selected_rec = next(
        (r for r in mapped_records if _make_key(r, group_fields) == selected_key),
        None,
    )
    if selected_rec is None:
        return

    formula_cols = build_formula_columns(selected_rec.get("years", {}), hist_keys)
    if not formula_cols:
        st.info("No historical data available for formula columns.")
        return

    col_opts = {c["id"]: f"{c['label']} ({c['value']:.2f}%)" for c in formula_cols}

    fc1, fc2, fc3 = st.columns([3, 1.5, 1.5])
    with fc1:
        sel_col = st.selectbox(
            "Column", list(col_opts.keys()),
            format_func=lambda x: col_opts[x],
            key=f"fc_col_{level_key}",
        )
    with fc2:
        mult = st.number_input("Multiplier", value=1.0, step=0.1, key=f"fc_mult_{level_key}")
    with fc3:
        off = st.number_input("Offset", value=0.0, step=0.1, key=f"fc_off_{level_key}")

    preview = resolve_formula(sel_col, mult, off, formula_cols)
    pc1, pc2 = st.columns([3, 1])
    with pc1:
        st.metric("Preview", f"{preview:.2f}%")
    with pc2:
        if st.button("Apply", key=f"apply_final_{level_key}", type="primary"):
            with st.spinner("Saving..."):
                try:
                    resp = update_fn(st.session_state.store_key, {selected_key: preview})
                    merge_response(resp)
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")
