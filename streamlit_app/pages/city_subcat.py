"""Reusable indexed level renderer for L2, L3, L4 tabs."""

from __future__ import annotations

import pandas as pd
import streamlit as st

import api_client
from constants import MAJOR_CITIES


def _key_label(key: str) -> str:
    labels = st.session_state.get("date_labels", {})
    if labels.get(key):
        return labels[key]
    if key == "current":
        return "Current"
    return key.replace("ref", "Ref ")


def _pct(v) -> str:
    if v is None:
        return ""
    return f"{v:.2f}%"


def _is_unmapped(rec: dict) -> bool:
    hub = rec.get("hub_name", "")
    return isinstance(hub, str) and hub.startswith("[Unmapped]")


def _build_indexed_df(
    records: list[dict],
    group_fields: list[tuple[str, str]],
    hist_keys: list[str],
    cur_key: str,
) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: dict = {}
        for field_key, field_label in group_fields:
            row[field_label] = rec.get(field_key, "")
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = _key_label(k)
            row[f"Base Rev (L) {lbl}"] = round(yd.get("baseline", 0) or 0, 2)
            row[f"Fest Rev (L) {lbl}"] = round(yd.get("actual", 0) or 0, 2)
            row[f"Drop {lbl}"] = _pct(yd.get("pristine_drop_pct"))
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = _key_label(k)
            row[f"Base Corr {lbl}"] = _pct(yd.get("base_corrected_drop_pct"))
        row[f"Base Rev (L) {_key_label(cur_key)}"] = round(
            rec.get("years", {}).get(cur_key, {}).get("baseline", 0) or 0, 2
        )
        row["Final %"] = round(rec.get("final_pct", 0) or 0, 2)
        row["Drop w/ Current"] = _pct(rec.get("drop_with_current_pct"))
        parent_drop = rec.get("city_drop_pct") or rec.get("subcat_drop_pct") or 0
        row["Parent Drop %"] = _pct(parent_drop)
        row["After Indexing %"] = _pct(rec.get("final_after_indexing_pct"))
        rows.append(row)
    return pd.DataFrame(rows)


def _make_composite_key(rec: dict, group_fields: list[tuple[str, str]]) -> str:
    return "||".join(str(rec.get(fk, "")) for fk, _ in group_fields)


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

    st.subheader(title)

    # Split mapped vs unmapped (hub levels only)
    if has_hub_field:
        mapped_records = [r for r in records if not _is_unmapped(r)]
        unmapped_records = [r for r in records if _is_unmapped(r)]
    else:
        mapped_records = records
        unmapped_records = []

    # Major / minor split
    major = [r for r in mapped_records if r.get("city_name") in MAJOR_CITIES]
    minor = [r for r in mapped_records if r.get("city_name") not in MAJOR_CITIES]

    # Major cities table
    st.markdown("**Major Cities**")
    if major:
        df = _build_indexed_df(major, group_fields, hist_keys, cur_key)
        df.rename(columns={"Parent Drop %": parent_drop_label}, inplace=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Minor cities table
    if minor:
        st.markdown("**Minor Cities**")
        df = _build_indexed_df(minor, group_fields, hist_keys, cur_key)
        df.rename(columns={"Parent Drop %": parent_drop_label}, inplace=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Unmapped hubs
    if has_hub_field and unmapped_records:
        show_unmapped = st.checkbox(
            f"Show Unmapped Hubs ({len(unmapped_records)} rows)",
            value=st.session_state.get("show_unmapped", False),
            key=f"unmapped_{level_key}",
        )
        if show_unmapped:
            st.markdown("**Unmapped Hubs**")
            df = _build_indexed_df(unmapped_records, group_fields, hist_keys, cur_key)
            df.rename(columns={"Parent Drop %": parent_drop_label}, inplace=True)
            st.dataframe(df, use_container_width=True, hide_index=True)

    # ── Editable finals ──────────────────────────────────────────────
    st.subheader(f"Edit Finals — {title}")
    st.caption("Adjust the Final % for individual rows. Changes cascade to downstream levels.")

    update_fn = {
        "l2": api_client.update_l2_finals,
        "l3": api_client.update_l3_finals,
        "l4": api_client.update_l4_finals,
    }.get(level_key)

    if update_fn is None:
        return

    # Build editable dataframe with composite keys
    edit_rows = []
    for rec in mapped_records:
        label_parts = [str(rec.get(fk, "")) for fk, _ in group_fields]
        edit_rows.append({
            "_key": _make_composite_key(rec, group_fields),
            "Label": " | ".join(label_parts),
            "Final %": round(rec.get("final_pct", 0) or 0, 2),
        })

    if not edit_rows:
        return

    edit_df = pd.DataFrame(edit_rows)
    edited = st.data_editor(
        edit_df[["Label", "Final %"]],
        use_container_width=True,
        hide_index=True,
        disabled=["Label"],
        key=f"editor_{level_key}",
    )

    if st.button(f"Save {title} Finals", key=f"save_{level_key}", type="primary"):
        finals: dict[str, float] = {}
        for i, row in edited.iterrows():
            original_val = edit_rows[i]["Final %"]
            new_val = row["Final %"]
            if new_val != original_val:
                finals[edit_rows[i]["_key"]] = float(new_val)

        if not finals:
            st.info("No changes detected.")
        else:
            with st.spinner("Saving..."):
                try:
                    resp = update_fn(st.session_state.store_key, finals)
                    from app import _merge_response
                    _merge_response(resp)
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")
