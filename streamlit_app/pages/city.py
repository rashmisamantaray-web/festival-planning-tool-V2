"""Level 1 — City tab: table with overrides + trend chart."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
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


def _build_formula_columns(rec: dict, hist_keys: list[str]) -> list[dict]:
    cols = []
    for k in hist_keys:
        yd = rec.get("years", {}).get(k, {})
        label = _key_label(k)
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


def _resolve_formula(col_id: str, multiplier: float, offset: float, columns: list[dict]) -> float:
    col = next((c for c in columns if c["id"] == col_id), None)
    if col is None:
        return offset
    return multiplier * col["value"] + offset


# ── Trend chart ──────────────────────────────────────────────────────

def _render_trend_chart(trend_data: dict | None, cities: list[str]):
    if not trend_data or "trends" not in trend_data:
        return
    trends = trend_data["trends"]
    if not trends:
        return

    ref_dates = list(trends.keys())
    colors = ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed"]

    st.subheader("% Change from Baseline (D-5 to D+5)")
    selected_city = st.selectbox("City", cities, key="trend_city_select")

    fig = go.Figure()

    std_values = []
    for ref_date in ref_dates:
        city_data = trends.get(ref_date, {}).get(selected_city, [])
        for pt in city_data:
            if pt.get("std_pct") is not None:
                std_values.append(pt["std_pct"])

    avg_std = sum(std_values) / len(std_values) if std_values else 0

    # Std band
    offsets = list(range(-5, 6))
    labels = [f"D{'+' if o >= 0 else ''}{o}" for o in offsets]
    fig.add_trace(go.Scatter(
        x=labels, y=[avg_std] * len(labels),
        mode="lines", line=dict(color="rgba(148,163,184,0.3)", dash="dot", width=1),
        showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=labels, y=[-avg_std] * len(labels),
        mode="lines", line=dict(color="rgba(148,163,184,0.3)", dash="dot", width=1),
        fill="tonexty", fillcolor="rgba(226,232,240,0.3)",
        showlegend=False,
    ))

    for i, ref_date in enumerate(ref_dates):
        city_data = trends.get(ref_date, {}).get(selected_city, [])
        if not city_data:
            continue
        sorted_pts = sorted(city_data, key=lambda p: p["day_offset"])
        x = [f"D{'+' if p['day_offset'] >= 0 else ''}{p['day_offset']}" for p in sorted_pts]
        y = [p["pct_change"] for p in sorted_pts]
        fig.add_trace(go.Scatter(
            x=x, y=y,
            mode="lines+markers",
            name=ref_date,
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=5),
        ))

    fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8", line_width=1)
    fig.update_layout(
        yaxis_title="% Change",
        height=350,
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── City table ───────────────────────────────────────────────────────

def _build_city_df(records: list[dict], all_keys: list[str], hist_keys: list[str]) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: dict = {"City": rec["city_name"]}
        for k in all_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = _key_label(k)
            row[f"Wk {lbl}"] = yd.get("week", "")
            row[f"Day {lbl}"] = yd.get("day_name", "")
            row[f"Base Rev (L) {lbl}"] = round(yd.get("baseline", 0) or 0, 2)
            row[f"Actual Rev (L) {lbl}"] = round(yd.get("actual", 0) or 0, 2)
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = _key_label(k)
            row[f"Pristine {lbl}"] = _pct(yd.get("pristine_drop_pct"))
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = _key_label(k)
            row[f"Base Corr {lbl}"] = _pct(yd.get("base_corrected_drop_pct"))
        row["Override 1"] = _pct(rec.get("override_row1", 0))
        row["Override 2"] = _pct(rec.get("override_row2", 0))
        row["Final %"] = _pct(rec.get("final_impact_pct", 0))
        rows.append(row)
    return pd.DataFrame(rows)


def render_city(city_data: dict, trend_data: dict | None):
    hist_keys = city_data.get("historical_keys", [])
    all_keys = city_data.get("all_keys", [])
    records = city_data.get("data", [])
    cities = city_data.get("cities", [])

    # Trend chart
    _render_trend_chart(trend_data, cities)

    st.subheader("City Level")

    # Major / minor split
    major = [r for r in records if r["city_name"] in MAJOR_CITIES]
    minor = [r for r in records if r["city_name"] not in MAJOR_CITIES]

    st.markdown("**Major Cities**")
    if major:
        df_major = _build_city_df(major, all_keys, hist_keys)
        st.dataframe(df_major, use_container_width=True, hide_index=True)

    if minor:
        st.markdown("**Minor Cities**")
        df_minor = _build_city_df(minor, all_keys, hist_keys)
        st.dataframe(df_minor, use_container_width=True, hide_index=True)

    # ── Override editing ─────────────────────────────────────────────
    st.subheader("City Overrides")
    st.caption("Select a city to edit override formulas. Changes cascade to all sub-levels.")

    city_names = [r["city_name"] for r in records]
    selected_city = st.selectbox("City to override", city_names, key="city_override_select")
    rec = next((r for r in records if r["city_name"] == selected_city), None)
    if rec is None:
        return

    formula_cols = _build_formula_columns(rec, hist_keys)
    if not formula_cols:
        st.info("No historical data available for formula columns.")
        return

    col_options = {c["id"]: f"{c['label']} ({c['value']:.2f}%)" for c in formula_cols}

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Override Row 1**")
        sel_col1 = st.selectbox("Column", list(col_options.keys()),
                                format_func=lambda x: col_options[x],
                                key="ov1_col")
        mult1 = st.number_input("Multiplier", value=1.0, step=0.1, key="ov1_mult")
        off1 = st.number_input("Offset", value=0.0, step=0.1, key="ov1_off")
        val1 = _resolve_formula(sel_col1, mult1, off1, formula_cols)
        st.metric("Preview", f"{val1:.2f}%")

    with col2:
        st.markdown("**Override Row 2**")
        sel_col2 = st.selectbox("Column", list(col_options.keys()),
                                format_func=lambda x: col_options[x],
                                key="ov2_col")
        mult2 = st.number_input("Multiplier", value=1.0, step=0.1, key="ov2_mult")
        off2 = st.number_input("Offset", value=0.0, step=0.1, key="ov2_off")
        val2 = _resolve_formula(sel_col2, mult2, off2, formula_cols)
        st.metric("Preview", f"{val2:.2f}%")

    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("Apply Override", key="apply_city_override", type="primary"):
            overrides_state = st.session_state.city_overrides
            overrides_state[selected_city] = {"row1": val1, "row2": val2}
            st.session_state.city_overrides = overrides_state

            row1_dict = {}
            row2_dict = {}
            for city, vals in overrides_state.items():
                if vals.get("row1") is not None:
                    row1_dict[city] = {"direct": vals["row1"]}
                if vals.get("row2") is not None:
                    row2_dict[city] = {"direct": vals["row2"]}

            with st.spinner("Updating..."):
                try:
                    resp = api_client.update_city_overrides(
                        st.session_state.store_key,
                        {"row1": row1_dict, "row2": row2_dict},
                    )
                    from app import _merge_response
                    _merge_response(resp)
                    st.rerun()
                except Exception as e:
                    st.error(f"Override failed: {e}")

    with bc2:
        if st.button("Apply Formula to Sub-Levels", key="apply_to_sublevels"):
            if not st.session_state.city_overrides:
                st.warning("Set at least one city override first.")
            else:
                _apply_formula_to_sublevels(records, hist_keys)


def _apply_formula_to_sublevels(city_records: list[dict], hist_keys: list[str]):
    """Propagate the row1 override formula to L2, L3, L4."""
    data = st.session_state.data
    store_key = st.session_state.store_key
    overrides = st.session_state.city_overrides

    l2_finals: dict[str, float] = {}
    l3_finals: dict[str, float] = {}
    l4_finals: dict[str, float] = {}

    for city, vals in overrides.items():
        row1_val = vals.get("row1")
        if row1_val is None:
            continue

        for rec in data.get("subcat", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            key = f"{city}||{rec['sub_category']}"
            l2_finals[key] = row1_val

        for rec in data.get("subcat_cut", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            key = f"{city}||{rec['sub_category']}||{rec['cut_class']}"
            l3_finals[key] = row1_val

        for rec in data.get("hub", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            key = f"{city}||{rec['hub_name']}"
            l4_finals[key] = row1_val

    with st.spinner("Applying to sub-levels..."):
        try:
            from app import _merge_response
            if l2_finals:
                resp = api_client.update_l2_finals(store_key, l2_finals)
                _merge_response(resp)
            if l3_finals:
                resp = api_client.update_l3_finals(store_key, l3_finals)
                _merge_response(resp)
            if l4_finals:
                resp = api_client.update_l4_finals(store_key, l4_finals)
                _merge_response(resp)
            st.rerun()
        except Exception as e:
            st.error(f"Apply to sub-levels failed: {e}")
