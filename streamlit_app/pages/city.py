"""Level 1 — City tab: trend chart, table with inline overrides."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import api_client
from constants import MAJOR_CITIES
from helpers import key_label, pct, merge_response, build_formula_columns, resolve_formula


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
    st.caption(
        "Y-axis: % change from baseline. Gray band: ±1 std (typical variance). "
        "Drops outside band are more meaningful."
    )
    selected_city = st.selectbox("City", cities, key="trend_city_select")

    fig = go.Figure()

    std_values = []
    for ref_date in ref_dates:
        city_data = trends.get(ref_date, {}).get(selected_city, [])
        for pt in city_data:
            if pt.get("std_pct") is not None:
                std_values.append(pt["std_pct"])

    avg_std = sum(std_values) / len(std_values) if std_values else 0

    offsets = list(range(-5, 6))
    x_labels = [f"D{'+' if o >= 0 else ''}{o}" for o in offsets]
    fig.add_trace(go.Scatter(
        x=x_labels, y=[avg_std] * len(x_labels),
        mode="lines", line=dict(color="rgba(148,163,184,0.3)", dash="dot", width=1),
        showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=x_labels, y=[-avg_std] * len(x_labels),
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


# ── City table builder ───────────────────────────────────────────────

def _build_city_df(
    records: list[dict], all_keys: list[str], hist_keys: list[str],
) -> pd.DataFrame:
    rows = []
    for rec in records:
        row: dict = {"City": rec["city_name"]}
        for k in all_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = key_label(k)
            row[f"Wk {lbl}"] = yd.get("week", "")
            row[f"Day {lbl}"] = yd.get("day_name", "")
            row[f"Base Rev (L) {lbl}"] = round(yd.get("baseline", 0) or 0, 2)
            row[f"Actual Rev (L) {lbl}"] = round(yd.get("actual", 0) or 0, 2)
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = key_label(k)
            row[f"Pristine {lbl}"] = pct(yd.get("pristine_drop_pct"))
        for k in hist_keys:
            yd = rec.get("years", {}).get(k, {})
            lbl = key_label(k)
            row[f"Base Corr {lbl}"] = pct(yd.get("base_corrected_drop_pct"))
        row["Override 1"] = pct(rec.get("override_row1", 0))
        row["Override 2"] = pct(rec.get("override_row2", 0))
        row["Final %"] = pct(rec.get("final_impact_pct", 0))
        rows.append(row)
    return pd.DataFrame(rows)


# ── Main renderer ────────────────────────────────────────────────────

def render_city(city_data: dict, trend_data: dict | None):
    hist_keys = city_data.get("historical_keys", [])
    all_keys = city_data.get("all_keys", [])
    records = city_data.get("data", [])
    cities = city_data.get("cities", [])

    _render_trend_chart(trend_data, cities)

    # ── Header row: title + action buttons ───────────────────────────
    hdr_l, hdr_r = st.columns([6, 4])
    with hdr_l:
        st.subheader("City")
    with hdr_r:
        btn_cols = st.columns(3)
        has_formulas = bool(st.session_state.city_overrides)
        with btn_cols[0]:
            if has_formulas:
                if st.button("Apply formula to sub-levels", key="apply_to_sublevels"):
                    _apply_formula_to_sublevels()
        with btn_cols[1]:
            label = "Hide Minor Cities" if st.session_state.show_minor else "Show Minor Cities"
            from app import _toggle_minor
            st.button(label, key="toggle_minor_city", on_click=_toggle_minor)

    major = [r for r in records if r["city_name"] in MAJOR_CITIES]
    minor = [r for r in records if r["city_name"] not in MAJOR_CITIES]

    st.markdown("**Major Cities**")
    if major:
        st.dataframe(_build_city_df(major, all_keys, hist_keys),
                      use_container_width=True, hide_index=True)

    if st.session_state.show_minor and minor:
        st.markdown("**Minor Cities**")
        st.dataframe(_build_city_df(minor, all_keys, hist_keys),
                      use_container_width=True, hide_index=True)

    # ── Override editing (per-city formula) ───────────────────────────
    st.subheader("City Overrides")
    st.caption("Select a city, set formula (column × multiplier + offset), then Apply.")

    city_names = [r["city_name"] for r in records]
    if not city_names:
        return
    selected_city = st.selectbox("City", city_names, key="city_override_select")
    rec = next((r for r in records if r["city_name"] == selected_city), None)
    if rec is None:
        return

    formula_cols = build_formula_columns(rec.get("years", {}), hist_keys)
    if not formula_cols:
        st.info("No historical data available for formula columns.")
        return

    col_options = {c["id"]: f"{c['label']} ({c['value']:.2f}%)" for c in formula_cols}

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Override Row 1**")
        sel1 = st.selectbox("Column", list(col_options.keys()),
                            format_func=lambda x: col_options[x], key="ov1_col")
        m1 = st.number_input("Multiplier", value=1.0, step=0.1, key="ov1_mult")
        o1 = st.number_input("Offset", value=0.0, step=0.1, key="ov1_off")
        v1 = resolve_formula(sel1, m1, o1, formula_cols)
        st.metric("Preview", f"{v1:.2f}%")

    with col2:
        st.markdown("**Override Row 2**")
        sel2 = st.selectbox("Column", list(col_options.keys()),
                            format_func=lambda x: col_options[x], key="ov2_col")
        m2 = st.number_input("Multiplier", value=1.0, step=0.1, key="ov2_mult")
        o2 = st.number_input("Offset", value=0.0, step=0.1, key="ov2_off")
        v2 = resolve_formula(sel2, m2, o2, formula_cols)
        st.metric("Preview", f"{v2:.2f}%")

    if st.button("Apply Override", key="apply_city_override", type="primary"):
        overrides_state = dict(st.session_state.city_overrides)
        overrides_state[selected_city] = {
            "row1": v1, "row2": v2,
            "spec1": {"col": sel1, "multiplier": m1, "offset": o1},
            "spec2": {"col": sel2, "multiplier": m2, "offset": o2},
        }
        st.session_state.city_overrides = overrides_state

        row1_dict, row2_dict = {}, {}
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
                merge_response(resp)
                st.rerun()
            except Exception as e:
                st.error(f"Override failed: {e}")


def _apply_formula_to_sublevels():
    """Propagate each city's row1 formula to L2, L3, L4 rows."""
    data = st.session_state.data
    store_key = st.session_state.store_key
    overrides = st.session_state.city_overrides
    if not data or not store_key or not overrides:
        return

    hist_keys = data.get("city", {}).get("historical_keys", [])
    l2_finals: dict[str, float] = {}
    l3_finals: dict[str, float] = {}
    l4_finals: dict[str, float] = {}

    for city, vals in overrides.items():
        spec = vals.get("spec1")
        if spec is None:
            row1_val = vals.get("row1")
            if row1_val is None:
                continue
            for rec in data.get("subcat", {}).get("data", []):
                if rec["city_name"] != city:
                    continue
                l2_finals[f"{city}||{rec['sub_category']}"] = row1_val
            for rec in data.get("subcat_cut", {}).get("data", []):
                if rec["city_name"] != city:
                    continue
                l3_finals[f"{city}||{rec['sub_category']}||{rec['cut_class']}"] = row1_val
            for rec in data.get("hub", {}).get("data", []):
                if rec["city_name"] != city:
                    continue
                l4_finals[f"{city}||{rec['hub_name']}"] = row1_val
            continue

        for rec in data.get("subcat", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            cols = build_formula_columns(rec.get("years", {}), hist_keys)
            resolved = resolve_formula(spec["col"], spec["multiplier"], spec["offset"], cols)
            l2_finals[f"{city}||{rec['sub_category']}"] = resolved

        for rec in data.get("subcat_cut", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            cols = build_formula_columns(rec.get("years", {}), hist_keys)
            resolved = resolve_formula(spec["col"], spec["multiplier"], spec["offset"], cols)
            l3_finals[f"{city}||{rec['sub_category']}||{rec['cut_class']}"] = resolved

        for rec in data.get("hub", {}).get("data", []):
            if rec["city_name"] != city:
                continue
            cols = build_formula_columns(rec.get("years", {}), hist_keys)
            resolved = resolve_formula(spec["col"], spec["multiplier"], spec["offset"], cols)
            l4_finals[f"{city}||{rec['hub_name']}"] = resolved

    with st.spinner("Applying to sub-levels..."):
        try:
            if l2_finals:
                resp = api_client.update_l2_finals(store_key, l2_finals)
                merge_response(resp)
            if l3_finals:
                resp = api_client.update_l3_finals(store_key, l3_finals)
                merge_response(resp)
            if l4_finals:
                resp = api_client.update_l4_finals(store_key, l4_finals)
                merge_response(resp)
            st.rerun()
        except Exception as e:
            st.error(f"Apply to sub-levels failed: {e}")
