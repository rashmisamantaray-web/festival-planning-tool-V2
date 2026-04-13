"""Festival Impact Planning — Streamlit frontend.

Run with:  streamlit run streamlit_app/app.py
Backend must be running on http://localhost:8000.
"""

from __future__ import annotations

from datetime import date

import streamlit as st

import api_client
from constants import MAJOR_CITIES

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Festival Impact Planning",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state defaults ───────────────────────────────────────────
for key, default in [
    ("data", None),
    ("trend_data", None),
    ("store_key", ""),
    ("date_labels", {}),
    ("include_minor", False),
    ("show_unmapped", False),
    ("last_current", ""),
    ("last_refs", []),
    ("city_overrides", {}),  # {city: {row1: val, row2: val}}
]:
    if key not in st.session_state:
        st.session_state[key] = default


def _key_label(key: str) -> str:
    if key == "current":
        return st.session_state.date_labels.get("current", "Current")
    return st.session_state.date_labels.get(key, key.replace("ref", "Ref "))


def _pct(v) -> str:
    if v is None:
        return ""
    return f"{v:.2f}%"


def _merge_response(resp: dict):
    """Merge a partial API response into session_state.data."""
    data = st.session_state.data
    if data is None:
        return
    for level_key in ("city", "subcat", "subcat_cut", "hub", "hub_cut"):
        if level_key in resp:
            data[level_key] = resp[level_key]
    if "store_key" in resp:
        st.session_state.store_key = resp["store_key"]


# ── Sidebar: date inputs + controls ─────────────────────────────────
with st.sidebar:
    st.header("Festival Dates")
    current_date = st.date_input("Current Date", value=None)
    ref1 = st.date_input("Reference Date 1", value=None)
    ref2 = st.date_input("Reference Date 2", value=None)
    ref3 = st.date_input("Reference Date 3", value=None)

    refs = [str(d) for d in [ref1, ref2, ref3] if d is not None]
    can_compute = current_date is not None and len(refs) > 0

    include_minor = st.checkbox(
        "Include Minor Cities",
        value=st.session_state.include_minor,
        key="cb_minor",
    )

    if st.button("Compute", disabled=not can_compute, type="primary", use_container_width=True):
        cur_str = str(current_date)
        st.session_state.last_current = cur_str
        st.session_state.last_refs = refs
        st.session_state.include_minor = include_minor
        st.session_state.city_overrides = {}
        st.session_state.show_unmapped = False

        labels = {"current": cur_str}
        for i, d in enumerate(refs):
            labels[f"ref{i+1}"] = d
        st.session_state.date_labels = labels

        with st.spinner("Computing festival impact... (first run may take several minutes)"):
            try:
                result = api_client.compute(cur_str, refs, include_minor)
                st.session_state.data = result
                st.session_state.store_key = result.get("store_key", "")
                try:
                    trend_result = api_client.fetch_trends(refs)
                    st.session_state.trend_data = trend_result
                except Exception:
                    st.session_state.trend_data = None
            except Exception as e:
                st.error(f"Compute failed: {e}")

    # Re-compute if minor cities toggle changed after initial compute
    if (
        st.session_state.data is not None
        and include_minor != st.session_state.include_minor
        and st.session_state.last_current
    ):
        st.session_state.include_minor = include_minor
        with st.spinner("Recomputing with minor cities..."):
            try:
                result = api_client.compute(
                    st.session_state.last_current,
                    st.session_state.last_refs,
                    include_minor,
                )
                st.session_state.data = result
                st.session_state.store_key = result.get("store_key", "")
            except Exception as e:
                st.error(f"Recompute failed: {e}")

    st.divider()
    if st.session_state.store_key:
        try:
            excel_bytes = api_client.get_export_bytes(st.session_state.store_key)
            st.download_button(
                "Download Excel",
                data=excel_bytes,
                file_name=f"Festival_Plan_{st.session_state.store_key[:8]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception:
            st.warning("Export not available yet — run Compute first.")


# ── Main area ────────────────────────────────────────────────────────
st.title("Festival Impact Planning")

if st.session_state.data is None:
    st.info("Enter dates in the sidebar and click **Compute** to begin.")
    st.stop()

data = st.session_state.data

# Import page renderers
from pages.city import render_city
from pages.city_subcat import render_indexed_level
from pages.city_hub_cut import render_hub_cut

tab_city, tab_subcat, tab_subcat_cut, tab_hub, tab_hub_cut = st.tabs([
    "City",
    "City-Subcategory",
    "City-SubCat-CutClass",
    "City-Hub",
    "City-Hub-CutClass",
])

with tab_city:
    render_city(data["city"], st.session_state.trend_data)

with tab_subcat:
    render_indexed_level(
        title="City-Subcategory",
        level_data=data["subcat"],
        group_fields=[("city_name", "City"), ("sub_category", "Sub Category")],
        parent_drop_label="City Drop %",
        level_key="l2",
    )

with tab_subcat_cut:
    render_indexed_level(
        title="City-SubCat-CutClass",
        level_data=data["subcat_cut"],
        group_fields=[
            ("city_name", "City"),
            ("sub_category", "Sub Category"),
            ("cut_class", "Cut Class"),
        ],
        parent_drop_label="SubCat Drop %",
        level_key="l3",
    )

with tab_hub:
    render_indexed_level(
        title="City-Hub",
        level_data=data["hub"],
        group_fields=[("city_name", "City"), ("hub_name", "Hub")],
        parent_drop_label="City Drop %",
        level_key="l4",
        has_hub_field=True,
    )

with tab_hub_cut:
    render_hub_cut(data["hub_cut"])
