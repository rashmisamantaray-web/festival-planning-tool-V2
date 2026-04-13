"""Festival Impact Planning — Streamlit frontend.

Run with:  streamlit run streamlit_app/app.py
Backend must be running on http://localhost:8000.
"""

from __future__ import annotations

import streamlit as st

import api_client

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Festival Impact Planning",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state defaults ───────────────────────────────────────────
_DEFAULTS: list[tuple[str, object]] = [
    ("data", None),
    ("trend_data", None),
    ("store_key", ""),
    ("date_labels", {}),
    ("show_minor", False),
    ("minor_data_loaded", False),
    ("show_unmapped", False),
    ("last_current", ""),
    ("last_refs", []),
    ("city_overrides", {}),
    ("loading", False),
    ("error", ""),
]
for _k, _v in _DEFAULTS:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Callbacks ────────────────────────────────────────────────────────

def _do_compute():
    """Run the full compute pipeline."""
    cur = st.session_state.get("inp_current")
    r1 = st.session_state.get("inp_ref1")
    r2 = st.session_state.get("inp_ref2")
    r3 = st.session_state.get("inp_ref3")
    refs = [str(d) for d in [r1, r2, r3] if d is not None]
    if cur is None or not refs:
        return

    cur_str = str(cur)
    st.session_state.last_current = cur_str
    st.session_state.last_refs = refs
    st.session_state.city_overrides = {}
    st.session_state.show_minor = False
    st.session_state.minor_data_loaded = False
    st.session_state.show_unmapped = False
    st.session_state.error = ""

    labels = {"current": cur_str}
    for i, d in enumerate(refs):
        labels[f"ref{i + 1}"] = d
    st.session_state.date_labels = labels

    try:
        result = api_client.compute(cur_str, refs, include_minor=False)
        st.session_state.data = result
        st.session_state.store_key = result.get("store_key", "")
        try:
            st.session_state.trend_data = api_client.fetch_trends(refs)
        except Exception:
            st.session_state.trend_data = None
    except Exception as e:
        st.session_state.error = str(e)
        st.session_state.data = None


# ── Header ───────────────────────────────────────────────────────────
hdr_left, hdr_right = st.columns([8, 2])
with hdr_left:
    st.markdown("## Festival Impact Planning")
with hdr_right:
    if st.session_state.store_key:
        try:
            excel_bytes = api_client.get_export_bytes(st.session_state.store_key)
            st.download_button(
                "Export Excel",
                data=excel_bytes,
                file_name=f"Festival_Plan_{st.session_state.store_key[:8]}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception:
            pass

# ── Date inputs (inline, matching React DateInputPanel) ──────────────
with st.container():
    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1.5])
    with c1:
        st.date_input("Current Date", value=None, key="inp_current")
    with c2:
        st.date_input("Reference Date 1", value=None, key="inp_ref1")
    with c3:
        st.date_input("Reference Date 2", value=None, key="inp_ref2")
    with c4:
        st.date_input("Reference Date 3", value=None, key="inp_ref3")
    with c5:
        st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
        _can_compute = (
            st.session_state.get("inp_current") is not None
            and any(st.session_state.get(f"inp_ref{i}") is not None for i in (1, 2, 3))
        )
        st.button(
            "Compute",
            disabled=not _can_compute,
            type="primary",
            use_container_width=True,
            on_click=_do_compute,
        )

# ── Loading / error states ───────────────────────────────────────────
if st.session_state.error:
    st.error(st.session_state.error)

if st.session_state.data is None:
    if not st.session_state.error:
        st.info("Enter dates above and click **Compute** to begin.")
    st.stop()

# ── Tabs ─────────────────────────────────────────────────────────────
data = st.session_state.data

from pages.city import render_city
from pages.city_subcat import render_indexed_level
from pages.city_hub_cut import render_hub_cut

tab_city, tab_subcat, tab_subcat_cut, tab_hub, tab_hub_cut = st.tabs([
    "City",
    "City-Subcategory",
    "City-Subcategory-CutClass",
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
        title="City-Subcategory-CutClass",
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
