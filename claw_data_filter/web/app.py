"""Streamlit main app with a single routing source."""
import streamlit as st

from claw_data_filter.web.state.models import PAGE_LABELS, MAIN_PAGES
from claw_data_filter.web.state.router import go_to_page, read_route

# Initialize page
st.set_page_config(
    page_title="Claw Data Filter",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation - query_params is the only routing source
st.sidebar.title("Claw Data Filter")

route = read_route(st.query_params)

# Create radio with current selection
page_key = st.sidebar.radio(
    "导航",
    options=list(MAIN_PAGES),
    format_func=lambda key: PAGE_LABELS[key],
    index=list(MAIN_PAGES).index(route.active_main_page),
    key="main_page_selector",
)

# Update query_params when selection changes
if page_key != route.active_main_page:
    go_to_page(st.query_params, page_key)
    st.rerun()

# Route to pages
if route.is_detail:
    from claw_data_filter.web.pages import sample_detail

    sample_detail.render(route)
elif route.page == "overview":
    from claw_data_filter.web.pages import overview

    overview.render()
elif route.page == "filter":
    from claw_data_filter.web.pages import filter

    filter.render(route)
elif route.page == "export":
    from claw_data_filter.web.pages import export

    export.render()
elif route.page == "tables":
    from claw_data_filter.web.pages import tables

    tables.render(route)