"""Streamlit main app with a single routing source."""
import streamlit as st

from claw_data_filter.web.components.page_shell import inject_global_styles, render_sidebar_header
from claw_data_filter.web.config import (
    ACTIVE_DB_PATH_INPUT_KEY,
    apply_active_db_path,
    ensure_db_path_state,
)
from claw_data_filter.web.state.models import PAGE_LABELS, MAIN_PAGES
from claw_data_filter.web.state.router import go_to_page, read_route

# Initialize page
st.set_page_config(
    page_title="Claw Data Filter",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_global_styles()

# Sidebar navigation - query_params is the only routing source
render_sidebar_header()
route = read_route(st.query_params)
active_db_path = ensure_db_path_state(st.session_state)

st.sidebar.markdown("**当前数据库**")
st.sidebar.caption(str(active_db_path))

with st.sidebar.form("db_switch_form"):
    new_db_path = st.text_input(
        "数据库路径",
        value=st.session_state.get(ACTIVE_DB_PATH_INPUT_KEY, str(active_db_path)),
        key=ACTIVE_DB_PATH_INPUT_KEY,
        help="输入 DuckDB 文件路径并点击加载，无需重启 Web。",
    )
    load_db = st.form_submit_button("加载数据库", type="primary", use_container_width=True)

if load_db:
    ok, error_message, selected_path = apply_active_db_path(st.session_state, new_db_path)
    if ok:
        go_to_page(st.query_params, route.active_main_page)
        st.success(f"已切换到数据库: {selected_path}")
        st.rerun()
    st.sidebar.error(error_message or "数据库切换失败")

page_key = st.sidebar.radio(
    "选择页面",
    options=list(MAIN_PAGES),
    format_func=lambda key: PAGE_LABELS[key],
    index=list(MAIN_PAGES).index(route.active_main_page),
    key="main_page_selector",
    label_visibility="collapsed",
)

# Update query_params when selection changes
if page_key != route.active_main_page:
    go_to_page(st.query_params, page_key)
    st.rerun()

# Route to pages
if route.is_detail:
    from claw_data_filter.web.views import sample_detail

    sample_detail.render(route)
elif route.page == "overview":
    from claw_data_filter.web.views import overview

    overview.render()
elif route.page == "filter":
    from claw_data_filter.web.views import filter

    filter.render(route)
elif route.page == "tables":
    from claw_data_filter.web.views import tables

    tables.render(route)