"""Table schema viewer page."""
from math import ceil

import pandas as pd
import streamlit as st

from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.components.page_shell import render_page_header
from claw_data_filter.web.components.sample_table import render_samples_table
from claw_data_filter.web.config import get_active_db_path
from claw_data_filter.web.services.sample_query_service import get_samples_preview_page, get_table_preview
from claw_data_filter.web.state.models import RouteState
from claw_data_filter.web.state.router import go_to_detail


def _page_key(table_name: str) -> str:
    return f"tables.page.{table_name}"


def _render_pagination_controls(table_name: str, current_page: int, total_pages: int) -> None:
    col_prev, col_page, col_next = st.columns([1, 2, 1])
    if current_page > 1 and col_prev.button("上一页", key=f"prev_{table_name}"):
        st.session_state[_page_key(table_name)] = current_page - 1
        st.rerun()
    col_page.markdown(f"第 {current_page} / {total_pages} 页")
    if current_page < total_pages and col_next.button("下一页", key=f"next_{table_name}"):
        st.session_state[_page_key(table_name)] = current_page + 1
        st.rerun()


def render(route: RouteState):
    render_page_header(
        "数据表预览",
        "直接浏览 DuckDB 中的样本表和明细表，快速核对 schema、分页数据以及样本详情跳转。",
        "Tables",
    )

    store = DuckDBStore(get_active_db_path(st.session_state), read_only=True)
    tables = store.get_table_list()
    if not tables:
        st.info("数据库中没有可预览的数据表")
        store.close()
        return

    selected_table = st.selectbox("选择表", tables)
    page_size = 20 if selected_table == "samples" else 50
    page_state_key = _page_key(selected_table)
    if page_state_key not in st.session_state:
        st.session_state[page_state_key] = 1
    current_page = max(1, int(st.session_state.get(page_state_key, 1)))

    schema = store.get_table_schema(selected_table)
    st.markdown("**表结构**")
    for col in schema:
        st.markdown(f"- `{col['name']}` : {col['type']}")

    st.divider()

    if selected_table == "samples":
        preview_rows, total = get_samples_preview_page(store, current_page, page_size)
        total_pages = max(1, ceil(total / page_size))
        current_page = min(current_page, total_pages)
        st.session_state[page_state_key] = current_page
        if current_page > 1 and not preview_rows:
            current_page = total_pages
            st.session_state[page_state_key] = current_page
            preview_rows, total = get_samples_preview_page(store, current_page, page_size)
        st.markdown(f"**数据预览 ({total} 条中的第 {(current_page - 1) * page_size + 1} - {min(current_page * page_size, total)} 条)**")

        def on_detail(sample_id: int) -> None:
            go_to_detail(st.query_params, sample_id, route.active_main_page)
            st.rerun()

        render_samples_table(preview_rows, page=1, total_pages=total_pages, on_detail_click=on_detail, show_pagination=False)
        _render_pagination_controls(selected_table, current_page, total_pages)
    else:
        columns, rows, total = get_table_preview(store, selected_table, limit=page_size, offset=(current_page - 1) * page_size)
        total_pages = max(1, ceil(total / page_size))
        current_page = min(current_page, total_pages)
        st.session_state[page_state_key] = current_page
        if current_page > 1 and not rows:
            current_page = total_pages
            st.session_state[page_state_key] = current_page
            columns, rows, total = get_table_preview(store, selected_table, limit=page_size, offset=(current_page - 1) * page_size)
        if not rows:
            st.info("表中无数据")
            store.close()
            return

        st.markdown(f"**数据预览 ({total} 条中的第 {(current_page - 1) * page_size + 1} - {min(current_page * page_size, total)} 条)**")
        st.dataframe(pd.DataFrame(rows, columns=columns), width="stretch")
        _render_pagination_controls(selected_table, current_page, total_pages)

    store.close()
