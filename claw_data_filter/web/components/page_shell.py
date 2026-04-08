"""Shared layout helpers for the Streamlit app shell."""

import streamlit as st


def inject_global_styles() -> None:
    """Apply a small, consistent visual theme to the app shell."""
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] {
            display: none;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(14, 165, 233, 0.10), transparent 28%),
                radial-gradient(circle at top right, rgba(34, 197, 94, 0.08), transparent 22%),
                linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
            border-right: 1px solid rgba(148, 163, 184, 0.18);
        }

        [data-testid="stSidebar"] * {
            color: #e5eefb;
        }

        [data-testid="stSidebar"] [data-testid="stTextInputRootElement"] {
            background: rgba(255, 255, 255, 0.96);
            border: 1px solid rgba(125, 211, 252, 0.55);
            border-radius: 12px;
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.35);
        }

        [data-testid="stSidebar"] [data-testid="stTextInputRootElement"] input {
            color: #0f172a !important;
            font-weight: 500;
        }

        [data-testid="stSidebar"] [data-testid="stTextInputRootElement"] input::placeholder {
            color: #64748b;
        }

        [data-testid="stSidebar"] button[kind="primaryFormSubmit"],
        [data-testid="stSidebar"] button[kind="primary"] {
            background: linear-gradient(135deg, #38bdf8 0%, #2563eb 100%);
            color: #eff6ff !important;
            border: 1px solid rgba(191, 219, 254, 0.35);
            font-weight: 700;
            box-shadow: 0 10px 24px rgba(37, 99, 235, 0.22);
        }

        [data-testid="stSidebar"] button[kind="secondaryFormSubmit"],
        [data-testid="stSidebar"] button[kind="secondary"] {
            background: rgba(255, 255, 255, 0.96);
            color: #334155 !important;
            border: 1px solid rgba(148, 163, 184, 0.42);
            font-weight: 600;
            box-shadow: none;
        }

        [data-testid="stSidebar"] button:hover {
            filter: brightness(1.04);
            border-color: rgba(148, 163, 184, 0.55);
        }

        .cdf-sidebar-card {
            padding: 1rem 1rem 0.9rem 1rem;
            border-radius: 16px;
            background: linear-gradient(135deg, rgba(14, 165, 233, 0.18), rgba(59, 130, 246, 0.08));
            border: 1px solid rgba(125, 211, 252, 0.22);
            margin-bottom: 1rem;
        }

        .cdf-sidebar-card h1 {
            font-size: 1.15rem;
            margin: 0 0 0.35rem 0;
            color: #f8fafc;
        }

        .cdf-sidebar-card p {
            font-size: 0.92rem;
            line-height: 1.45;
            margin: 0;
            color: #cbd5e1;
        }

        .cdf-page-hero {
            padding: 1.25rem 1.35rem;
            border-radius: 20px;
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(148, 163, 184, 0.18);
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.06);
            margin-bottom: 1rem;
        }

        .cdf-page-hero .eyebrow {
            display: inline-block;
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #0369a1;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }

        .cdf-page-hero h1 {
            font-size: 1.9rem;
            line-height: 1.15;
            color: #0f172a;
            margin: 0 0 0.5rem 0;
        }

        .cdf-page-hero p {
            font-size: 1rem;
            line-height: 1.6;
            color: #475569;
            margin: 0;
            max-width: 72ch;
        }

        .cdf-panel-note {
            padding: 0.85rem 1rem;
            border-radius: 14px;
            background: rgba(255, 255, 255, 0.70);
            border: 1px solid rgba(148, 163, 184, 0.18);
            color: #334155;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_header() -> None:
    """Render the sidebar brand card."""
    st.sidebar.markdown(
        """
        <div class="cdf-sidebar-card">
          <h1>Claw Data Filter</h1>
          <p>单入口工作台。统一查看统计、筛选、导出和表预览，避免被 Streamlit 默认多页导航打散。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, description: str, eyebrow: str) -> None:
    """Render a consistent page header."""
    st.markdown(
        f"""
        <section class="cdf-page-hero">
          <div class="eyebrow">{eyebrow}</div>
          <h1>{title}</h1>
          <p>{description}</p>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_panel_note(text: str) -> None:
    """Render a compact note panel below the page header."""
    st.markdown(f'<div class="cdf-panel-note">{text}</div>', unsafe_allow_html=True)
