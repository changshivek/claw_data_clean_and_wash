"""Route models for the Streamlit web UI."""
from dataclasses import dataclass


MAIN_PAGES = ("overview", "filter", "tables")
DETAIL_PAGE = "detail"
DEFAULT_PAGE = "overview"

PAGE_LABELS = {
    "overview": "📊 统计概览",
    "filter": "🔍 数据筛选",
    "tables": "📋 数据表预览",
    "detail": "📄 Sample 详情",
}


@dataclass(slots=True)
class RouteState:
    """Canonical route state derived from query params."""

    page: str = DEFAULT_PAGE
    sample_id: int | None = None
    return_to: str | None = None

    @property
    def active_main_page(self) -> str:
        if self.page in MAIN_PAGES:
            return self.page
        if self.return_to in MAIN_PAGES:
            return self.return_to
        return "filter"

    @property
    def is_detail(self) -> bool:
        return self.page == DETAIL_PAGE

    @property
    def back_target(self) -> str:
        if self.return_to in MAIN_PAGES:
            return self.return_to
        return "filter"
