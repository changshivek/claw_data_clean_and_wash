"""Helpers for reading and updating Streamlit query-param routes."""
from collections.abc import MutableMapping

from claw_data_filter.web.state.models import DEFAULT_PAGE, DETAIL_PAGE, MAIN_PAGES, RouteState


CONTROLLED_KEYS = ("page", "sample_uid", "return_to")


def _normalize_page(page: str | None) -> str:
    if page == DETAIL_PAGE:
        return DETAIL_PAGE
    if page in MAIN_PAGES:
        return page
    return DEFAULT_PAGE


def _parse_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def read_route(params: MutableMapping[str, object]) -> RouteState:
    """Build a route state from query params."""
    page = _normalize_page(str(params.get("page", DEFAULT_PAGE)))
    sample_uid = _parse_optional_text(params.get("sample_uid"))
    return_to = str(params.get("return_to")) if params.get("return_to") else None
    if return_to not in MAIN_PAGES:
        return_to = None
    if page != DETAIL_PAGE:
        sample_uid = None
    return RouteState(page=page, sample_uid=sample_uid, return_to=return_to)


def write_route(params: MutableMapping[str, object], route: RouteState) -> None:
    """Persist route state back to query params."""
    for key in CONTROLLED_KEYS:
        if key in params:
            params.pop(key)
    params["page"] = route.page
    if route.sample_uid is not None:
        params["sample_uid"] = route.sample_uid
    if route.return_to:
        params["return_to"] = route.return_to


def go_to_page(params: MutableMapping[str, object], page: str) -> RouteState:
    """Navigate to a top-level page and clear drill-down context."""
    route = RouteState(page=_normalize_page(page))
    write_route(params, route)
    return route


def go_to_detail(params: MutableMapping[str, object], sample_uid: str, return_to: str | None) -> RouteState:
    """Navigate to the detail page while preserving the source page."""
    target = return_to if return_to in MAIN_PAGES else "filter"
    route = RouteState(page=DETAIL_PAGE, sample_uid=sample_uid, return_to=target)
    write_route(params, route)
    return route


def go_back(params: MutableMapping[str, object], route: RouteState) -> RouteState:
    """Return from detail to its source page."""
    target = RouteState(page=route.back_target)
    write_route(params, target)
    return target
