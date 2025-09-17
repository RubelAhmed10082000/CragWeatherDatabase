from typing import Optional, List, Dict
import pandas as pd

def list_crags_core(
    *,
    q: Optional[str],
    county: Optional[List[str]],
    rocktype: Optional[List[str]],
    styles: Optional[List[str]],
    page: int,
    per_page: int,
    sort_by: str,
    sort_order: str,
    db,
) -> Dict:
    """Search for, and paginate, crags.

    Combines text search with filters, applies sorting, and returns a
    simple paginated page.

    Args:
        q: Optional free-text query against crag name.
        county: One or more county names to include.
        rocktype: One or more rock types to include.
        style: One or more styles from the `style` parameter (alias for `climbing_style`).
        climbing_style: One or more climbing styles to include.
        page: 1-based page index (validated to be ≥ 1).
        per_page: Page size (validated to 1–100).
        sort_by: Column name to sort by (e.g., `"name"`).
        sort_order: `"asc"` or `"desc"`.

    Returns:
        Dict with:
            - `items`: List of crag records for the requested slice.
            - `total`: Total number of rows after filters.
            - `page`: Current page number.
            - `per_page`: Page size.

    Notes:
        If no results match, returns an empty `items` list with `total=0`.
    """
    filters = {
        "county": county or None,
        "rocktype": rocktype or None,
        "climbing_style": styles or None,
    }
    df: pd.DataFrame = db.search_crags(query=q, filters=filters)

    if df is None or df.empty:
        return {"items": [], "total": 0, "page": page, "per_page": per_page}

    if sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=(sort_order.lower() != "desc"))

    total = int(df.shape[0])
    start, end = (page - 1) * per_page, (page - 1) * per_page + per_page
    items = df.iloc[start:end].to_dict(orient="records")
    return {"items": items, "total": total, "page": page, "per_page": per_page}

def get_crag_facets_core(db) -> Dict:
    facets = getattr(db, "list_crag_facets", None) or getattr(db, "crag_facets", None) or getattr(db, "get_facets", None)
    if callable(facets):
        return facets()
    return {"countries": [], "rock_types": [], "counties": [], "climbing_styles": []}
