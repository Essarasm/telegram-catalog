from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_db, transliterate_to_latin, normalize_uzbek

router = APIRouter(prefix="/api/search", tags=["search"])


@router.post("/log")
def log_search(
    telegram_id: int = 0,
    query: str = "",
    results_count: int = 0,
    category_id: Optional[int] = None,
    producer_id: Optional[int] = None,
):
    """Log a search query. Returns the search_log_id for tracking clicks."""
    if not query or not query.strip():
        return {"ok": False}
    conn = get_db()
    cursor = conn.execute(
        """INSERT INTO search_logs (telegram_id, query, results_count, category_id, producer_id)
           VALUES (?, ?, ?, ?, ?)""",
        (telegram_id, query.strip().lower(), results_count, category_id, producer_id),
    )
    search_log_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"ok": True, "search_log_id": search_log_id}


@router.post("/click")
def log_click(
    search_log_id: int = 0,
    telegram_id: int = 0,
    product_id: int = 0,
    action: str = "click",
):
    """Log a product click/add-to-cart from search results.
    action: 'click' = viewed product, 'cart' = added to cart from search.
    """
    if not product_id:
        return {"ok": False}
    conn = get_db()
    conn.execute(
        """INSERT INTO search_clicks (search_log_id, telegram_id, product_id, action)
           VALUES (?, ?, ?, ?)""",
        (search_log_id or None, telegram_id, product_id, action),
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ── Analytics endpoints ──────────────────────────────────────────


@router.get("/stats/top-queries")
def top_queries(
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(20, ge=1, le=100),
):
    """Most frequent search queries in the last N days."""
    conn = get_db()
    rows = conn.execute(
        """SELECT query, COUNT(*) as search_count,
                  ROUND(AVG(results_count), 1) as avg_results,
                  MIN(results_count) as min_results
           FROM search_logs
           WHERE created_at >= datetime('now', ?)
           GROUP BY query
           ORDER BY search_count DESC
           LIMIT ?""",
        (f"-{days} days", limit),
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows], "days": days}


@router.get("/stats/zero-results")
def zero_results(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
):
    """Searches that returned zero results — unmet demand signals.
    Ranked by frequency (most wanted products clients can't find)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT query, COUNT(*) as search_count,
                  COUNT(DISTINCT telegram_id) as unique_users,
                  MAX(created_at) as last_searched
           FROM search_logs
           WHERE results_count = 0
             AND created_at >= datetime('now', ?)
           GROUP BY query
           ORDER BY search_count DESC
           LIMIT ?""",
        (f"-{days} days", limit),
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows], "days": days}


@router.get("/stats/funnel")
def search_funnel(days: int = Query(7, ge=1, le=365)):
    """Search-to-cart conversion funnel for the last N days."""
    conn = get_db()

    total_searches = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    searches_with_results = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE results_count > 0 AND created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    searches_with_clicks = conn.execute(
        """SELECT COUNT(DISTINCT sl.id)
           FROM search_logs sl
           JOIN search_clicks sc ON sc.search_log_id = sl.id AND sc.action = 'click'
           WHERE sl.created_at >= datetime('now', ?)""",
        (f"-{days} days",),
    ).fetchone()[0]

    searches_with_cart = conn.execute(
        """SELECT COUNT(DISTINCT sl.id)
           FROM search_logs sl
           JOIN search_clicks sc ON sc.search_log_id = sl.id AND sc.action = 'cart'
           WHERE sl.created_at >= datetime('now', ?)""",
        (f"-{days} days",),
    ).fetchone()[0]

    conn.close()
    return {
        "days": days,
        "total_searches": total_searches,
        "with_results": searches_with_results,
        "with_clicks": searches_with_clicks,
        "with_cart_add": searches_with_cart,
        "conversion_rate": round(searches_with_cart / total_searches * 100, 1) if total_searches else 0,
    }


@router.get("/stats/recent")
def recent_searches(limit: int = Query(50, ge=1, le=200)):
    """Most recent search queries with user info."""
    conn = get_db()
    rows = conn.execute(
        """SELECT sl.id, sl.query, sl.results_count, sl.created_at,
                  sl.telegram_id, u.first_name, u.last_name,
                  ac.name as client_name, ac.company_name
           FROM search_logs sl
           LEFT JOIN users u ON u.telegram_id = sl.telegram_id
           LEFT JOIN allowed_clients ac ON ac.matched_telegram_id = sl.telegram_id
           ORDER BY sl.created_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows]}


@router.get("/stats/per-client")
def per_client_searches(
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(20, ge=1, le=100),
):
    """Search activity per client — who's searching the most and for what."""
    conn = get_db()
    rows = conn.execute(
        """SELECT sl.telegram_id,
                  u.first_name, u.last_name,
                  ac.name as client_name, ac.company_name,
                  COUNT(*) as search_count,
                  COUNT(DISTINCT sl.query) as unique_queries,
                  SUM(CASE WHEN sl.results_count = 0 THEN 1 ELSE 0 END) as zero_result_count
           FROM search_logs sl
           LEFT JOIN users u ON u.telegram_id = sl.telegram_id
           LEFT JOIN allowed_clients ac ON ac.matched_telegram_id = sl.telegram_id
           WHERE sl.created_at >= datetime('now', ?)
           GROUP BY sl.telegram_id
           ORDER BY search_count DESC
           LIMIT ?""",
        (f"-{days} days", limit),
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows], "days": days}


@router.get("/stats/summary")
def search_summary(days: int = Query(7, ge=1, le=365)):
    """Quick overview stats for bot command / dashboard header."""
    conn = get_db()

    total = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    unique_users = conn.execute(
        "SELECT COUNT(DISTINCT telegram_id) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    unique_queries = conn.execute(
        "SELECT COUNT(DISTINCT query) FROM search_logs WHERE created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    zero_result_count = conn.execute(
        "SELECT COUNT(*) FROM search_logs WHERE results_count = 0 AND created_at >= datetime('now', ?)",
        (f"-{days} days",),
    ).fetchone()[0]

    conn.close()
    return {
        "days": days,
        "total_searches": total,
        "unique_users": unique_users,
        "unique_queries": unique_queries,
        "zero_result_searches": zero_result_count,
        "zero_result_pct": round(zero_result_count / total * 100, 1) if total else 0,
    }


# ── Autocomplete / Suggestions ─────────────────────────────────────

@router.get("/suggestions")
def search_suggestions(
    q: str = Query("", min_length=1),
    limit: int = Query(8, ge=1, le=20),
):
    """Return search suggestions based on:
    1. Popular past queries matching the prefix
    2. Product names matching the prefix
    Designed for autocomplete dropdown as user types.
    """
    if not q or len(q.strip()) < 1:
        return {"suggestions": []}

    query = q.strip().lower()
    query_latin = transliterate_to_latin(query)
    query_norm = normalize_uzbek(query)
    conn = get_db()
    suggestions = []
    seen = set()

    # 1. Popular queries that start with or contain the prefix
    rows = conn.execute(
        """SELECT query, COUNT(*) as cnt
           FROM search_logs
           WHERE results_count > 0
             AND (query LIKE ? OR query LIKE ? OR query LIKE ?)
           GROUP BY query
           ORDER BY cnt DESC
           LIMIT ?""",
        (f"{query}%", f"{query_latin}%", f"{query_norm}%", limit),
    ).fetchall()
    for r in rows:
        q_text = r["query"]
        if q_text not in seen:
            suggestions.append({"text": q_text, "type": "query", "count": r["cnt"]})
            seen.add(q_text)

    # 2. Product names matching the prefix (fill remaining slots)
    remaining = limit - len(suggestions)
    if remaining > 0:
        rows = conn.execute(
            """SELECT DISTINCT COALESCE(p.name_display, p.name) as display_name
               FROM products p
               WHERE p.is_active = 1
                 AND (p.search_text LIKE ? OR p.search_text LIKE ? OR p.search_text LIKE ?)
               LIMIT ?""",
            (f"%{query}%", f"%{query_latin}%", f"%{query_norm}%", remaining * 2),
        ).fetchall()
        for r in rows:
            name = r["display_name"]
            name_lower = name.lower()
            if name_lower not in seen and len(suggestions) < limit:
                suggestions.append({"text": name, "type": "product"})
                seen.add(name_lower)

    conn.close()
    return {"suggestions": suggestions}


@router.get("/did-you-mean")
def did_you_mean(
    q: str = Query("", min_length=2),
    limit: int = Query(3, ge=1, le=10),
):
    """When a search returns zero results, suggest alternative queries.
    Uses edit distance against popular queries and product names.
    """
    if not q or len(q.strip()) < 2:
        return {"suggestions": []}

    query = q.strip().lower()
    query_latin = transliterate_to_latin(query)
    query_norm = normalize_uzbek(query)
    conn = get_db()

    candidates = []

    # 1. Check popular queries with results
    rows = conn.execute(
        """SELECT query, COUNT(*) as cnt
           FROM search_logs
           WHERE results_count > 0
           GROUP BY query
           HAVING cnt >= 2
           ORDER BY cnt DESC
           LIMIT 200""",
    ).fetchall()
    for r in rows:
        candidates.append((r["query"], r["cnt"], "query"))

    # 2. Get product display names (sample for performance)
    rows = conn.execute(
        """SELECT DISTINCT LOWER(COALESCE(name_display, name)) as nm
           FROM products WHERE is_active = 1"""
    ).fetchall()
    for r in rows:
        candidates.append((r["nm"], 1, "product"))

    # Score candidates by edit distance / similarity
    scored = []
    for text, popularity, source in candidates:
        if not text:
            continue
        # Check edit distance for each query variant
        best_dist = min(
            _edit_distance_bounded(query, text, 4),
            _edit_distance_bounded(query_latin, text, 4),
            _edit_distance_bounded(query_norm, text, 4),
        )
        # Also check individual words in multi-word product names
        for word in text.split():
            if len(word) >= 3:
                best_dist = min(
                    best_dist,
                    _edit_distance_bounded(query, word, 3),
                    _edit_distance_bounded(query_latin, word, 3),
                )
        if best_dist <= 3 and best_dist > 0:  # Close but not identical
            # Higher score = better suggestion (lower distance, higher popularity)
            score = (4 - best_dist) * 100 + min(popularity, 50)
            scored.append((score, text, source))

    scored.sort(key=lambda x: -x[0])

    # Deduplicate and limit
    seen = set()
    suggestions = []
    for _, text, source in scored:
        if text not in seen and len(suggestions) < limit:
            suggestions.append({"text": text, "type": source})
            seen.add(text)

    conn.close()
    return {"suggestions": suggestions}


def _edit_distance_bounded(s1, s2, max_dist):
    """Levenshtein edit distance, but stop early if exceeding max_dist."""
    if abs(len(s1) - len(s2)) > max_dist:
        return max_dist + 1
    if len(s1) < len(s2):
        s1, s2 = s2, s1
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        row_min = i + 1
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            val = min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost)
            curr.append(val)
            row_min = min(row_min, val)
        if row_min > max_dist:
            return max_dist + 1
        prev = curr
    return prev[-1]
