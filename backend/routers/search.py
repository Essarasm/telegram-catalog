from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_db, transliterate_to_latin, transliterate_to_cyrillic, normalize_uzbek

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


# Interest-click tracking for hidden (stale/never) products — drives demand signal
# to the Inventory Telegram group.
INTEREST_THRESHOLD_USERS = 5       # distinct clients
INTEREST_WINDOW_DAYS = 30          # rolling window
INTEREST_COOLDOWN_DAYS = 60        # per-product cooldown between alerts


@router.post("/interest-click")
def interest_click(
    product_id: int = 0,
    telegram_id: int = 0,
    search_query: str = "",
    match_score: float = 0.0,
):
    """Log a click on a hidden product (lifecycle='stale' or 'never').

    Fires a demand-signal alert to the Inventory group if the product has
    ≥5 distinct clients clicking within the last 30 days AND it's been
    >60 days since the last alert (or never alerted).
    """
    if not product_id or not telegram_id:
        return {"ok": False, "reason": "missing_ids"}

    conn = get_db()

    # Verify product is actually hidden before logging — prevents abuse
    row = conn.execute(
        "SELECT id, name, name_display, lifecycle, last_interest_alert_at, "
        "       category_id, producer_id "
        "FROM products WHERE id = ?",
        (product_id,),
    ).fetchone()
    if not row or row["lifecycle"] not in ("stale", "never"):
        conn.close()
        return {"ok": False, "reason": "not_hidden"}

    conn.execute(
        """INSERT INTO product_interest_clicks
           (product_id, telegram_id, search_query, match_score)
           VALUES (?, ?, ?, ?)""",
        (product_id, telegram_id, (search_query or "")[:200], match_score or 0.0),
    )
    conn.commit()

    # Count distinct users in window
    distinct_users = conn.execute(
        """SELECT COUNT(DISTINCT telegram_id) FROM product_interest_clicks
           WHERE product_id = ? AND clicked_at >= datetime('now', ?)""",
        (product_id, f"-{INTEREST_WINDOW_DAYS} days"),
    ).fetchone()[0]

    alert_sent = False
    if distinct_users >= INTEREST_THRESHOLD_USERS:
        # Respect 60-day cooldown
        can_alert = row["last_interest_alert_at"] is None
        if not can_alert:
            still_in_cooldown = conn.execute(
                "SELECT ? < datetime('now', ?)",
                (row["last_interest_alert_at"], f"-{INTEREST_COOLDOWN_DAYS} days"),
            ).fetchone()[0]
            can_alert = bool(still_in_cooldown)

        if can_alert:
            # Fetch category + producer names for the alert
            meta = conn.execute(
                """SELECT c.name as category, pr.name as producer
                   FROM products p
                   LEFT JOIN categories c ON c.id = p.category_id
                   LEFT JOIN producers pr ON pr.id = p.producer_id
                   WHERE p.id = ?""",
                (product_id,),
            ).fetchone()

            # Send alert (synchronous, but has httpx timeout=10)
            try:
                from backend.services.notify_interest import send_interest_alert
                ok = send_interest_alert(
                    product_name=row["name"] or row["name_display"] or f"#{product_id}",
                    lifecycle=row["lifecycle"],
                    distinct_users=distinct_users,
                    window_days=INTEREST_WINDOW_DAYS,
                    category=meta["category"] if meta else "",
                    producer=meta["producer"] if meta else "",
                    last_supplied="",
                )
            except Exception:
                ok = False

            if ok:
                conn.execute(
                    "UPDATE products SET last_interest_alert_at = datetime('now') WHERE id = ?",
                    (product_id,),
                )
                conn.commit()
                alert_sent = True

    conn.close()
    return {
        "ok": True,
        "distinct_users": distinct_users,
        "alert_sent": alert_sent,
    }


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
    limit: int = Query(6, ge=1, le=20),
):
    """Rich dropdown suggestions, single list using the 1C Cyrillic name as display.

    The Cyrillic `products.name` field (raw 1C data from daily /stock + /prices uploads)
    is the primary source for both the displayed text AND the matching engine.
    Latin-typed queries are matched via transliteration variants so users can search
    in either script but see the 1C name.

    Matches full-search behaviour:
      - Filters to catalog-visible products (is_active=1, lifecycle IN (active, aging))
      - Multi-term AND: every word in the query must match the candidate
      - Per-term tiering: exact (4) > any-word-starts (3) > contains (2);
        product tier = min across all terms
      - Ordering: tier DESC, popularity DESC, name length ASC
      - Variants per term: raw, Latin-transliterated, Uzbek-normalized, Cyrillic-transliterated
      - Producer name folded into searchable text (many display names drop the brand)

    Response:
      {suggestions: [{id, text, producer, price_uzs, price_usd, unit,
                      stock_status, stock_quantity, image_path, popularity}],
       total_matches}
    """
    if not q or len(q.strip()) < 1:
        return {"suggestions": [], "total_matches": 0}

    raw = q.strip().lower()
    # Split into individual terms (whitespace-separated). Empty query handled above.
    terms = [t for t in raw.split() if t]
    if not terms:
        return {"suggestions": [], "total_matches": 0}

    # For each term, build variants: raw / latin-transliterated / uzbek-normalized / cyrillic-transliterated
    term_variants = []
    for t in terms:
        v = {t}
        v.add(transliterate_to_latin(t))
        v.add(normalize_uzbek(t))
        try:
            v.add(transliterate_to_cyrillic(t))
        except Exception:
            pass  # some input can't be round-tripped
        term_variants.append({x for x in v if x})

    # Candidate set: products where every term (some variant) appears in search_text.
    # This gives a broad superset; precise per-field tiering happens in Python.
    conn = get_db()
    conditions = ["p.is_active = 1", "p.lifecycle IN ('active','aging')"]
    params = []
    for variants in term_variants:
        any_variant_likes = " OR ".join(["LOWER(p.search_text) LIKE ?" for _ in variants])
        conditions.append(f"({any_variant_likes})")
        params.extend([f"%{v}%" for v in variants])
    where = " AND ".join(conditions)

    rows = conn.execute(
        f"""SELECT p.id, p.name, p.name_display, p.popularity_score,
                   p.price_uzs, p.price_usd, p.unit, p.stock_status, p.stock_quantity,
                   p.image_path,
                   pr.name as producer_name
            FROM products p
            LEFT JOIN producers pr ON pr.id = p.producer_id
            WHERE {where}
            LIMIT 100""",
        params,
    ).fetchall()

    total_matches = len(rows)

    def score_against(name_lower, producer_lower=""):
        """Returns the min-tier across all terms for this (name + producer) text.
        0 means at least one term doesn't match — exclude product.
        Producer name is folded in because many display names drop the brand
        (e.g. Weber → name_display='PF-115 Oq', producer='Weber')."""
        if not name_lower:
            return 0
        searchable = f"{name_lower} {producer_lower}".strip()
        words = set(searchable.split())
        min_tier = 4
        for variants in term_variants:
            best = 0
            for term in variants:
                if term == name_lower:
                    best = max(best, 4)
                elif any(w.startswith(term) for w in words):
                    best = max(best, 3)
                elif term in searchable:
                    best = max(best, 2)
            if best == 0:
                return 0
            min_tier = min(min_tier, best)
        return min_tier

    # Score every candidate against the Cyrillic name + producer (the 1C primary source).
    # The Latin display field is intentionally NOT scored separately — the old two-section
    # dropdown produced near-duplicate entries; single-list ranking is cleaner.
    scored = []
    for r in rows:
        name_cyr = (r["name"] or "").lower()
        name_lat = (r["name_display"] or "").lower()
        producer = (r["producer_name"] or "").lower()
        # Combine Cyrillic name + Latin display + producer — covers all scripts in one pass.
        combined = f"{name_cyr} {name_lat}".strip()
        tier = score_against(combined, producer)
        if tier > 0:
            pop = r["popularity_score"] or 0
            scored.append((tier, pop, len(name_cyr), r))

    scored.sort(key=lambda x: (-x[0], -x[1], x[2]))

    def to_item(row):
        # Always display the 1C Cyrillic name as the dropdown label.
        return {
            "id": row["id"],
            "text": row["name"],
            "name_cyrillic": row["name"],
            "name_display": row["name_display"],
            "producer": row["producer_name"],
            "price_uzs": row["price_uzs"],
            "price_usd": row["price_usd"],
            "unit": row["unit"],
            "stock_status": row["stock_status"],
            "stock_quantity": row["stock_quantity"] if "stock_quantity" in row.keys() else None,
            "image_path": row["image_path"],
            "popularity": row["popularity_score"] or 0,
        }

    suggestions = [to_item(r) for _, _, _, r in scored[:limit]]

    conn.close()
    return {
        "suggestions": suggestions,
        "total_matches": total_matches,
    }


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
