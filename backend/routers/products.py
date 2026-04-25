import threading
from fastapi import APIRouter, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import Optional, List
from backend.database import get_db, transliterate_to_latin, transliterate_to_cyrillic, normalize_uzbek
from backend.services.update_prices import apply_price_updates
from backend.services.update_stock import apply_stock_updates
from backend.services.refresh_catalog import refresh_catalog_from_excel
from backend.admin_auth import check_admin_key

router = APIRouter(prefix="/api/products", tags=["products"])


# ── Fuzzy matching helpers ────────────────────────────────────────

def _edit_distance(s1, s2):
    """Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _edit_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            cost = 0 if c1 == c2 else 1
            curr.append(min(curr[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        prev = curr
    return prev[-1]


def _trigrams(s):
    """Generate character trigrams from a string."""
    s = f"  {s} "
    return {s[i:i+3] for i in range(len(s) - 2)}


def _trigram_similarity(s1, s2):
    """Trigram similarity between two strings (0.0 to 1.0)."""
    t1, t2 = _trigrams(s1), _trigrams(s2)
    if not t1 or not t2:
        return 0.0
    return len(t1 & t2) / len(t1 | t2)


def _score_match(search_term, search_latin, search_norm, product, search_cyrillic=None):
    """Score a product match: higher = better.
    4 = exact name match,
    3 = ANY word in either name starts with the term (covers "Грунтовка…" and "ХАЯТ грунт…"),
    2 = term appears anywhere in the search_text blob (producer / unit / category / mid-word),
    0 = no match.
    """
    st = (product["search_text"] or "").lower()
    name_disp = (product["name_display"] or "").lower()
    name_cyr = (product["name"] or "").lower()
    words = set(name_disp.split()) | set(name_cyr.split())

    variants = {search_term, search_latin, search_norm}
    if search_cyrillic:
        variants.add(search_cyrillic)

    best = 0
    for term in variants:
        if not term:
            continue
        if term == name_disp or term == name_cyr:
            return 4
        # Tier 3: any word in either name starts with the term
        if any(w.startswith(term) for w in words):
            best = max(best, 3)
            continue  # can't improve past 3 from this variant without hitting 4
        # Tier 2: fallback — term anywhere in combined search_text blob
        if term in st:
            best = max(best, 2)

    return best


def _fuzzy_match_products(conn, search_term, search_latin, search_norm, category_id=None, producer_id=None, max_results=30, search_cyrillic=None, lifecycle_filter=None, min_score=0.25):
    """Find products using fuzzy matching.
    Uses trigram similarity on product names.
    lifecycle_filter: tuple/list of lifecycle values to include (e.g. ('stale','never') for hidden-only).
    Returns list of (product_id, similarity_score) tuples, sorted by score desc.
    """
    conditions = ["p.is_active = 1"]
    params = []
    if category_id:
        conditions.append("p.category_id = ?")
        params.append(category_id)
    if producer_id:
        conditions.append("p.producer_id = ?")
        params.append(producer_id)
    if lifecycle_filter:
        placeholders = ",".join("?" for _ in lifecycle_filter)
        conditions.append(f"p.lifecycle IN ({placeholders})")
        params.extend(lifecycle_filter)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"""SELECT p.id, p.name, p.name_display, p.search_text
            FROM products p WHERE {where}""",
        params,
    ).fetchall()

    variants = {search_term, search_latin, search_norm}
    if search_cyrillic:
        variants.add(search_cyrillic)

    scored = []
    for r in rows:
        name_disp = (r["name_display"] or "").lower()
        name_cyr = (r["name"] or "").lower()
        best_sim = 0.0
        for term in variants:
            if not term or len(term) < 2:
                continue
            for name in (name_disp, name_cyr):
                sim = _trigram_similarity(term, name)
                best_sim = max(best_sim, sim)
                for word in name.split():
                    if len(word) >= 3:
                        wsim = _trigram_similarity(term, word)
                        best_sim = max(best_sim, wsim)
        if best_sim >= min_score:
            scored.append((best_sim, r["id"]))

    scored.sort(key=lambda x: -x[0])
    return [(pid, score) for score, pid in scored[:max_results]]


def _log_search_bg(telegram_id, query, results_count, category_id, producer_id):
    """Background thread: log search to search_logs table."""
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO search_logs (telegram_id, query, results_count, category_id, producer_id)
               VALUES (?, ?, ?, ?, ?)""",
            (telegram_id, query.strip().lower(), results_count, category_id, producer_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # Never fail the product request because of logging


@router.get("")
def list_products(
    category_id: Optional[int] = None,
    producer_id: Optional[int] = None,
    search: Optional[str] = None,
    telegram_id: Optional[int] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    lifecycle_strict: bool = Query(False, description="Session X / uncle's dashboard: filter to lifecycle='active' only (hide aging)"),
):
    """List products with filtering by category, producer, and/or search term.

    Search uses weighted ranking: exact match > starts with > contains > fuzzy.
    Falls back to fuzzy matching (trigram similarity) when exact search yields 0 results.
    """
    conn = get_db()
    offset = (page - 1) * limit
    # Catalog browse is limited to active+aging lifecycle.
    # Stale+never products are surfaced only via fuzzy search (see below) for demand-signal tracking.
    # lifecycle_strict (Session X dashboard): drop 'aging' so uncle sees only truly-active SKUs.
    if lifecycle_strict:
        conditions = ["p.is_active = 1", "p.lifecycle = 'active'"]
    else:
        conditions = ["p.is_active = 1", "p.lifecycle IN ('active','aging')"]
    params = []
    fuzzy_ids = None  # Will be set if we need fuzzy fallback
    hidden_matches = []  # (product_id, match_score) from stale/never, populated on search

    if category_id:
        conditions.append("p.category_id = ?")
        params.append(category_id)

    if producer_id:
        conditions.append("p.producer_id = ?")
        params.append(producer_id)

    search_term = None
    search_latin = None
    search_norm = None

    if search:
        search_term = search.strip().lower()
        search_latin = transliterate_to_latin(search_term)
        search_norm = normalize_uzbek(search_term)
        search_cyrillic = transliterate_to_cyrillic(search_term)

        # Build LIKE conditions for all query variants
        like_terms = set()
        like_terms.add(search_term)
        if search_latin != search_term:
            like_terms.add(search_latin)
        if search_norm != search_term and search_norm != search_latin:
            like_terms.add(search_norm)
        if search_cyrillic != search_term and search_cyrillic != search_latin:
            like_terms.add(search_cyrillic)

        like_conditions = " OR ".join(["p.search_text LIKE ?" for _ in like_terms])
        conditions.append(f"({like_conditions})")
        params.extend([f"%{t}%" for t in like_terms])

    where = " AND ".join(conditions)

    total = conn.execute(
        f"SELECT COUNT(*) FROM products p WHERE {where}",
        params,
    ).fetchone()[0]

    # If exact search yielded 0 results, try fuzzy matching
    if search and total == 0:
        fuzzy_results = _fuzzy_match_products(
            conn, search_term, search_latin, search_norm,
            category_id, producer_id, search_cyrillic=search_cyrillic,
            lifecycle_filter=('active', 'aging'),
        )
        fuzzy_ids = [pid for pid, _ in fuzzy_results]
        if fuzzy_ids:
            total = len(fuzzy_ids)
            # Get paginated slice of fuzzy results
            page_ids = fuzzy_ids[offset:offset + limit]
            if page_ids:
                placeholders = ",".join("?" for _ in page_ids)
                # Maintain fuzzy ranking order with CASE
                order_clause = "CASE p.id " + " ".join(
                    f"WHEN {pid} THEN {i}" for i, pid in enumerate(page_ids)
                ) + " END"
                rows = conn.execute(
                    f"""SELECT p.id, p.name, p.name_display, p.lifecycle, p.popularity_score,
                               p.category_id, c.name as category_name,
                               p.producer_id, pr.name as producer_name,
                               p.unit, p.price_usd, p.price_uzs, p.weight, p.image_path,
                               p.stock_quantity, p.stock_status
                        FROM products p
                        JOIN categories c ON c.id = p.category_id
                        JOIN producers pr ON pr.id = p.producer_id
                        WHERE p.id IN ({placeholders})
                        ORDER BY {order_clause}""",
                    page_ids,
                ).fetchall()
            else:
                rows = []
        else:
            rows = []
    elif search and total > 0:
        # Weighted ranking: fetch all matching products for this page and sort by relevance
        rows = conn.execute(
            f"""SELECT p.id, p.name, p.name_display, p.lifecycle, p.search_text, p.popularity_score,
                       p.category_id, c.name as category_name,
                       p.producer_id, pr.name as producer_name,
                       p.unit, p.price_usd, p.price_uzs, p.weight, p.image_path,
                       p.stock_quantity, p.stock_status
                FROM products p
                JOIN categories c ON c.id = p.category_id
                JOIN producers pr ON pr.id = p.producer_id
                WHERE {where}
                ORDER BY p.name""",
            params,
        ).fetchall()

        # Score and sort by relevance tier, then by popularity (most-sold first), then by name.
        scored = []
        for r in rows:
            score = _score_match(search_term, search_latin, search_norm, r, search_cyrillic)
            scored.append((score, dict(r)))
        scored.sort(key=lambda x: (-x[0], -(x[1].get("popularity_score") or 0), x[1].get("name_display", "")))

        # Paginate
        page_items = scored[offset:offset + limit]
        rows = [item for _, item in page_items]
        # Remove search_text from response (internal field)
        for item in rows:
            item.pop("search_text", None)
    else:
        rows = conn.execute(
            f"""SELECT p.id, p.name, p.name_display, p.lifecycle, p.category_id, c.name as category_name,
                       p.producer_id, pr.name as producer_name,
                       p.unit, p.price_usd, p.price_uzs, p.weight, p.image_path,
                       p.stock_quantity, p.stock_status
                FROM products p
                JOIN categories c ON c.id = p.category_id
                JOIN producers pr ON pr.id = p.producer_id
                WHERE {where}
                ORDER BY p.name
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()
    # Build filter chips data on first page of search results (no category/producer pre-selected)
    filters = None
    if search and page == 1 and not category_id and not producer_id:
        try:
            filter_rows = conn.execute(
                """SELECT c.id as cid, c.name as cname, pr.id as pid, pr.name as pname,
                          COUNT(*) as cnt
                   FROM products p
                   JOIN categories c ON c.id = p.category_id
                   JOIN producers pr ON pr.id = p.producer_id
                   WHERE p.is_active = 1 AND p.id IN (
                       SELECT p2.id FROM products p2 WHERE p2.is_active = 1
                       AND ({like_cond})
                   )
                   GROUP BY c.id, pr.id
                   ORDER BY cnt DESC""".format(
                    like_cond=" OR ".join(["p2.search_text LIKE ?" for _ in like_terms])
                ),
                [f"%{t}%" for t in like_terms],
            ).fetchall()
            cats = {}
            prods = {}
            for fr in filter_rows:
                if fr["cid"] not in cats:
                    cats[fr["cid"]] = {"id": fr["cid"], "name": fr["cname"], "count": 0}
                cats[fr["cid"]]["count"] += fr["cnt"]
                if fr["pid"] not in prods:
                    prods[fr["pid"]] = {"id": fr["pid"], "name": fr["pname"], "count": 0}
                prods[fr["pid"]]["count"] += fr["cnt"]
            filters = {
                "categories": sorted(cats.values(), key=lambda x: -x["count"]),
                "producers": sorted(prods.values(), key=lambda x: -x["count"]),
            }
        except Exception:
            pass

    # Log search in background (only on first page to avoid duplicates from pagination)
    if search and page == 1:
        threading.Thread(
            target=_log_search_bg,
            args=(telegram_id or 0, search, total, category_id, producer_id),
            daemon=True,
        ).start()

    # Normalize rows to dicts
    items = [dict(r) if not isinstance(r, dict) else r for r in rows]
    for item in items:
        item.pop("search_text", None)

    # Hidden products (stale/never lifecycle): surface on page 1 of search via loose fuzzy match (≥0.5)
    # to signal demand for discontinued SKUs. Appended AFTER visible items, marked with match_score.
    hidden_items = []
    if search and page == 1:
        hidden_matches = _fuzzy_match_products(
            conn, search_term, search_latin, search_norm,
            category_id, producer_id, max_results=10,
            search_cyrillic=search_cyrillic,
            lifecycle_filter=('stale', 'never'),
            min_score=0.5,
        )
        if hidden_matches:
            # Avoid duplicates if a hidden product somehow showed up in visible (shouldn't, but safe)
            visible_ids = {it['id'] for it in items}
            hidden_ids = [pid for pid, _ in hidden_matches if pid not in visible_ids]
            score_by_id = {pid: score for pid, score in hidden_matches}
            if hidden_ids:
                placeholders = ",".join("?" for _ in hidden_ids)
                hidden_rows = conn.execute(
                    f"""SELECT p.id, p.name, p.name_display, p.lifecycle,
                               p.category_id, c.name as category_name,
                               p.producer_id, pr.name as producer_name,
                               p.unit, p.price_usd, p.price_uzs, p.weight, p.image_path,
                               p.stock_quantity, p.stock_status
                        FROM products p
                        JOIN categories c ON c.id = p.category_id
                        JOIN producers pr ON pr.id = p.producer_id
                        WHERE p.id IN ({placeholders})""",
                    hidden_ids,
                ).fetchall()
                rows_by_id = {r["id"]: dict(r) for r in hidden_rows}
                # Preserve ranking order from hidden_matches
                for pid, _ in hidden_matches:
                    if pid in rows_by_id:
                        item = rows_by_id[pid]
                        item['match_score'] = round(score_by_id[pid], 3)
                        item['hidden'] = True  # frontend flag: disable cart, log interest click
                        hidden_items.append(item)

    conn.close()

    result = {
        "items": items + hidden_items,
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit,
        "fuzzy": fuzzy_ids is not None and len(fuzzy_ids) > 0 if fuzzy_ids is not None else False,
        "hidden_count": len(hidden_items),
    }
    if filters:
        result["filters"] = filters
    return result


@router.get("/by-ids")
def get_products_by_ids(ids: str = Query(..., description="Comma-separated product IDs")):
    """Batch lookup: return multiple products by their IDs (for cart reconstruction)."""
    try:
        id_list = [int(x.strip()) for x in ids.split(",") if x.strip()]
    except ValueError:
        return {"items": []}
    if not id_list:
        return {"items": []}

    placeholders = ",".join("?" for _ in id_list)
    conn = get_db()
    rows = conn.execute(
        f"""SELECT p.id, p.name, p.name_display, p.unit,
                   p.price_usd, p.price_uzs, p.image_path,
                   p.stock_quantity, p.stock_status
            FROM products p
            WHERE p.id IN ({placeholders})""",
        id_list,
    ).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows]}


@router.post("/update-prices")
async def update_prices(file: UploadFile = File(...), admin_key: str = Form("")):
    """Upload Excel file to update product prices."""
    if not check_admin_key(admin_key):
        return JSONResponse(status_code=403, content={"error": "Invalid admin key"})
    content = await file.read()
    result = apply_price_updates(content)
    return result


@router.post("/update-stock")
async def update_stock(
    file: UploadFile = File(...),
    admin_key: str = Form(""),
    force: str = Form(""),
):
    """Upload Excel file to update stock/inventory levels."""
    if not check_admin_key(admin_key):
        return JSONResponse(status_code=403, content={"error": "Invalid admin key"})
    content = await file.read()
    result = apply_stock_updates(content, force=bool(force))
    return result


@router.post("/refresh-catalog")
async def refresh_catalog(file: UploadFile = File(...), admin_key: str = Form("")):
    """Upload Excel file to refresh the product catalog (add new, deactivate removed)."""
    if not check_admin_key(admin_key):
        return JSONResponse(status_code=403, content={"error": "Invalid admin key"})
    content = await file.read()
    result = refresh_catalog_from_excel(content)
    return result


@router.get("/{product_id}")
def get_product(product_id: int):
    """Get single product details."""
    conn = get_db()
    row = conn.execute(
        """SELECT p.*, c.name as category_name, pr.name as producer_name
           FROM products p
           JOIN categories c ON c.id = p.category_id
           JOIN producers pr ON pr.id = p.producer_id
           WHERE p.id = ?""",
        (product_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {"error": "Product not found"}, 404
    return dict(row)
