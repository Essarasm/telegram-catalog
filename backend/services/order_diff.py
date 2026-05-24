"""Line-by-line diff between a Mini App wishlist order and the 1C real
order, for the post-confirmation client DM and Sotuv echo.

Used by `bot/handlers/orders.py:handle_order_confirmation_reply` after a
Sotuv handler replies with the 1C-exported Excel. The diff is computed
on demand and never persisted — both sides remain reconstructible from
`order_items` (Mini App) and `confirmed_orders.items_json` (1C parse).
"""

from __future__ import annotations
from html import escape as html_escape
from typing import List, Dict, Any, Optional

from backend.database import get_db


# Float roundtrip / 1C-rounding noise thresholds — anything below counts as
# "same price" so trivial penny diffs don't trigger client notifications.
_UZS_EPSILON = 1.0
_USD_EPSILON = 0.005

# Telegram message body is capped at 4096 chars; with header + totals +
# 2-line entries we comfortably fit ~15 product diffs before the tail.
_MAX_LINES_IN_DM = 15


def _try_match_product_id(name_1c: str, conn) -> Optional[int]:
    """Resolve a 1C product name to products.id.

    Mirrors backend.services.import_real_orders._try_match_product (exact
    + lowercase fallback) but without the import-time module cache —
    diffs run once per order confirmation, low enough volume to skip it.
    """
    if not name_1c:
        return None
    row = conn.execute(
        "SELECT id FROM products WHERE name = ? LIMIT 1",
        (name_1c,),
    ).fetchone()
    if row:
        return row["id"]
    normalized = name_1c.strip().lower()
    row = conn.execute(
        "SELECT id FROM products WHERE LOWER(TRIM(name)) = ? LIMIT 1",
        (normalized,),
    ).fetchone()
    return row["id"] if row else None


def compute_order_diff(
    wishlist_order_id: int,
    real_items_payload: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute the structured diff. See module docstring for context.

    Args:
        wishlist_order_id: orders.id (Mini App side; line items read from
            order_items).
        real_items_payload: list of dicts in the shape constructed in
            `bot/handlers/orders.py` after `parse_real_orders_xls` — each
            item has keys `name`, `qty`, `price_uzs`, `price_usd`,
            `total_uzs`, `total_usd`.

    Returns:
        Dict with:
        - `has_diff`: bool — any per-line diff OR totals delta above epsilon
        - `lines`: list of per-line diffs; each has `type` in
          {changed, missing, added} plus name/qty/price fields
        - `totals_delta`: {`uzs`, `usd`} = real_total − wish_total per currency
        - `counts`: {`changed`, `missing`, `added`} integer summary
    """
    conn = get_db()
    try:
        wishlist_rows = conn.execute(
            """SELECT oi.product_id, oi.product_name, oi.quantity, oi.price,
                      oi.currency, p.name_display, p.name as name_1c
               FROM order_items oi
               LEFT JOIN products p ON p.id = oi.product_id
               WHERE oi.order_id = ?""",
            (wishlist_order_id,),
        ).fetchall()

        wish_by_pid: Dict[int, Dict[str, Any]] = {}
        for r in wishlist_rows:
            pid = r["product_id"]
            if pid is None:
                # Mini App items normally always carry product_id; defensive
                # skip — without a key there's nothing to join on.
                continue
            currency = (r["currency"] or "USD").upper()
            qty = float(r["quantity"] or 0)
            price = float(r["price"] or 0)
            display = r["name_display"] or r["name_1c"] or r["product_name"] or "—"
            wish_by_pid[pid] = {
                "name_display": display,
                "qty": qty,
                "price_uzs": price if currency == "UZS" else 0.0,
                "price_usd": price if currency == "USD" else 0.0,
                "total_uzs": price * qty if currency == "UZS" else 0.0,
                "total_usd": price * qty if currency == "USD" else 0.0,
            }

        real_by_pid: Dict[int, Dict[str, Any]] = {}
        real_unresolved: List[Dict[str, Any]] = []
        for it in real_items_payload:
            pid = _try_match_product_id(it.get("name") or "", conn)
            line = {
                "name_1c": it.get("name") or "",
                "qty": float(it.get("qty") or 0),
                "price_uzs": float(it.get("price_uzs") or 0),
                "price_usd": float(it.get("price_usd") or 0),
                "total_uzs": float(it.get("total_uzs") or 0),
                "total_usd": float(it.get("total_usd") or 0),
            }
            if pid is not None:
                real_by_pid[pid] = line
            else:
                real_unresolved.append(line)

        # Display-name lookup for "added" lines whose product_id resolved
        # but never appeared on the wishlist.
        added_pids = set(real_by_pid.keys()) - set(wish_by_pid.keys())
        added_display: Dict[int, str] = {}
        if added_pids:
            placeholders = ",".join("?" for _ in added_pids)
            for r in conn.execute(
                f"SELECT id, name_display, name FROM products WHERE id IN ({placeholders})",
                tuple(added_pids),
            ).fetchall():
                added_display[r["id"]] = r["name_display"] or r["name"] or "—"
    finally:
        conn.close()

    lines: List[Dict[str, Any]] = []
    counts = {"changed": 0, "missing": 0, "added": 0}

    for pid, w in wish_by_pid.items():
        r = real_by_pid.get(pid)
        if r is None:
            lines.append({
                "type": "missing",
                "name": w["name_display"],
                "wish_qty": w["qty"],
            })
            counts["missing"] += 1
            continue
        qty_delta = r["qty"] - w["qty"]
        uzs_price_delta = r["price_uzs"] - w["price_uzs"]
        usd_price_delta = r["price_usd"] - w["price_usd"]
        if (
            abs(qty_delta) > 0
            or abs(uzs_price_delta) >= _UZS_EPSILON
            or abs(usd_price_delta) >= _USD_EPSILON
        ):
            lines.append({
                "type": "changed",
                "name": w["name_display"],
                "wish_qty": w["qty"],
                "real_qty": r["qty"],
                "qty_delta": qty_delta,
                "wish_price_uzs": w["price_uzs"],
                "real_price_uzs": r["price_uzs"],
                "wish_price_usd": w["price_usd"],
                "real_price_usd": r["price_usd"],
            })
            counts["changed"] += 1

    for pid in added_pids:
        r = real_by_pid[pid]
        lines.append({
            "type": "added",
            "name": added_display.get(pid) or r["name_1c"] or "—",
            "real_qty": r["qty"],
        })
        counts["added"] += 1

    for r in real_unresolved:
        lines.append({
            "type": "added",
            "name": r["name_1c"] or "—",
            "real_qty": r["qty"],
        })
        counts["added"] += 1

    wish_total_uzs = sum(w["total_uzs"] for w in wish_by_pid.values())
    wish_total_usd = sum(w["total_usd"] for w in wish_by_pid.values())
    real_total_uzs = (
        sum(r["total_uzs"] for r in real_by_pid.values())
        + sum(r["total_uzs"] for r in real_unresolved)
    )
    real_total_usd = (
        sum(r["total_usd"] for r in real_by_pid.values())
        + sum(r["total_usd"] for r in real_unresolved)
    )
    totals_delta = {
        "uzs": real_total_uzs - wish_total_uzs,
        "usd": real_total_usd - wish_total_usd,
    }

    # has_diff is count-driven only. Totals-delta accumulates per-line
    # rounding noise (N_lines × per_line_epsilon), so a totals-only trigger
    # produces spurious notifications when each line is individually below
    # the noise floor. If a totals diff is real, it must surface as ≥1
    # changed/missing/added line.
    has_diff = counts["changed"] + counts["missing"] + counts["added"] > 0

    return {
        "has_diff": has_diff,
        "lines": lines,
        "totals_delta": totals_delta,
        "counts": counts,
    }


# ── Formatters ─────────────────────────────────────────────────────


def _fmt_uzs(v: float) -> str:
    return f"{v:,.0f}".replace(",", " ")


def _fmt_usd(v: float) -> str:
    return f"{v:,.2f}".replace(",", " ")


def _fmt_qty(v: float) -> str:
    # Drop trailing .0 for whole numbers; keep up to 3 decimals otherwise.
    if abs(v - round(v)) < 1e-6:
        return f"{int(round(v))}"
    return f"{v:.3f}".rstrip("0").rstrip(".")


def format_diff_for_client(diff: Dict[str, Any], cabinet_url: str = "") -> str:
    """Render the diff as Uzbek HTML for the client DM. Returns empty string
    when has_diff is False (caller should use the generic match-case message).
    """
    if not diff["has_diff"]:
        return ""

    parts: List[str] = ["✅ <b>Buyurtmangiz 1C ga kiritildi</b>", ""]
    parts.append("Quyidagi farqlar bor:")
    parts.append("")

    # changed → missing → added; alphabetical within group for stability
    type_order = {"changed": 0, "missing": 1, "added": 2}
    sorted_lines = sorted(
        diff["lines"],
        key=lambda L: (type_order.get(L["type"], 9), L.get("name", "")),
    )

    truncated = max(0, len(sorted_lines) - _MAX_LINES_IN_DM)
    for L in sorted_lines[:_MAX_LINES_IN_DM]:
        name = html_escape(L["name"])
        if L["type"] == "changed":
            qd = L["qty_delta"]
            qd_str = ""
            if abs(qd) > 0:
                sign = "+" if qd > 0 else "−"
                qd_str = f" ({sign}{_fmt_qty(abs(qd))})"
            row = f"• <b>{name}</b>: {_fmt_qty(L['wish_qty'])}→{_fmt_qty(L['real_qty'])} dona{qd_str}"
            if abs(L["real_price_uzs"] - L["wish_price_uzs"]) >= _UZS_EPSILON:
                row += (
                    f"\n  narx: {_fmt_uzs(L['wish_price_uzs'])} → "
                    f"{_fmt_uzs(L['real_price_uzs'])} so'm"
                )
            if abs(L["real_price_usd"] - L["wish_price_usd"]) >= _USD_EPSILON:
                row += (
                    f"\n  narx: ${_fmt_usd(L['wish_price_usd'])} → "
                    f"${_fmt_usd(L['real_price_usd'])}"
                )
            parts.append(row)
        elif L["type"] == "missing":
            parts.append(f"• <b>{name}</b>: {_fmt_qty(L['wish_qty'])}→0 (yo'q)")
        elif L["type"] == "added":
            parts.append(f"• <b>{name}</b>: +{_fmt_qty(L['real_qty'])} (yangi)")

    if truncated:
        parts.append(f"…va yana {truncated} ta farq")

    td = diff["totals_delta"]
    uzs_d, usd_d = td["uzs"], td["usd"]
    if abs(uzs_d) >= _UZS_EPSILON or abs(usd_d) >= _USD_EPSILON:
        parts.append("")
        parts.append("<b>Jami farq:</b>")
        if abs(uzs_d) >= _UZS_EPSILON:
            sign = "+" if uzs_d > 0 else "−"
            parts.append(f"  {sign}{_fmt_uzs(abs(uzs_d))} so'm")
        if abs(usd_d) >= _USD_EPSILON:
            sign = "+" if usd_d > 0 else "−"
            parts.append(f"  {sign}${_fmt_usd(abs(usd_d))}")

    if cabinet_url:
        parts.append("")
        parts.append(f"📋 Batafsil: <a href=\"{cabinet_url}\">Kabinet</a>")

    return "\n".join(parts)


def format_diff_for_sotuv(diff: Dict[str, Any]) -> str:
    """Terse one-or-two-liner echoed back in the Sotuv group so handlers
    see at a glance that the client got a diff message + how many lines.
    """
    if not diff["has_diff"]:
        return ""
    c = diff["counts"]
    total = c["changed"] + c["missing"] + c["added"]
    head = f"✅ Mijozga xabar yuborildi · {total} ta farq"
    bits = []
    if c["changed"]:
        bits.append(f"{c['changed']} o'zgarish")
    if c["missing"]:
        bits.append(f"{c['missing']} yo'q")
    if c["added"]:
        bits.append(f"{c['added']} yangi")
    if bits:
        return f"{head}\n   ({', '.join(bits)})"
    return head
