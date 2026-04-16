"""Agent-only dashboard endpoints (Phase 1: simple stats).

The MVP returns today / this-month realization volume for orders the agent
physically placed (orders.placed_by_telegram_id = agent.telegram_id). No
commission math yet — once we wire the Session T 3-tier producer table
into the DB, we'll add a /earnings endpoint that applies the rates.
"""
from datetime import date
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from backend.database import get_db

router = APIRouter(prefix="/api/agent", tags=["agent"])


def _is_agent(conn, telegram_id: int) -> bool:
    row = conn.execute(
        "SELECT is_agent FROM users WHERE telegram_id = ?", (telegram_id,),
    ).fetchone()
    return bool(row and row["is_agent"])


@router.get("/stats")
def agent_stats(telegram_id: int = Query(...)):
    """Return today + this-month order counts and totals placed by this agent."""
    conn = get_db()
    try:
        if not _is_agent(conn, telegram_id):
            return JSONResponse({"ok": False, "error": "not an agent"}, status_code=403)

        today_str = date.today().isoformat()
        month_prefix = today_str[:7]  # 'YYYY-MM'

        today = conn.execute(
            """SELECT COUNT(*) AS n,
                      COALESCE(SUM(total_uzs), 0) AS uzs,
                      COALESCE(SUM(total_usd), 0) AS usd
               FROM orders
               WHERE placed_by_telegram_id = ?
                 AND date(created_at) = ?""",
            (telegram_id, today_str),
        ).fetchone()

        month = conn.execute(
            """SELECT COUNT(*) AS n,
                      COALESCE(SUM(total_uzs), 0) AS uzs,
                      COALESCE(SUM(total_usd), 0) AS usd,
                      COUNT(DISTINCT client_id) AS clients
               FROM orders
               WHERE placed_by_telegram_id = ?
                 AND substr(created_at, 1, 7) = ?""",
            (telegram_id, month_prefix),
        ).fetchone()

        # Recent orders (last 5 placed by this agent)
        recent = conn.execute(
            """SELECT o.id, o.created_at, o.total_uzs, o.total_usd,
                      o.item_count, ac.client_id_1c
               FROM orders o
               LEFT JOIN allowed_clients ac ON ac.id = o.client_id
               WHERE o.placed_by_telegram_id = ?
               ORDER BY o.created_at DESC
               LIMIT 5""",
            (telegram_id,),
        ).fetchall()

        return {
            "ok": True,
            "is_agent": True,
            "today": {
                "order_count": today["n"],
                "total_uzs": round(float(today["uzs"]) or 0),
                "total_usd": round(float(today["usd"]) or 0, 2),
            },
            "month": {
                "order_count": month["n"],
                "total_uzs": round(float(month["uzs"]) or 0),
                "total_usd": round(float(month["usd"]) or 0, 2),
                "unique_clients": month["clients"],
            },
            "recent_orders": [
                {
                    "id": r["id"],
                    "created_at": r["created_at"],
                    "client_1c": r["client_id_1c"] or "—",
                    "item_count": r["item_count"],
                    "total_uzs": round(float(r["total_uzs"]) or 0),
                    "total_usd": round(float(r["total_usd"]) or 0, 2),
                }
                for r in recent
            ],
        }
    finally:
        conn.close()
