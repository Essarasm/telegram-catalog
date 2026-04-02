"""Issue reports and product request endpoints."""
import os
import logging
from pathlib import Path
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
from backend.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["reports"])

REPORT_TYPES = ["wrong_photo", "wrong_price", "wrong_name", "wrong_category", "other"]
VALID_STATUSES = ["new", "reviewed", "fixed", "dismissed"]

# Resolve images/ directory relative to repo root
IMAGES_DIR = Path(__file__).resolve().parent.parent.parent / "images"


# ── Pydantic models ──

class ReportCreate(BaseModel):
    product_id: int
    telegram_id: int
    report_type: str = "other"
    note: Optional[str] = None


class ReportStatusUpdate(BaseModel):
    status: str


class ProductRequestCreate(BaseModel):
    telegram_id: int
    request_text: str


# ── Telegram notification helper ──

async def notify_report_to_group(report_id: int, product_name: str, report_type: str, note: str | None):
    """Send report notification to Telegram group (fire-and-forget)."""
    try:
        import httpx
        bot_token = os.getenv("BOT_TOKEN", "")
        group_id = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))
        if not bot_token or not group_id:
            return

        type_labels = {
            "wrong_photo": "📷 Noto'g'ri rasm",
            "wrong_price": "💰 Noto'g'ri narx",
            "wrong_name": "📝 Noto'g'ri nom",
            "wrong_category": "📂 Noto'g'ri kategoriya",
            "other": "❓ Boshqa",
        }
        type_label = type_labels.get(report_type, report_type)

        text = (
            f"🚩 <b>Yangi xatolik xabari #{report_id}</b>\n\n"
            f"📦 Mahsulot: {product_name}\n"
            f"🏷 Turi: {type_label}"
        )
        if note:
            text += f"\n💬 Izoh: {note}"

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={"chat_id": group_id, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        logger.warning(f"Failed to notify report to group: {e}")


async def notify_product_request_to_group(request_id: int, request_text: str):
    """Send product request notification to Telegram group."""
    try:
        import httpx
        bot_token = os.getenv("BOT_TOKEN", "")
        group_id = int(os.getenv("ADMIN_GROUP_CHAT_ID", "-5224656051"))
        if not bot_token or not group_id:
            return

        text = (
            f"🔍 <b>Mahsulot so'rovi #{request_id}</b>\n\n"
            f"💬 {request_text}"
        )

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        async with httpx.AsyncClient() as client:
            await client.post(url, json={"chat_id": group_id, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        logger.warning(f"Failed to notify product request to group: {e}")


# ── Issue reports ──

@router.post("/reports")
async def create_report(body: ReportCreate):
    """Client reports an issue with a product."""
    if body.report_type not in REPORT_TYPES:
        return {"ok": False, "error": f"Invalid report_type. Must be one of: {REPORT_TYPES}"}

    conn = get_db()

    # Get product name for notification
    product = conn.execute("SELECT name, name_display FROM products WHERE id = ?", (body.product_id,)).fetchone()
    if not product:
        conn.close()
        return {"ok": False, "error": "Product not found"}

    conn.execute(
        "INSERT INTO reports (product_id, telegram_id, report_type, note) VALUES (?, ?, ?, ?)",
        (body.product_id, body.telegram_id, body.report_type, body.note),
    )
    conn.commit()
    report_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()

    product_name = product["name_display"] or product["name"]

    # Fire-and-forget notification
    try:
        await notify_report_to_group(report_id, product_name, body.report_type, body.note)
    except Exception:
        pass

    return {"ok": True, "report_id": report_id}


@router.get("/reports")
def list_reports(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Admin endpoint: list recent reports."""
    conn = get_db()
    conditions = []
    params = []

    if status:
        conditions.append("r.status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT r.id, r.product_id, p.name as product_name, p.name_display,
                   r.telegram_id, r.report_type, r.note, r.status, r.created_at
            FROM reports r
            JOIN products p ON p.id = r.product_id
            {where}
            ORDER BY r.created_at DESC
            LIMIT ?""",
        params + [limit],
    ).fetchall()
    conn.close()

    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
    }


# ── Wrong photo summary (must be before /{report_id} routes for path resolution) ──

@router.get("/reports/wrong-photos")
def wrong_photo_summary():
    """Admin endpoint: wrong_photo reports grouped by product, sorted by report count (priority)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT r.product_id, p.name_display, p.name as product_name, p.image_path,
                  COUNT(*) as report_count,
                  GROUP_CONCAT(r.id) as report_ids,
                  MIN(r.created_at) as first_reported,
                  MAX(r.created_at) as last_reported
           FROM reports r
           JOIN products p ON p.id = r.product_id
           WHERE r.report_type = 'wrong_photo' AND r.status = 'new'
           GROUP BY r.product_id
           ORDER BY report_count DESC, last_reported DESC""",
    ).fetchall()
    conn.close()

    items = []
    for r in rows:
        items.append({
            "product_id": r["product_id"],
            "product_name": r["name_display"] or r["product_name"],
            "has_photo": bool(r["image_path"]),
            "report_count": r["report_count"],
            "report_ids": [int(x) for x in r["report_ids"].split(",")],
            "first_reported": r["first_reported"],
            "last_reported": r["last_reported"],
        })

    return {"items": items, "count": len(items)}


# ── Report status update ──

@router.patch("/reports/{report_id}/status")
def update_report_status(report_id: int, body: ReportStatusUpdate):
    """Admin endpoint: update report status. TODO: add auth gate in future."""
    if body.status not in VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {VALID_STATUSES}")

    conn = get_db()

    report = conn.execute(
        "SELECT r.id, r.product_id, r.report_type, r.status as old_status FROM reports r WHERE r.id = ?",
        (report_id,),
    ).fetchone()
    if not report:
        conn.close()
        raise HTTPException(404, "Report not found")

    conn.execute("UPDATE reports SET status = ? WHERE id = ?", (body.status, report_id))
    conn.commit()

    result = {"ok": True, "report_id": report_id, "old_status": report["old_status"], "new_status": body.status}

    # If marking a wrong_photo report as "fixed", remove the photo
    if report["report_type"] == "wrong_photo" and body.status == "fixed":
        product_id = report["product_id"]
        photo_path = IMAGES_DIR / f"{product_id}.jpg"
        photo_removed = False

        if photo_path.exists():
            photo_path.unlink()
            photo_removed = True
            logger.info(f"Removed wrong photo: {photo_path}")

        # Clear image_path in DB so frontend falls back to emoji
        conn2 = get_db()
        conn2.execute("UPDATE products SET image_path = NULL WHERE id = ?", (product_id,))
        conn2.commit()
        conn2.close()
        logger.info(f"Cleared image_path for product #{product_id}")

        result["photo_removed"] = photo_removed
        result["product_id"] = product_id

    conn.close()
    return result


# ── Product requests ──

@router.post("/product-requests")
async def create_product_request(body: ProductRequestCreate):
    """Client requests a product they can't find."""
    if not body.request_text.strip():
        return {"ok": False, "error": "Request text cannot be empty"}

    conn = get_db()
    conn.execute(
        "INSERT INTO product_requests (telegram_id, request_text) VALUES (?, ?)",
        (body.telegram_id, body.request_text.strip()),
    )
    conn.commit()
    request_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()

    # Fire-and-forget notification
    try:
        await notify_product_request_to_group(request_id, body.request_text.strip())
    except Exception:
        pass

    return {"ok": True, "request_id": request_id}


@router.get("/product-requests")
def list_product_requests(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Admin endpoint: list product requests."""
    conn = get_db()
    conditions = []
    params = []

    if status:
        conditions.append("pr.status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = conn.execute(
        f"""SELECT pr.id, pr.telegram_id, pr.request_text, pr.status, pr.created_at
            FROM product_requests pr
            {where}
            ORDER BY pr.created_at DESC
            LIMIT ?""",
        params + [limit],
    ).fetchall()
    conn.close()

    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
    }
