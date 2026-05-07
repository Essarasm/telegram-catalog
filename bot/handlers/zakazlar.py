"""/zakazlar — per-supplier reorder view (Session N Phase 1).

Inventory-group command. Lists active suppliers as inline-keyboard buttons;
clicking one returns BOTH:
  (a) a simple chat message with name + qoldiq + buyurtma per item
      (read-top-down to place an order — non-tech-friendly), and
  (b) an xlsx attachment with 4 sheets for desktop analysis:
      Tushuntirish (trilingual guide) / Buyurtma / Hammasi / Yig'ma.

Formula: rolling 90-day daily sales rate × 30-day buffer − current stock.
No prices (per memory feedback_order_prep_no_prices).
"""
from __future__ import annotations

import io
import logging
from typing import List

from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot.shared import is_admin
from backend.services.reorder import (
    DEFAULT_BUFFER_DAYS,
    DEFAULT_WINDOW_DAYS,
    compute_supplier_reorder,
    list_supplier_full,
    list_suppliers_with_products,
)

router = Router()
logger = logging.getLogger(__name__)


# ── Inline keyboard ─────────────────────────────────────────────────────

def _supplier_keyboard(suppliers: List[dict]) -> InlineKeyboardMarkup:
    """Two-column supplier list. Format: 'NAME (oos/total)'."""
    rows = []
    pair: List[InlineKeyboardButton] = []
    for s in suppliers:
        label = f"{s['name_1c']} ({s['oos_count']}/{s['product_count']})"
        if len(label) > 60:
            label = label[:57] + "..."
        cb = f"zakaz:s:{s['id']}" if s['id'] is not None else "zakaz:s:none"
        pair.append(InlineKeyboardButton(text=label, callback_data=cb))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([InlineKeyboardButton(text="❌ Yopish", callback_data="zakaz:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Simple chat text (name + qoldiq + buyurtma only) ────────────────────

def _format_simple_text(supplier_label: str, items: List[dict]) -> str:
    """Plain text: 1 line per item. Reads top-down for hand-placing an order."""
    if not items:
        return (f"📦 {supplier_label}\n\n"
                f"Hech qanday mahsulot uchun buyurtma kerak emas.\n"
                f"(Oxirgi {DEFAULT_WINDOW_DAYS} kun sotuv × {DEFAULT_BUFFER_DAYS} "
                f"kun bufer formulasiga ko'ra qoldiq yetarli.)")

    total_buy = sum(it["suggested_buy"] for it in items)
    lines = [
        f"📦 {supplier_label}",
        f"Buyurtma kerak: {len(items)} ta mahsulot · jami {total_buy:,} dona",
        "",
    ]
    for i, it in enumerate(items, 1):
        name = it["name"]
        if len(name) > 55:
            name = name[:52] + "..."
        lines.append(f"{i}. {name}")
        lines.append(f"   Qoldiq: {int(it['stock'])} → Buyurtma: {it['suggested_buy']}")
    lines.append("")
    lines.append(f"💡 Batafsil ma'lumot — biriktirilgan Excel fayldan oching.")
    return "\n".join(lines)


def _chunk_text(text: str, limit: int = 3900) -> List[str]:
    """Split on newlines so chunks fit inside Telegram's 4096-char limit."""
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = (cur + "\n" + line) if cur else line
    if cur:
        chunks.append(cur)
    return chunks


# ── 4-sheet xlsx (mirrors Plintus_Reorder structure) ────────────────────

def _build_xlsx(supplier_label: str, reorder_items: List[dict],
                full_items: List[dict]) -> bytes:
    """Generate the rich xlsx — same shape as Plintus_Reorder_2026-05-07.xlsx."""
    from datetime import date
    from openpyxl import Workbook
    from openpyxl.comments import Comment
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    bold = Font(bold=True)
    head_font = Font(bold=True, color="FFFFFF")
    head_fill = PatternFill("solid", fgColor="2E4F73")
    red_fill = PatternFill("solid", fgColor="FFD6D6")
    amber_fill = PatternFill("solid", fgColor="FFF3CC")
    green_fill = PatternFill("solid", fgColor="D8F0D8")
    center = Alignment(horizontal="center", vertical="center")
    right = Alignment(horizontal="right", vertical="center")
    left = Alignment(horizontal="left", vertical="center", wrap_text=True)
    left_top = Alignment(horizontal="left", vertical="top", wrap_text=True)
    thin = Side(style="thin", color="BBBBBB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    # Column metadata: (label, key, width, EN, RU, UZ)
    COL_DEFS = [
        ("#", "rank", 5,
         "Rank: top of list = biggest reorder need (sorted by Buyurtma desc).",
         "Ранг: вверху — самые срочные позиции.",
         "Tartib: yuqorida eng katta buyurtma."),
        ("Mahsulot nomi", "name", 42,
         "Full 1C product name (Cyrillic). Copy verbatim when ordering.",
         "Полное название из 1С (кириллица). Копировать дословно.",
         "1C-dagi to'liq nom (kirill). Buyurtmada aynan shu matnni nusxalang."),
        ("Stock", "stock", 8,
         "Current warehouse stock from latest 1C balances upload (donalar).",
         "Текущий складской остаток из последней загрузки 1С (шт.).",
         "Hozirgi ombor qoldig'i (oxirgi 1C yuklamasi, dona)."),
        (f"Sotilgan ({DEFAULT_WINDOW_DAYS}d)", "sold_window", 14,
         f"Units sold in the last {DEFAULT_WINDOW_DAYS} days (rolling window).",
         f"Продано за последние {DEFAULT_WINDOW_DAYS} дней.",
         f"Oxirgi {DEFAULT_WINDOW_DAYS} kunda sotilgan dona."),
        ("Kunlik rate", "daily_rate", 11,
         f"Average daily sales = sotilgan / {DEFAULT_WINDOW_DAYS} days.",
         f"Среднесуточные продажи = sotilgan / {DEFAULT_WINDOW_DAYS}.",
         f"O'rtacha kunlik sotuv = sotilgan / {DEFAULT_WINDOW_DAYS}."),
        ("Buyurtma", "suggested_buy", 11,
         f"Suggested order qty = ceil(daily_rate × {DEFAULT_BUFFER_DAYS}) − stock. 0 = enough on hand.",
         f"Рекомендуемый заказ = ceil(daily_rate × {DEFAULT_BUFFER_DAYS}) − stock. 0 = хватает.",
         f"Tavsiya qilingan buyurtma = ceil(kunlik × {DEFAULT_BUFFER_DAYS}) − qoldiq. 0 = yetarli."),
        ("Last sale", "last_sale", 11,
         "Most recent date this SKU was sold. >90 days = check if dying.",
         "Дата последней продажи. Старше 90 дней — проверьте.",
         "Oxirgi sotilgan sana. 90 kundan eski bo'lsa, tekshiring."),
    ]

    today = date.today().isoformat()
    wb = Workbook()

    # ────────── Sheet 1: Tushuntirish (Guide) ──────────
    ws_g = wb.active
    ws_g.title = "Tushuntirish (Guide)"

    ws_g["A1"] = f"{supplier_label} — Buyurtma Tahlili / Анализ заказа / Order Analysis"
    ws_g["A1"].font = Font(bold=True, size=15)
    ws_g.merge_cells("A1:D1")
    ws_g["A2"] = (f"Sana / Дата / Date: {today} | "
                  f"Window: {DEFAULT_WINDOW_DAYS}d sales | "
                  f"Buffer: {DEFAULT_BUFFER_DAYS}d | "
                  f"{len(full_items)} ta mahsulot · {len(reorder_items)} buyurtma kerak")
    ws_g["A2"].font = Font(italic=True, color="666666")
    ws_g.merge_cells("A2:D2")

    def section(ws, row, title):
        c = ws.cell(row=row, column=1, value=title)
        c.font = Font(bold=True, size=13, color="FFFFFF")
        c.fill = head_fill
        c.alignment = left
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=4)
        ws.row_dimensions[row].height = 22
        return row + 1

    r = 4
    r = section(ws_g, r, "1. Bu hisobot nima haqida / О чём этот отчёт / What this report does")
    overview = [
        ("UZ", f"'{supplier_label}' yetkazib beruvchidagi {len(full_items)} ta aktiv mahsulot uchun "
               f"buyurtma tavsiyasi. Formula: oxirgi {DEFAULT_WINDOW_DAYS} kun sotuvi × "
               f"{DEFAULT_BUFFER_DAYS} kun buferi − hozirgi qoldiq."),
        ("RU", f"Рекомендация по заказу для {len(full_items)} активных товаров поставщика "
               f"'{supplier_label}'. Формула: продажи за последние {DEFAULT_WINDOW_DAYS} дней × "
               f"{DEFAULT_BUFFER_DAYS}-дневный буфер − текущий остаток."),
        ("EN", f"Reorder recommendation for {len(full_items)} active products mapped to "
               f"'{supplier_label}'. Formula: last-{DEFAULT_WINDOW_DAYS}-day sales × "
               f"{DEFAULT_BUFFER_DAYS}-day buffer − current stock."),
    ]
    for lang, text in overview:
        ws_g.cell(row=r, column=1, value=lang).font = bold
        ws_g.cell(row=r, column=1).alignment = Alignment(horizontal="center", vertical="top")
        ws_g.cell(row=r, column=2, value=text).alignment = left_top
        ws_g.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
        ws_g.row_dimensions[r].height = 50
        r += 1

    r += 1
    r = section(ws_g, r, "2. Hisoblash usuli / Метод расчёта / Calculation method")
    method_lines = [
        ("Window / Окно", f"{DEFAULT_WINDOW_DAYS} kun",
         f"Oxirgi {DEFAULT_WINDOW_DAYS} kun sotuvi sanovi.",
         f"Считаем продажи за последние {DEFAULT_WINDOW_DAYS} дней."),
        ("Buffer / Буфер", f"{DEFAULT_BUFFER_DAYS} kun",
         f"Buyurtma {DEFAULT_BUFFER_DAYS} kunlik talabni qoplashga mo'ljallangan.",
         f"Заказ покрывает {DEFAULT_BUFFER_DAYS} дней спроса."),
        ("Daily rate", "sotilgan / 90",
         f"Kunlik o'rtacha sotuv darajasi.",
         f"Среднесуточные продажи."),
        ("Buyurtma", "ceil(daily × 30) − stock",
         f"Manfiy bo'lsa 0 — yetarli.",
         f"Если отрицательное — 0, запас достаточен."),
        ("Mavsumiy?", "Yo'q (flat-rate)",
         f"Bu formula mavsumiylikni hisobga olmaydi. Cho'qqi mavsumda kam baholashi mumkin.",
         f"Формула не учитывает сезонность; в пиковый сезон может занижать."),
    ]
    headers = ["Atama / Параметр / Term", "Qiymat / Значение / Value",
               "Tushuntirish (UZ)", "Объяснение (RU)"]
    for ci, h in enumerate(headers, start=1):
        c = ws_g.cell(row=r, column=ci, value=h)
        c.font = head_font
        c.fill = head_fill
        c.alignment = center
        c.border = border
    ws_g.row_dimensions[r].height = 24
    r += 1
    for term, val, uz, ru in method_lines:
        ws_g.cell(row=r, column=1, value=term).font = bold
        ws_g.cell(row=r, column=1).alignment = left_top
        ws_g.cell(row=r, column=2, value=val).alignment = left_top
        ws_g.cell(row=r, column=3, value=uz).alignment = left_top
        ws_g.cell(row=r, column=4, value=ru).alignment = left_top
        for ci in range(1, 5):
            ws_g.cell(row=r, column=ci).border = border
        ws_g.row_dimensions[r].height = 50
        r += 1

    r += 1
    r = section(ws_g, r, "3. Ustun tushuntirishlari / Описание колонок / Column reference")
    headers = ["Ustun / Колонка / Column", "EN", "RU", "UZ"]
    for ci, h in enumerate(headers, start=1):
        c = ws_g.cell(row=r, column=ci, value=h)
        c.font = head_font
        c.fill = head_fill
        c.alignment = center
        c.border = border
    ws_g.row_dimensions[r].height = 22
    r += 1
    for label, key, width, en, ru, uz in COL_DEFS:
        ws_g.cell(row=r, column=1, value=label).font = bold
        ws_g.cell(row=r, column=1).alignment = left_top
        ws_g.cell(row=r, column=2, value=en).alignment = left_top
        ws_g.cell(row=r, column=3, value=ru).alignment = left_top
        ws_g.cell(row=r, column=4, value=uz).alignment = left_top
        for ci in range(1, 5):
            ws_g.cell(row=r, column=ci).border = border
        ws_g.row_dimensions[r].height = 40
        r += 1

    r += 1
    r = section(ws_g, r, "4. Qaror qabul qilish / Принятие решения / Decision rubric")
    decision = [
        ("Qizil — RED", "Stock=0 + Buyurtma>0", red_fill,
         "OUT OF STOCK. Sales lost until restocked. HIGHEST URGENCY.",
         "НЕТ В НАЛИЧИИ. Теряем продажи. Высший приоритет.",
         "Sotuvda yo'q. Sotuv yo'qotamiz. Eng yuqori shoshiluvchanlik."),
        ("Sariq — AMBER", "Stock>0 + Buyurtma>0", amber_fill,
         "Stock exists but won't cover the buffer window. Order to top up.",
         "Есть остаток, но не хватит на буфер. Дозаказать.",
         "Qoldiq bor, lekin bufer davriga yetmaydi. To'ldirish kerak."),
        ("Yashil — GREEN", "Stock>0 + Buyurtma=0", green_fill,
         "Sufficient stock. Skip this order.",
         "Достаточно. Не заказывать.",
         "Yetarli. Buyurtma bermang."),
        ("Bo'sh — N/A", "Stock>0 + Sotilgan=0", PatternFill("solid", fgColor="FFFFFF"),
         "Slow-mover or retired. Verify with uncle before reordering.",
         "Не движется или списан. Уточнить у дяди.",
         "Sekin yoki olib tashlangan. Amakidan tekshiring."),
    ]
    headers = ["Belgi / Маркер / Marker", "Shart / Условие / Condition",
               "EN", "RU + UZ"]
    for ci, h in enumerate(headers, start=1):
        c = ws_g.cell(row=r, column=ci, value=h)
        c.font = head_font
        c.fill = head_fill
        c.alignment = center
        c.border = border
    ws_g.row_dimensions[r].height = 22
    r += 1
    for marker, cond, fill, en, ru, uz in decision:
        c1 = ws_g.cell(row=r, column=1, value=marker)
        c1.font = bold
        c1.alignment = center
        c1.fill = fill
        ws_g.cell(row=r, column=2, value=cond).alignment = left_top
        ws_g.cell(row=r, column=3, value=en).alignment = left_top
        ws_g.cell(row=r, column=4, value=f"RU: {ru}\nUZ: {uz}").alignment = left_top
        for ci in range(1, 5):
            ws_g.cell(row=r, column=ci).border = border
        ws_g.row_dimensions[r].height = 50
        r += 1

    r += 1
    r = section(ws_g, r, "5. Cheklovlar / Ограничения / Caveats")
    cav = [
        ("UZ",
         "• Mavsumiylik hisobga olinmagan. Mavsumiy mahsulotlar uchun cho'qqi davrida kam baholashi mumkin.\n"
         "• Yetkazib berish muddati hisobga olinmagan. Agar 4+ hafta bo'lsa, buyurtmani oldinroq bering.\n"
         "• 0 sotilgan + 0 qoldiq mahsulotlar — eskirgan bo'lishi mumkin, ko'rib chiqish kerak."),
        ("RU",
         "• Сезонность не учтена. Для сезонных товаров в пик может занижать заказ.\n"
         "• Срок поставки не учтён. Если 4+ недель — заказывать заранее.\n"
         "• Товары с 0 продаж и 0 остатком возможно сняты с продаж — проверить."),
        ("EN",
         "• Seasonality not modelled. May undershoot for seasonal SKUs in peak season.\n"
         "• Supplier lead time not modelled. Order earlier if lead time ≥4 weeks.\n"
         "• SKUs with zero sales + zero stock may be retired — review."),
    ]
    for lang, text in cav:
        ws_g.cell(row=r, column=1, value=lang).font = bold
        ws_g.cell(row=r, column=1).alignment = Alignment(horizontal="center", vertical="top")
        ws_g.cell(row=r, column=2, value=text).alignment = left_top
        ws_g.merge_cells(start_row=r, start_column=2, end_row=r, end_column=4)
        ws_g.row_dimensions[r].height = 65
        r += 1

    for col, w in [("A", 22), ("B", 28), ("C", 50), ("D", 50)]:
        ws_g.column_dimensions[col].width = w

    # ────────── Sheet 2: Buyurtma (Reorder candidates) ──────────
    ws = wb.create_sheet("Buyurtma (Order)")
    ws["A1"] = f"{supplier_label} — Buyurtma kerak ({len(reorder_items)} ta mahsulot)"
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells("A1:G1")
    ws["A2"] = ("Hover the column header for explanation. "
                "See 'Tushuntirish' sheet for full reference.")
    ws["A2"].font = Font(italic=True, color="666666")
    ws.merge_cells("A2:G2")

    for ci, (label, key, width, en, ru, uz) in enumerate(COL_DEFS, start=1):
        c = ws.cell(row=4, column=ci, value=label)
        c.font = head_font
        c.fill = head_fill
        c.alignment = center
        c.border = border
        cm = Comment(f"EN: {en}\n\nRU: {ru}\n\nUZ: {uz}", "Reorder Analysis")
        cm.width = 360
        cm.height = 140
        c.comment = cm
        ws.column_dimensions[get_column_letter(ci)].width = width

    sorted_reorder = sorted(reorder_items, key=lambda x: -x["suggested_buy"])
    r_idx = 5
    for i, it in enumerate(sorted_reorder, 1):
        ws.cell(row=r_idx, column=1, value=i).alignment = center
        ws.cell(row=r_idx, column=2, value=it["name"]).alignment = left
        ws.cell(row=r_idx, column=3, value=int(it["stock"])).alignment = right
        ws.cell(row=r_idx, column=4, value=int(it["sold_window"])).alignment = right
        ws.cell(row=r_idx, column=5, value=it["daily_rate"]).alignment = right
        buy = ws.cell(row=r_idx, column=6, value=it["suggested_buy"])
        buy.alignment = right
        buy.font = bold
        ws.cell(row=r_idx, column=7, value=it["last_sale"]).alignment = center
        fill = red_fill if it["stock"] <= 0 else amber_fill
        for ci in range(1, 8):
            ws.cell(row=r_idx, column=ci).fill = fill
            ws.cell(row=r_idx, column=ci).border = border
        r_idx += 1

    if sorted_reorder:
        total_buy = sum(it["suggested_buy"] for it in sorted_reorder)
        total_stock = sum(int(it["stock"]) for it in sorted_reorder)
        ws.cell(row=r_idx, column=2, value=f"JAMI ({len(sorted_reorder)} ta)").font = bold
        ws.cell(row=r_idx, column=2).alignment = right
        ws.cell(row=r_idx, column=3, value=total_stock).font = bold
        ws.cell(row=r_idx, column=6, value=total_buy).font = bold
        for ci in range(1, 8):
            ws.cell(row=r_idx, column=ci).border = border
            ws.cell(row=r_idx, column=ci).fill = head_fill
            if ws.cell(row=r_idx, column=ci).value is not None:
                ws.cell(row=r_idx, column=ci).font = Font(bold=True, color="FFFFFF")
    ws.freeze_panes = "A5"

    # ────────── Sheet 3: Hammasi (All products) ──────────
    ws_all = wb.create_sheet(f"Hammasi ({len(full_items)})")
    ws_all["A1"] = f"{supplier_label} — barcha aktiv mahsulotlar ({len(full_items)} ta)"
    ws_all["A1"].font = Font(bold=True, size=14)
    ws_all.merge_cells("A1:H1")

    all_cols = COL_DEFS + [("Lifecycle", "lifecycle", 10,
                             "Popularity classifier: active / aging / stale / never. Stale + zero sales = retirement candidate.",
                             "Классификатор: active / aging / stale / never.",
                             "Faollik: active / aging / stale / never.")]
    for ci, (label, key, width, en, ru, uz) in enumerate(all_cols, start=1):
        c = ws_all.cell(row=3, column=ci, value=label)
        c.font = head_font
        c.fill = head_fill
        c.alignment = center
        c.border = border
        cm = Comment(f"EN: {en}\n\nRU: {ru}\n\nUZ: {uz}", "Reorder Analysis")
        cm.width = 360
        cm.height = 140
        c.comment = cm
        ws_all.column_dimensions[get_column_letter(ci)].width = width

    sorted_all = sorted(full_items, key=lambda x: (-x["suggested_buy"], -x["sold_window"]))
    r_idx = 4
    for i, it in enumerate(sorted_all, 1):
        ws_all.cell(row=r_idx, column=1, value=i).alignment = center
        ws_all.cell(row=r_idx, column=2, value=it["name"]).alignment = left
        ws_all.cell(row=r_idx, column=3, value=int(it["stock"])).alignment = right
        ws_all.cell(row=r_idx, column=4, value=int(it["sold_window"])).alignment = right
        ws_all.cell(row=r_idx, column=5, value=it["daily_rate"]).alignment = right
        ws_all.cell(row=r_idx, column=6, value=it["suggested_buy"]).alignment = right
        ws_all.cell(row=r_idx, column=7, value=it["last_sale"]).alignment = center
        ws_all.cell(row=r_idx, column=8, value=it["lifecycle"]).alignment = center
        if it["stock"] <= 0 and it["suggested_buy"] > 0:
            fill = red_fill
        elif it["suggested_buy"] > 0:
            fill = amber_fill
        elif it["sold_window"] == 0 and it["stock"] == 0:
            fill = None
        else:
            fill = green_fill
        for ci in range(1, 9):
            if fill:
                ws_all.cell(row=r_idx, column=ci).fill = fill
            ws_all.cell(row=r_idx, column=ci).border = border
        r_idx += 1
    ws_all.freeze_panes = "A4"

    # ────────── Sheet 4: Yig'ma (Summary) ──────────
    ws_y = wb.create_sheet("Yig'ma (Summary)")
    ws_y["A1"] = f"{supplier_label} — Yig'ma / Summary"
    ws_y["A1"].font = Font(bold=True, size=14)
    ws_y.merge_cells("A1:C1")

    oos = sum(1 for it in full_items if it["stock"] <= 0)
    low = sum(1 for it in full_items if 0 < it["stock"] and it["suggested_buy"] > 0)
    ok = sum(1 for it in full_items if it["suggested_buy"] == 0 and it["sold_window"] > 0)
    no_demand = sum(1 for it in full_items if it["suggested_buy"] == 0 and it["sold_window"] == 0)
    total_buy = sum(it["suggested_buy"] for it in full_items)
    total_stock = sum(int(it["stock"]) for it in full_items)
    total_sold = sum(int(it["sold_window"]) for it in full_items)

    rows_data = [
        ("Jami mahsulotlar / Всего товаров / Total products", len(full_items)),
        ("Tugagan (stock=0) / Нет в наличии / Out of stock", oos),
        ("Kam qoldi (stock>0 + buyurtma>0) / Низкий запас", low),
        ("Yetarli / Достаточно / Sufficient", ok),
        ("Talab yo'q / Нет спроса / No demand", no_demand),
        ("", ""),
        (f"Jami qoldiq / Stock total (donalar)", total_stock),
        (f"Oxirgi {DEFAULT_WINDOW_DAYS}d sotuv / Sales window total", total_sold),
        ("Tavsiya qilingan jami buyurtma / Total suggested order", total_buy),
    ]
    r = 3
    for label, val in rows_data:
        if label:
            ws_y.cell(row=r, column=1, value=label).alignment = left
            ws_y.cell(row=r, column=2, value=val).alignment = right
            if isinstance(val, int) and val > 0:
                ws_y.cell(row=r, column=2).font = bold
        r += 1
    ws_y.column_dimensions["A"].width = 60
    ws_y.column_dimensions["B"].width = 14

    # Notes
    r += 1
    ws_y.cell(row=r, column=1, value="Method:").font = bold
    ws_y.cell(row=r, column=2,
              value=f"sold_window / {DEFAULT_WINDOW_DAYS} × {DEFAULT_BUFFER_DAYS} − stock (flat-rate, no seasonality)")
    ws_y.cell(row=r + 1, column=1, value="Generated:").font = bold
    ws_y.cell(row=r + 1, column=2, value=today)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Bot handlers ────────────────────────────────────────────────────────

@router.message(Command("zakazlar"))
async def cmd_zakazlar(message: Message):
    if not is_admin(message):
        return
    suppliers = list_suppliers_with_products()
    if not suppliers:
        await message.reply(
            "Hech qanday yetkazib beruvchi topilmadi.\n"
            "(Mahsulotlarda `latest_supplier_id` belgilanmagan — /supply yuklanishi kerak.)"
        )
        return
    total_oos = sum(s["oos_count"] for s in suppliers)
    total_prod = sum(s["product_count"] for s in suppliers)
    text = (
        f"📦 Yetkazib beruvchini tanlang:\n"
        f"({len(suppliers)} ta supplier · {total_prod} ta mahsulot · "
        f"{total_oos} ta tugagan)\n\n"
        f"Format: <b>NAME (tugagan/jami)</b>"
    )
    await message.reply(text, parse_mode="HTML",
                        reply_markup=_supplier_keyboard(suppliers))


@router.callback_query(F.data.startswith("zakaz:s:"))
async def cb_supplier_pick(cb: CallbackQuery):
    """Send (a) simple chat list + (b) detailed xlsx attachment."""
    await cb.answer("Hisoblanmoqda...")

    suffix = cb.data.removeprefix("zakaz:s:")
    supplier_id = None if suffix == "none" else int(suffix)

    if supplier_id is None:
        label = "(noma'lum supplier)"
    else:
        from backend.database import get_db
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT name_1c FROM suppliers WHERE id = ?", (supplier_id,)
            ).fetchone()
            label = row["name_1c"] if row else f"#{supplier_id}"
        finally:
            conn.close()

    full_items = list_supplier_full(supplier_id)
    reorder_items = sorted([x for x in full_items if x["suggested_buy"] > 0],
                            key=lambda x: -x["suggested_buy"])

    # Send the simple chat message (chunked for long lists)
    text = _format_simple_text(label, reorder_items)
    for chunk in _chunk_text(text):
        await cb.message.answer(chunk)

    # Send xlsx attachment
    xlsx_bytes = _build_xlsx(label, reorder_items, full_items)
    from datetime import date as _date
    safe_name = "".join(c if ord(c) < 128 else "_" for c in label).strip("_") or "supplier"
    fname = f"buyurtma_{safe_name}_{_date.today().isoformat()}.xlsx"
    caption = (f"📊 {label} — to'liq tahlil ({len(full_items)} mahsulot, "
               f"{len(reorder_items)} buyurtma kerak)")
    await cb.message.answer_document(
        BufferedInputFile(xlsx_bytes, filename=fname[:120]),
        caption=caption,
    )


@router.callback_query(F.data == "zakaz:close")
async def cb_close(cb: CallbackQuery):
    await cb.answer()
    try:
        await cb.message.delete()
    except Exception:
        pass
