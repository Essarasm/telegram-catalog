"""Chat-context-aware /help + onboarding announcement text.

Each of the 5 known chat roles gets its own command list. The same spec is
reused for /announce (post the objective + command list into each group
once) and /help (filtered response per chat).
"""
from html import escape as _h
from typing import NamedTuple, Optional


class Cmd(NamedTuple):
    syntax: str         # e.g. "/prices (reply Excel)"
    purpose: str        # one-line explanation


# ── Group objectives ────────────────────────────────────────────────

OBJECTIVES = {
    'daily': (
        "🗓️ <b>Daily — Kunlik yuklashlar</b>\n\n"
        "Maqsad: har kuni 10 ta 1C ma'lumot yuklovi. /today ro'yxati to'liq "
        "✅ bo'lishi kerak. 10:00 va 17:00 Toshkent vaqtida avtomatik "
        "eslatmalar yuboriladi."
    ),
    'admin': (
        "🔧 <b>Admin — Ichki boshqaruv</b>\n\n"
        "Maqsad: mijozlar ro'yxati, kredit skor, diagnostika va bir martalik "
        "backfill vazifalari. Barcha ma'lumotlar ustidan to'liq nazorat."
    ),
    'sales': (
        "💰 <b>Sales — Savdo va buyurtmalar</b>\n\n"
        "Maqsad: kiruvchi buyurtmalarni ko'rish va kunlik yuklash holatini "
        "tezkor tekshirish. Avtomatik ravishda har yangi buyurtma shu yerga "
        "yuboriladi (Excel fayl bilan)."
    ),
    'inventory': (
        "📦 <b>Inventory — Ombor va stok</b>\n\n"
        "Maqsad: stokdagi kamomadlar va mijoz qiziqishi signallarini "
        "monitoring qilish. Har kuni 08:00 Toshkent vaqtida "
        "/stockalert avtomatik postlanadi."
    ),
    'cashier': (
        "💼 <b>Kassa</b>\n\n"
        "Maqsad: klientlardan to'lovlarni qabul qilish va agentlardan "
        "kelayotgan to'lovlarni tasdiqlash. /qabul — yangi sessiya. "
        "Qog'oz daftar — zaxira nusxa (saqlanadi)."
    ),
}

# ── Command lists per chat role ─────────────────────────────────────
# Each role maps to a list of (section_title, [Cmd, ...]) tuples.

SPECS = {
    'daily': [
        ("📋 Kunlik holat", [
            Cmd("/today", "Bugungi 10 ta vazifa — ✅/🟡/⏭/❌ belgilar bilan"),
            Cmd("/missing [N]", "Oxirgi N kun bo'yicha kamomadlar (default 7)"),
        ]),
        ("📥 10 ta yuklash buyrug'i", [
            Cmd("/prices (reply Excel)", "Narxlar (1C price list)"),
            Cmd("/stock (reply Excel)", "Прайс лист — to'liq snapshot (faylda yo'q = tugagan)"),
            Cmd("/catalog (reply Excel)", "Katalog yangi/o'chirilgan mahsulotlar"),
            Cmd("/balances (reply XLS)", "Оборотка 40.10 (UZS) + 40.11 (USD) — bitta fayl"),
            Cmd("/debtors (reply XLS)", "Дебиторка (дебиторская задолженность)"),
            Cmd("/realorders (reply XLS)", "Реализация товаров — haqiqiy buyurtmalar"),
            Cmd("/cash (reply XLS)", "Касса — tushgan to'lovlar"),
            Cmd("/fxrate 12650", "Bugungi USD/UZS kursi (raqam)"),
            Cmd("/supply (reply XLS)", "Kirim/Qaytarish — Поступление/Возврат"),
            Cmd("/clients (reply Excel)", "Mijozlar ro'yxati — Справочник Контрагенты"),
        ]),
        ("🛠 Yordamchi", [
            Cmd("/bulksupply", "Ko'p oylik supply fayllarni bir martada import"),
            Cmd("/syncimages", "Product rasmlarni sinxronlash (/Pictures)"),
            Cmd("/skipupload <type>", "Vazifani ⏭ (skipped) deb belgilash"),
            Cmd("/chatid", "Chat va user ID — diagnostika"),
        ]),
    ],
    'admin': [
        ("👤 Mijoz boshqaruvi", [
            Cmd("/add <phone> <name> <location>", "Yangi mijozni qo'lda qo'shish"),
            Cmd("/approve <telegram_id>", "Foydalanuvchini tasdiqlash"),
            Cmd("/link <telegram_id> <1C_name|phone>", "Foydalanuvchini mavjud 1C mijozga bog'lash"),
            Cmd("/list", "Tasdiqlanmaganlar ro'yxati"),
            Cmd("/unlinked", "1C ga ulanmagan foydalanuvchilar"),
            Cmd("/makeagent <user_id>", "Savdo agenti qilish"),
            Cmd("/testclient [имя или #ID]", "Test: o'zingizni mijoz sifatida bog'lash"),
            Cmd("/duplicateclients [qidiruv]", "Takrorlanuvchi mijozlar auditi"),
            Cmd("/clientmaster (reply XLSX)", "Client Master importi (1C cyrillic + phone)"),
        ]),
        ("🏅 Kredit skor va ballar", [
            Cmd("/clientscore <name|#id>", "Mijoz skori (to'liq breakdown)"),
            Cmd("/scorestats", "Tier/bucket taqsimot"),
            Cmd("/runscore", "Barcha mijozlarni qayta skorlash"),
            Cmd("/adjustscore", "Qo'lda tuzatish (30 kun amal qiladi)"),
            Cmd("/scoreanomalies", "Shubhali hollar (stale ma'lumot)"),
            Cmd("/payments <name>", "Mijoz to'lovlari tarixi"),
            Cmd("/simpoints, /calcpoints, /clientpoints, /leaderboard", "Points tizimi (simulyatsiya)"),
        ]),
        ("📊 Real orders tahlil", [
            Cmd("/realordersstats", "Match rates + agent taqsimot + wish-list gap"),
            Cmd("/realordersample <name>", "DB dump: bitta buyurtmaning xom narx ustunlari"),
            Cmd("/unmatchedclients", "Bog'lanmagan mijozlar ro'yxati (ko'p hujjat)"),
            Cmd("/unmatchedproducts", "Bog'lanmagan mahsulotlar"),
            Cmd("/relinkrealorders", "Qayta bog'lash — allowed_clients yangilagandan keyin"),
            Cmd("/ingestskus", "Bog'lanmagan mahsulotlarni katalogga qo'shish"),
        ]),
        ("🔔 To'lov bildirishnomalari", [
            Cmd("/missed", "Mijozga yetkazilmagan to'lov bildirishnomalari (Session N)"),
        ]),
        ("📈 Hisobotlar", [
            Cmd("/reports", "So'nggi xatolik xabarlari va mahsulot so'rovlari"),
            Cmd("/wrongphotos", "Noto'g'ri rasm xabarlari"),
            Cmd("/searches [kunlar]", "Qidiruv statistikasi (default 7)"),
            Cmd("/datacoverage [valyuta]", "Yuklangan ma'lumotlar qamrovi"),
            Cmd("/demand [kunlar]", "Tugagan mahsulotlarga talab (default 30)"),
            Cmd("/lastorders [N]", "Oxirgi N buyurtma"),
        ]),
        ("🛠 Bir martalik / backfill", [
            Cmd("/backfilldailyuploads", "daily_uploads'ga eski aktivlikdan retro qatorlar"),
            Cmd("/backfillordernames", "Eski wish-list nomlarini 1C cyrillicga"),
            Cmd("/backfillrealordertotals", "RO'larda yo'qolgan jami narxlarni qayta hisoblash"),
            Cmd("/rebuildsearch", "products.search_text indeksini qayta qurish"),
            Cmd("/addmissing", "Mahsulot so'rovlarini ko'rish"),
            Cmd("/aliases, /seedaliases", "Ishlab chiqaruvchi aliaslari"),
            Cmd("/holiday <YYYY-MM-DD>", "Sanani bayram deb belgilash (checklist o'tkazib yuborish)"),
        ]),
        ("⚠️ Ehtiyotkorlik", [
            Cmd("/wipewishlists CONFIRM", "Demo wish-list ma'lumotlarini tozalash (bir martalik)"),
            Cmd("/cashbook", "Cashbook intake — oxirgi yozuvlar + bekor qilish (Session Z)"),
        ]),
        ("🔧 Diagnostika", [
            Cmd("/chatid", "Chat va user ID"),
        ]),
    ],
    'sales': [
        ("💰 Savdo buyruqlari", [
            Cmd("/lastorders [N]", "Oxirgi N buyurtma (default 10)"),
            Cmd("/today", "Kunlik yuklash holati (read-only)"),
            Cmd("/chatid", "Chat va user ID"),
        ]),
    ],
    'inventory': [
        ("📦 Inventar buyruqlari", [
            Cmd("/today", "Yuklash holati (read-only)"),
            Cmd("/missing", "Oxirgi kun(lar) kamomadlari"),
            Cmd("/stockalert", "Stok alert (yig'ma — top 25 tugagan + top 30 kam qoldi)"),
            Cmd("/stockalert tugagan", "Faqat TUGAGAN mahsulotlar (to'liq ro'yxat)"),
            Cmd("/stockalert kam", "Faqat KAM QOLDI (to'liq ro'yxat)"),
            Cmd("/stockalert full", "Ikkalasi ham, to'liq"),
            Cmd("/cleanupinactive [N]", f"Faol bo'lmagan mahsulotlarni preview (def 60d)"),
            Cmd("/cleanupinactive [N] confirm", "Qo'llash — is_active=0 ga o'tkazadi"),
            Cmd("/demand [kunlar]", "Tugagan mahsulotlarga mijoz talabi"),
            Cmd("/stock (reply Excel)", "Qoldiqni yangilash"),
            Cmd("/chatid", "Chat va user ID"),
        ]),
    ],
    'cashier': [
        ("💼 Kassa amallari", [
            Cmd("/qabul", "Yangi to'lov qabul qilish — Klientdan yoki Agentdan"),
            Cmd("/bugun", "Bugungi qabul qilingan to'lovlar — yig'ma jadval"),
            Cmd("/bekor", "Joriy sessiyani bekor qilish"),
            Cmd("/chatid", "Chat va user ID"),
        ]),
    ],
    'dm_user': [
        # Non-admin private chat — no commands, just a hint to use Mini App
    ],
}


def render_help_for_context(ctx: str) -> str:
    """Return the /help body for a given chat context."""
    if ctx == 'dm_user':
        return (
            "👋 <b>Salom!</b>\n\n"
            "Buyurtma berish va katalogni ko'rish uchun Mini App'dan foydalaning — "
            "menyudagi <b>Open App</b> tugmasini bosing.\n\n"
            "Yordam kerakmi? Savdo jamoasiga yozish uchun "
            "<code>/start support</code> buyrug'ini yuboring."
        )
    if ctx == 'unknown':
        return "ℹ️ Bu chatda buyruqlar mavjud emas."

    spec = SPECS.get(ctx) or SPECS.get('admin')  # dm_admin → admin
    objective = OBJECTIVES.get(ctx) or OBJECTIVES.get('admin')

    parts = [objective, ""]
    for section_title, cmds in spec:
        parts.append(f"<b>{_h(section_title)}</b>")
        for c in cmds:
            # Escape < and > inside syntax/purpose — placeholders like <phone>
            # look like HTML tags to Telegram's parser otherwise.
            parts.append(f"  <code>{_h(c.syntax)}</code> — {_h(c.purpose)}")
        parts.append("")
    return "\n".join(parts).rstrip()


def render_onboarding_for_group(ctx: str) -> str:
    """Objective + commands — same content as /help, shorter header for /announce.
    Admin DM is treated identical to admin group."""
    if ctx == 'dm_admin':
        ctx = 'admin'
    return render_help_for_context(ctx)


# ── Chat-id → role lookup for /announce ─────────────────────────────
def role_for_chat_id(chat_id: int) -> Optional[str]:
    from bot.shared import (
        DAILY_GROUP_CHAT_ID, ADMIN_GROUP_CHAT_ID,
        ORDER_GROUP_CHAT_ID, INVENTORY_GROUP_CHAT_ID,
    )
    return {
        DAILY_GROUP_CHAT_ID: 'daily',
        ADMIN_GROUP_CHAT_ID: 'admin',
        ORDER_GROUP_CHAT_ID: 'sales',
        INVENTORY_GROUP_CHAT_ID: 'inventory',
    }.get(chat_id)
