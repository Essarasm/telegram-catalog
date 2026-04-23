"""Testclient handlers — client search, link, and add-and-link.

Extracted from bot/main.py monolith. Uses aiogram Router so a bug
here cannot crash upload or order handlers.
"""
import unicodedata
from collections import OrderedDict

from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import (
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
)

from bot.shared import (
    get_db, html_escape, is_agent_or_admin, is_agent_or_admin_cb,
    TESTCLIENT_PROMPT, WEBAPP_URL, logger,
)


def _log_agent_switch(conn, agent_tg_id: int, client_id: int) -> None:
    """Record an agent → client switch for the mini app's recent list."""
    try:
        conn.execute(
            "INSERT INTO agent_client_switches (agent_telegram_id, client_id) "
            "VALUES (?, ?)",
            (agent_tg_id, client_id),
        )
    except Exception as e:
        logger.warning(f"agent_client_switches insert failed: {e}")

router = Router()

@router.callback_query(F.data.startswith("tc:"))
async def on_testclient_callback(cb: types.CallbackQuery):
    """Inline-button replacement for typing /testclient #ID or addclient."""
    if not is_agent_or_admin_cb(cb):
        await cb.answer("Ruxsat yo'q", show_alert=False)
        return
    data = cb.data or ""
    parts = data.split(":", 2)
    if len(parts) < 2:
        await cb.answer("Noma'lum amal", show_alert=False)
        return
    action = parts[1]
    telegram_id = cb.from_user.id if cb.from_user else 0
    if not telegram_id:
        await cb.answer("Foydalanuvchi aniqlanmadi", show_alert=False)
        return
    conn = get_db()
    try:
        # Ensure this user has a users row
        conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id, first_name, is_approved) "
            "VALUES (?, ?, 1)",
            (telegram_id, (cb.from_user.first_name or "Admin")),
        )

        if action == "link" and len(parts) == 3 and parts[2].isdigit():
            target_id = int(parts[2])
            target = conn.execute(
                "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
                (target_id,),
            ).fetchone()
            if not target:
                await cb.answer("Mijoz topilmadi", show_alert=True)
                return
            conn.execute(
                "UPDATE users SET client_id = ? WHERE telegram_id = ?",
                (target_id, telegram_id),
            )
            _log_agent_switch(conn, telegram_id, target_id)
            conn.commit()
            client_1c = target["client_id_1c"] or target["name"] or f"#{target_id}"
            name_html = html_escape(client_1c)
            agent_first = cb.from_user.first_name or ""
            agent_last = cb.from_user.last_name or ""
            agent_display = " ".join(filter(None, [agent_first, agent_last])) or str(telegram_id)
            agent_username = f"@{cb.from_user.username}" if cb.from_user.username else ""

            # Look up client's phone from allowed_clients
            ph_row = conn.execute(
                "SELECT phone_normalized FROM allowed_clients WHERE id = ? AND phone_normalized != '' LIMIT 1",
                (target_id,),
            ).fetchone()
            client_phone = ph_row["phone_normalized"] if ph_row else ""

            await cb.answer(f"Bog'landi: {client_1c}"[:200])
            try:
                lines = [
                    f"✅ <b>Bog'landi</b>",
                    "",
                    f"🧾 Mijoz (1C): <b>{name_html}</b>",
                ]
                if client_phone:
                    lines.append(f"📞 Telefon: {client_phone}")
                lines.append(f"🆔 ID: #{target_id}")
                lines.append("")
                lines.append(f"👤 Agent: <b>{html_escape(agent_display)}</b>")
                if agent_username:
                    lines.append(f"   {agent_username}")
                lines.append(f"🆔 Agent TG: <code>{telegram_id}</code>")
                lines.append("")
                lines.append("Kabinetni oching — mijoz ma'lumotlarini ko'ring.")
                await cb.message.reply("\n".join(lines), parse_mode="HTML")
            except Exception:
                pass
            return

        if action == "add" and len(parts) == 3:
            client_name_prefix = parts[2]
            # callback_data is truncated to fit 64-byte limit; match by
            # prefix (LIKE) first, fall back to exact match, then to
            # client_balances name lookup.
            existing = conn.execute(
                "SELECT id, client_id_1c FROM allowed_clients WHERE client_id_1c LIKE ? LIMIT 1",
                (client_name_prefix + "%",),
            ).fetchone()
            # Resolve the full name from allowed_clients or client_balances
            if existing:
                client_name_1c = existing["client_id_1c"]
            else:
                cb_row = conn.execute(
                    "SELECT client_name_1c FROM client_balances WHERE client_name_1c LIKE ? LIMIT 1",
                    (client_name_prefix + "%",),
                ).fetchone()
                client_name_1c = cb_row[0] if cb_row else client_name_prefix
            if existing:
                new_id = existing["id"]
            else:
                conn.execute(
                    "INSERT INTO allowed_clients (phone_normalized, name, "
                    "client_id_1c, source_sheet, status) VALUES (?, ?, ?, ?, ?)",
                    ("", client_name_1c, client_name_1c, "bot_from_1c", "active"),
                )
                new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # Link every financial table that keys on client_name_1c so the
            # Cabinet (which queries by client_id) has data to show. Without
            # this, the cabinet is empty because only client_balances was
            # being linked before.
            linked_counts = {}
            for table in ("client_balances", "real_orders",
                          "client_payments", "client_debts"):
                cur = conn.execute(
                    f"UPDATE {table} SET client_id = ? "
                    f"WHERE client_name_1c = ? AND client_id IS NULL",
                    (new_id, client_name_1c),
                )
                linked_counts[table] = cur.rowcount

            conn.execute(
                "UPDATE users SET client_id = ? WHERE telegram_id = ?",
                (new_id, telegram_id),
            )
            _log_agent_switch(conn, telegram_id, new_id)
            conn.commit()
            agent_first = cb.from_user.first_name or ""
            agent_last = cb.from_user.last_name or ""
            agent_display = " ".join(filter(None, [agent_first, agent_last])) or str(telegram_id)
            agent_username = f"@{cb.from_user.username}" if cb.from_user.username else ""

            await cb.answer(f"Ro'yxatga qo'shildi va bog'landi", show_alert=False)
            try:
                linked_summary = " · ".join(
                    f"{k.replace('client_', '').replace('_', '.')}={v}"
                    for k, v in linked_counts.items() if v
                ) or "yo'q"
                lines = [
                    f"✅ <b>Ro'yxatga qo'shildi va bog'landi</b>",
                    "",
                    f"🧾 Mijoz (1C): <b>{html_escape(client_name_1c)}</b>",
                    f"🆔 ID: #{new_id}",
                    f"🔗 Bog'langan jadvallar: {linked_summary}",
                    "",
                    f"👤 Agent: <b>{html_escape(agent_display)}</b>",
                ]
                if agent_username:
                    lines.append(f"   {agent_username}")
                lines.append(f"🆔 Agent TG: <code>{telegram_id}</code>")
                await cb.message.reply("\n".join(lines), parse_mode="HTML")
            except Exception:
                pass
            return

        # Unknown action (defensive; no UI path currently reaches this)
        await cb.answer("Noma'lum tugma", show_alert=False)
    finally:
        conn.close()


@router.message(Command("testclient"))
async def cmd_testclient(message: types.Message, _override_arg: str | None = None):
    """Link admin's account to a 1C client for testing the Cabinet balance view.

    Usage:
        /testclient              — prompt user for a name (force-reply)
        /testclient КЛИЕНТ       — search by name and link to first match
        /testclient #123         — link to allowed_clients.id directly
        /testclient clear        — remove the test link
    """
    if not is_agent_or_admin(message):
        return

    telegram_id = message.from_user.id
    conn = get_db()
    if _override_arg is not None:
        arg = _override_arg.strip()
    else:
        parts = message.text.split(maxsplit=1)
        arg = parts[1].strip() if len(parts) > 1 else ""

    # Ensure this admin has a users record
    user = conn.execute("SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    if not user:
        conn.execute(
            "INSERT INTO users (telegram_id, first_name, is_approved) VALUES (?, ?, 1)",
            (telegram_id, message.from_user.first_name or "Admin"),
        )
        conn.commit()
        user = conn.execute("SELECT client_id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()

    # /testclient clear — remove link
    if arg.lower() == 'clear':
        conn.execute("UPDATE users SET client_id = NULL WHERE telegram_id = ?", (telegram_id,))
        conn.commit()
        conn.close()
        await message.reply("✅ Test link removed. Cabinet will show no balance data.")
        return

    # /testclient #ID — link by allowed_clients.id
    if arg.startswith('#') and arg[1:].isdigit():
        target_id = int(arg[1:])
        target = conn.execute(
            "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?", (target_id,)
        ).fetchone()
        if not target:
            conn.close()
            await message.reply(f"❌ allowed_clients ID {target_id} not found.")
            return
        conn.execute("UPDATE users SET client_id = ? WHERE telegram_id = ?", (target_id, telegram_id))
        _log_agent_switch(conn, telegram_id, target_id)
        conn.commit()
        # Check if this client has balance data
        bal_count = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_id = ?", (target_id,)
        ).fetchone()[0]
        conn.close()
        await message.reply(
            f"✅ Linked to: <b>{html_escape(target['name'] or '—')}</b>\n"
            f"1C: <code>{html_escape(target['client_id_1c'] or '—')}</code>\n"
            f"Balance records: {bal_count}\n\n"
            f"Open 🏛️ Cabinet to see their data.",
            parse_mode="HTML",
        )
        return

    # /testclient addclient NAME — auto-create allowed_clients record from client_balances
    if arg.lower() == 'addclient' and len(parts) > 1:
        rest = message.text.split(maxsplit=2)
        if len(rest) < 3 or not rest[2].strip():
            conn.close()
            await message.reply(
                "❌ <b>Foydalanish:</b>\n"
                "<code>/testclient addclient 1C nomi</code>",
                parse_mode="HTML",
            )
            return
        client_name_1c = rest[2].strip()
        # Verify this name exists in client_balances
        cb_exists = conn.execute(
            "SELECT COUNT(*) FROM client_balances WHERE client_name_1c = ?",
            (client_name_1c,),
        ).fetchone()[0]
        if not cb_exists:
            conn.close()
            await message.reply(f"❌ 1C'da <b>{html_escape(client_name_1c)}</b> topilmadi.", parse_mode="HTML")
            return
        # Check if already in allowed_clients
        existing = conn.execute(
            "SELECT id FROM allowed_clients WHERE client_id_1c = ? LIMIT 1",
            (client_name_1c,),
        ).fetchone()
        if existing:
            conn.close()
            await message.reply(
                f"ℹ️ Allaqachon ro'yxatda: #{existing['id']}\n"
                f"<code>/testclient #{existing['id']}</code>",
                parse_mode="HTML",
            )
            return
        # Create allowed_clients record with client_id_1c set
        conn.execute(
            "INSERT INTO allowed_clients (phone_normalized, name, client_id_1c, source_sheet, status) "
            "VALUES (?, ?, ?, ?, ?)",
            ("", client_name_1c, client_name_1c, "bot_from_1c", "active"),
        )
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        # Link client_balances records to this new allowed_clients.id
        conn.execute(
            "UPDATE client_balances SET client_id = ? WHERE client_name_1c = ? AND client_id IS NULL",
            (new_id, client_name_1c),
        )
        conn.commit()
        conn.close()
        await message.reply(
            f"✅ Mijoz qo'shildi!\n\n"
            f"🏢 1C nomi: <b>{html_escape(client_name_1c)}</b>\n"
            f"🆔 ID: #{new_id}\n\n"
            f"Bog'lash: <code>/testclient #{new_id}</code>",
            parse_mode="HTML",
        )
        return

    # /testclient link TELEGRAM_ID CLIENT_ID — link any user to a client
    if arg.lower() == 'link' and len(parts) > 1:
        rest = message.text.split(maxsplit=2)
        link_args = rest[2].strip().split() if len(rest) > 2 else []
        if len(link_args) == 2 and link_args[0].isdigit() and link_args[1].isdigit():
            tg_id = int(link_args[0])
            client_id = int(link_args[1])
            client = conn.execute(
                "SELECT id, name, client_id_1c FROM allowed_clients WHERE id = ?",
                (client_id,),
            ).fetchone()
            if not client:
                conn.close()
                await message.reply(f"❌ Mijoz #{client_id} topilmadi.")
                return
            conn.execute(
                "UPDATE users SET client_id = ?, is_approved = 1 WHERE telegram_id = ?",
                (client_id, tg_id),
            )
            conn.execute(
                "UPDATE allowed_clients SET matched_telegram_id = ? WHERE id = ?",
                (tg_id, client_id),
            )
            conn.commit()
            # Persist to backup
            try:
                from backend.services.backup_users import save_user_to_backup
                row = conn.execute(
                    "SELECT telegram_id, phone, first_name, last_name, username, "
                    "latitude, longitude, is_approved, client_id FROM users WHERE telegram_id = ?",
                    (tg_id,),
                ).fetchone()
                if row:
                    save_user_to_backup(dict(row))
            except Exception as e:
                logging.warning(f"backup after /testclient link failed: {e}")
            conn.close()
            name = html_escape(client["client_id_1c"] or client["name"] or f"#{client_id}")
            await message.reply(
                f"✅ Bog'landi!\n"
                f"👤 Telegram ID: <code>{tg_id}</code>\n"
                f"🏢 Mijoz: {name} (#{client_id})",
                parse_mode="HTML",
            )
            return
        else:
            conn.close()
            await message.reply(
                "❌ <b>Foydalanish:</b>\n"
                "<code>/testclient link TELEGRAM_ID CLIENT_ID</code>",
                parse_mode="HTML",
            )
            return

    # /testclient NAME — search by name (Cyrillic or Latin), show tappable buttons
    if arg:
        import unicodedata
        normalized = unicodedata.normalize("NFC", arg).strip().lower()
        search = f"%{normalized}%"
        # Case-insensitive search using our custom Unicode LOWER function.
        # Both sides (DB value + query) are NFC-normalized + lowercased.
        matches = conn.execute(
            """SELECT ac.id, ac.name, ac.client_id_1c, ac.phone_normalized,
                      (SELECT COUNT(*) FROM client_balances WHERE client_id = ac.id) as bal_count
               FROM allowed_clients ac
               WHERE (LOWER(ac.client_id_1c) LIKE ? OR LOWER(ac.name) LIKE ?
                  OR ac.id IN (
                      SELECT DISTINCT client_id FROM client_balances
                      WHERE LOWER(client_name_1c) LIKE ? AND client_id IS NOT NULL
                  ))
                 AND COALESCE(ac.status, 'active') != 'merged'
                 AND ac.client_id_1c IS NOT NULL AND ac.client_id_1c != ''
               ORDER BY bal_count DESC
               LIMIT 15""",
            (search, search, search),
        ).fetchall()

        # Always fetch up to 5 🟡 (not-yet-whitelisted) 1C-only matches so
        # new clients aren't buried when 15 whitelisted hits fill the list.
        cb_only = conn.execute(
            """SELECT DISTINCT cb.client_name_1c,
                      COUNT(*) as bal_count,
                      MAX(cb.period_end) as latest_period
               FROM client_balances cb
               WHERE LOWER(cb.client_name_1c) LIKE ?
                 AND (cb.client_id IS NULL
                      OR cb.client_id NOT IN (SELECT id FROM allowed_clients))
               GROUP BY cb.client_name_1c
               LIMIT 5""",
            (search,),
        ).fetchall()

        if not matches and not cb_only:
            conn.close()
            await message.reply(
                f"❌ '{html_escape(arg)}' bo'yicha hech narsa topilmadi.\n\n"
                f"💡 1C dagi nom bilan qidiring, masalan:\n"
                f"<code>/testclient Улугбек</code>",
                parse_mode="HTML",
            )
            return

        # Build a caption + an inline keyboard so each match is a single-tap
        # link button. Buttons look like blue actionable links in Telegram.
        header = f"🔍 '{html_escape(arg)}' — {len(matches) + len(cb_only)} ta natija"
        if cb_only:
            header += (
                "\n\n🟡 = 1C da bor, ilova ro'yxatiga qo'shilmagan "
                "(tugmani bossangiz avtomatik qo'shiladi va bog'lanasiz)."
            )
        conn.close()

        kb_rows: list[list[InlineKeyboardButton]] = []

        # 🟡 entries first — they're rare, actionable, and easy to miss
        # when buried below 15 existing whitelisted matches.
        for c in cb_only:
            cname = (c['client_name_1c'] or "").strip()
            kb_rows.append([InlineKeyboardButton(
                text=f"🟡 {cname}"[:64],
                callback_data=f"tc:add:{cname[:25]}",
            )])

        # Group whitelisted matches by client_id_1c so multi-phone siblings roll up
        from collections import OrderedDict
        _grouped = OrderedDict()
        for m in matches:
            cid = (m['client_id_1c'] or '').strip()
            key = cid if cid else f"__no1c_{m['id']}"
            _grouped.setdefault(key, []).append(m)

        # One button per unique 1C client. Siblings (multi-phone) all lead
        # to the same Cabinet via the server-side sibling resolver, so
        # showing each phone separately is just clutter. We link to the
        # first sibling row as the canonical whitelist anchor.
        for key, group in _grouped.items():
            first = group[0]
            name = (first['client_id_1c'] or first['name'] or f"#{first['id']}").strip()
            kb_rows.append([InlineKeyboardButton(
                text=name[:64],
                callback_data=f"tc:link:{first['id']}",
            )])

        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
        await message.reply(header, parse_mode="HTML", reply_markup=kb)
        return

    # /testclient (no args) — force-reply prompt. User's next message (a
    # plain name) is picked up by handle_testclient_reply below and routed
    # through the normal search path. Keeps agents one tap away from the
    # search without needing to remember the /testclient name syntax.
    conn.close()
    await message.reply(
        TESTCLIENT_PROMPT,
        reply_markup=ForceReply(
            selective=True,
            input_field_placeholder="Masalan: Улугбек",
        ),
    )




@router.message(Command("panel"))
async def cmd_panel(message: types.Message):
    """Open the agent panel mini app.

    Preferred entry point for agents — replaces scattered /testclient,
    /fxrate-read, and phone-lookup flows. The mini app handles client
    search, FX rate display, and acting-as in one UI.
    """
    if not is_agent_or_admin(message):
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="🧭 Agent panelini ochish",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        ]]
    )
    await message.reply(
        "Panel:\n"
        "• Bugungi valyuta kursi\n"
        "• Mijoz qidirish va kabinetga kirish\n"
        "• Oxirgi mijozlar ro'yxati",
        reply_markup=kb,
    )


@router.message(F.reply_to_message & F.text)
async def handle_testclient_reply(message: types.Message):
    """When a user replies to the /testclient force-reply prompt, treat
    the reply's text as the search query and dispatch to cmd_testclient."""
    if not is_agent_or_admin(message):
        return
    rt = message.reply_to_message
    if not rt or not rt.from_user or not rt.from_user.is_bot:
        return
    # Match on the exact prompt we sent so we don't eat other reply flows.
    if (rt.text or "").strip() != TESTCLIENT_PROMPT:
        return
    query = (message.text or "").strip()
    if not query:
        return
    await cmd_testclient(message, _override_arg=query)

