"""Canonical phone-number normalizer — single source of truth (Error Log #86, audit M2).

Uzbek local numbers are matched on their last 9 digits (country/operator code
stripped). This is a LEAF module (only `re`), so any layer — routers, services,
the bot — can import it with no circular-import risk. That circular risk is the
reason several call sites historically re-implemented this (see the old comment
in import_client_master._normalize_phone); a leaf module removes the excuse.

Semantics: strip non-digits → last 9 digits; if fewer than 9, return what's
there (callers needing strict ≥9 validation check length themselves). `None` →
"". Equivalent to the former copies in routers/users, bot/shared, and
import_client_master_v2.

Two call sites intentionally do NOT use this and must stay separate:
  - `import_clients.normalize_phone` — multi-phone-cell aware (parse_phone_cell)
    for 1C's combined cells; this is the load-bearing identity path.
  - `import_client_master._normalize_phone` — wraps this but returns "" for
    sub-9-digit input (an empty phone is exempt from the active-phone UNIQUE
    index; a partial is not — that distinction is load-bearing).
"""
import re


def normalize_phone(raw) -> str:
    if raw is None:
        return ""
    digits = re.sub(r"\D", "", str(raw))
    return digits[-9:] if len(digits) >= 9 else digits
