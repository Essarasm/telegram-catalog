"""Phone-smart `fuzzy_client_1c_dups` audit check (Error Log #67).

Definitive principle (Alisher + Ulugbek 2026-06-01): client_id_1c is a 1C
*name label*, not a unique key. A same-name cluster is only a real duplicate
when ≥2 sibling rows SHARE a phone. Different-phone clusters are legitimate
("two shops next to each other") and must never be flagged.
"""
import os
import sqlite3

from backend.services.consistency_audit import run_audit


def _add(conn, cid, name, phone="", r02="", r03="", status="active"):
    conn.execute(
        "INSERT INTO allowed_clients "
        "(id, client_id_1c, phone_normalized, raqam_02, raqam_03, status) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (cid, name, phone, r02, r03, status),
    )


def _audit(conn):
    conn.commit()
    return run_audit().get("fuzzy_client_1c_dups")


def test_shared_phone_cluster_is_flagged(db):
    # Two rows, same name, share phone 111 (B's raqam_02 == A's primary).
    _add(db, 1, "ТЕСТ ШОП", phone="111")
    _add(db, 2, "ТЕСТ ШОП", phone="222", r02="111")
    res = _audit(db)
    assert res is not None and res["count"] == 1


def test_all_distinct_phones_not_flagged(db):
    # Same name, no shared phone → legitimate different shops → NOT flagged.
    _add(db, 1, "БОШҚА ШОП", phone="333")
    _add(db, 2, "БОШҚА ШОП", phone="444")
    assert _audit(db) is None


def test_confirmed_distinct_name_suppressed_even_with_shared_phone(db):
    # Registry override: a confirmed legitimate multi-shop name is never
    # flagged, even if a (coincidental) phone overlap would otherwise trip it.
    name = "АБДУЛЛО ЯНГИ-АРИК /ЯНГИ ЗАПЧ. БОЗОР/"  # in CONFIRMED_DISTINCT_SHARED_NAMES
    _add(db, 23, name, phone="555")
    _add(db, 24, name, phone="666", r02="555")
    assert _audit(db) is None


def test_merged_rows_excluded(db):
    # One active + one tombstone → not a live cluster.
    _add(db, 1, "ЭСКИ ШОП", phone="111")
    _add(db, 2, "ЭСКИ ШОП", phone="111", status="merged_into:1")
    assert _audit(db) is None


def test_cyrillic_case_and_whitespace_normalized(db):
    # Different case + double space collapse to one cluster; shared phone flags.
    # (Two active rows can't share phone_normalized — UNIQUE index — so the
    # shared digit lives in the second row's raqam_02, as it does in prod.)
    _add(db, 1, "Абдулло  Тест", phone="111")
    _add(db, 2, "абдулло тест", phone="222", r02="111")
    res = _audit(db)
    assert res is not None and res["count"] == 1


def test_true_count_not_capped_at_20(db):
    # 22 distinct shared-phone clusters → count must be 22, not the old
    # LIMIT-20 cap (Error Log #56 reporting bug).
    for i in range(22):
        _add(db, 100 + i * 2, f"КЛАСТЕР {i}", phone=f"p{i}")
        _add(db, 101 + i * 2, f"КЛАСТЕР {i}", phone=f"q{i}", r02=f"p{i}")
    res = _audit(db)
    assert res is not None and res["count"] == 22
