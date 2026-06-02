"""Regression matrix for the merge tool's phone-slot planner (Error Log #77).

`plan_phone_slots` computes a canonical row's FINAL raqam_02/raqam_03 state
after a duplicate-cluster merge. The bugs it replaces:
  - older planner only filled *empty* slots, so a stale `raqam_02 == primary`
    duplicate blocked promotion of a genuinely distinct second number forever
    (the `phone_moves_count: 0` symptom on Жамшед Сифат Гагарин);
  - a number equal to the primary could survive in a secondary slot.

These tests pin the contract: primary is never duplicated into a slot, the
first two distinct secondaries land in raqam_02/raqam_03, excess overflows, and
slots with no assigned number are absent from `assignments` (caller clears them).
"""
import importlib.util
import os

_PATH = os.path.join(os.path.dirname(__file__), "..", "tools",
                     "merge_duplicate_1c_clients.py")
_spec = importlib.util.spec_from_file_location("merge_dup_1c", _PATH)
merge_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(merge_mod)
plan_phone_slots = merge_mod.plan_phone_slots


def _row(phone=None, raqam_02=None, ism_02=None, raqam_03=None, ism_03=None,
         name=None):
    """A dict mimicking an allowed_clients row (function uses __getitem__)."""
    return {"phone_normalized": phone, "raqam_02": raqam_02, "ism_02": ism_02,
            "raqam_03": raqam_03, "ism_03": ism_03, "name": name}


def test_no_secondaries_anywhere_clears_both_slots():
    canon = _row(phone="111")
    assignments, overflow, dropped = plan_phone_slots(canon, [])
    assert assignments == {}            # caller clears raqam_02 + raqam_03
    assert overflow is False
    assert dropped == []


def test_canonical_raqam_02_equals_primary_is_dropped():
    # The exact #77 dup shape: raqam_02 is a useless copy of the primary.
    canon = _row(phone="915294400", raqam_02="915294400", ism_02="Jamshed")
    assignments, overflow, dropped = plan_phone_slots(canon, [])
    assert assignments == {}            # dup removed, slot reclaimable
    assert overflow is False


def test_distinct_secondary_on_tombstone_is_promoted():
    # Жамшед: canonical primary, second number 940474747 parked on a merged row.
    canon = _row(phone="915294400", raqam_02="915294400", ism_02="Jamshed")
    non_canon = [_row(phone="915294400", raqam_02="940474747", ism_02="ШАХБОЗ")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    assert assignments == {"raqam_02": ("940474747", "ШАХБОЗ")}
    assert overflow is False
    assert dropped == []


def test_two_distinct_secondaries_fill_both_slots_in_order():
    canon = _row(phone="111")
    non_canon = [_row(phone="222", name="A"), _row(phone="333", name="B")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    assert assignments == {"raqam_02": ("222", "A"), "raqam_03": ("333", "B")}
    assert overflow is False


def test_overflow_keeps_first_two_drops_rest_and_flags():
    canon = _row(phone="111")
    non_canon = [_row(phone="222", name="A"), _row(phone="333", name="B"),
                 _row(phone="444", name="C")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    assert assignments == {"raqam_02": ("222", "A"), "raqam_03": ("333", "B")}
    assert overflow is True
    assert dropped == ["444"]


def test_canonical_genuine_secondary_preserved_and_ordered_first():
    canon = _row(phone="111", raqam_02="222", ism_02="own")
    non_canon = [_row(phone="333", name="other")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    # Canonical's own secondary keeps raqam_02; non-canon's distinct fills raqam_03.
    assert assignments == {"raqam_02": ("222", "own"),
                           "raqam_03": ("333", "other")}


def test_whitespace_variants_dedupe():
    canon = _row(phone=" 111 ")
    non_canon = [_row(phone="111"), _row(raqam_02=" 222 ", ism_02="x")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    # " 111 " == "111" (primary) → excluded; " 222 " normalized into a slot.
    assert assignments == {"raqam_02": ("222", "x")}
    assert overflow is False


def test_non_canon_secondary_equal_to_primary_skipped():
    canon = _row(phone="111")
    non_canon = [_row(phone="111", raqam_02="111", ism_02="dup")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    assert assignments == {}
    assert overflow is False


def test_empty_string_phones_ignored():
    canon = _row(phone="111", raqam_02="", ism_02="")
    non_canon = [_row(phone="", raqam_02="222", ism_02="real")]
    assignments, overflow, dropped = plan_phone_slots(canon, non_canon)
    assert assignments == {"raqam_02": ("222", "real")}


def test_name_missing_becomes_none():
    canon = _row(phone="111")
    non_canon = [_row(phone="222")]   # no name
    assignments, _, _ = plan_phone_slots(canon, non_canon)
    assert assignments == {"raqam_02": ("222", None)}
