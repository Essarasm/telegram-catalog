"""Stale-upload warning helper tests.

Covers the two signals that catch the 2026-05-14 /realorders failure mode
(Alisher re-uploaded yesterday's file; importer was a silent no-op):

  * stale-date — file's `date_max` more than `grace_days` before today
  * no-new-rows — `inserted == 0 and updated > 0`
"""
from datetime import date

import pytest

from backend.services.upload_warnings import stale_upload_warnings


TODAY = date(2026, 5, 25)


class TestStaleDate:
    def test_today_data_no_warning(self):
        assert stale_upload_warnings(
            date_min="2026-05-25", date_max="2026-05-25",
            inserted=10, updated=5, today=TODAY,
        ) == []

    def test_yesterday_within_grace_no_warning(self):
        # Default grace_days=1 — yesterday's data is the normal workflow
        # (upload at 19:00 for the day that just ended).
        assert stale_upload_warnings(
            date_min="2026-05-24", date_max="2026-05-24",
            inserted=10, updated=5, today=TODAY,
        ) == []

    def test_two_days_old_warns(self):
        w = stale_upload_warnings(
            date_min="2026-05-23", date_max="2026-05-23",
            inserted=10, updated=5, today=TODAY,
        )
        assert len(w) == 1
        assert "Eski ma'lumot" in w[0]
        assert "2026-05-23" in w[0]
        assert "2026-05-25" in w[0]

    def test_range_dates_use_max_for_check(self):
        # date_min very old but date_max within grace → no warning
        assert stale_upload_warnings(
            date_min="2026-05-01", date_max="2026-05-24",
            inserted=10, updated=5, today=TODAY,
        ) == []

    def test_range_dates_use_min_in_message_when_different(self):
        w = stale_upload_warnings(
            date_min="2026-05-10", date_max="2026-05-13",
            inserted=10, updated=5, today=TODAY,
        )
        assert len(w) == 1
        assert "2026-05-10…2026-05-13" in w[0]

    def test_missing_dates_no_warning(self):
        assert stale_upload_warnings(
            date_min=None, date_max=None,
            inserted=0, updated=0, today=TODAY,
        ) == []

    def test_unparseable_date_no_crash(self):
        # Bad input — gracefully skip rather than 500
        assert stale_upload_warnings(
            date_min="not-a-date", date_max="not-a-date",
            inserted=10, updated=5, today=TODAY,
        ) == []

    def test_custom_grace_days(self):
        # grace_days=2 → 5/23 (=2 days old) within tolerance,
        # 5/22 (=3 days old) over tolerance.
        assert stale_upload_warnings(
            date_min="2026-05-23", date_max="2026-05-23",
            inserted=10, updated=5, today=TODAY, grace_days=2,
        ) == []
        w = stale_upload_warnings(
            date_min="2026-05-22", date_max="2026-05-22",
            inserted=10, updated=5, today=TODAY, grace_days=2,
        )
        assert len(w) == 1


class TestNoNewRows:
    def test_inserted_zero_updated_positive_warns(self):
        w = stale_upload_warnings(
            date_min="2026-05-25", date_max="2026-05-25",
            inserted=0, updated=50, today=TODAY,
        )
        assert len(w) == 1
        assert "Yangi qator yo'q" in w[0]
        assert "50" in w[0]

    def test_inserted_positive_no_warning(self):
        # New rows landed — not a silent re-upload
        assert stale_upload_warnings(
            date_min="2026-05-25", date_max="2026-05-25",
            inserted=10, updated=50, today=TODAY,
        ) == []

    def test_both_zero_no_warning(self):
        # Empty upload (probably an error elsewhere) — no signal
        assert stale_upload_warnings(
            date_min="2026-05-25", date_max="2026-05-25",
            inserted=0, updated=0, today=TODAY,
        ) == []

    def test_only_inserted_no_warning(self):
        assert stale_upload_warnings(
            date_min="2026-05-25", date_max="2026-05-25",
            inserted=20, updated=0, today=TODAY,
        ) == []


class TestBothSignals:
    def test_may_14_actual_case_fires_no_new_rows_only(self):
        """The 2026-05-14 incident: Alisher uploaded `real orders
        13.05.26.xls` on 5/14. date_max=5/13 is within 1-day grace, so
        stale-date stays silent, but no-new-rows fires because the
        importer replaced the existing 5/13 rows in place.
        """
        w = stale_upload_warnings(
            date_min="2026-05-13", date_max="2026-05-13",
            inserted=0, updated=61, today=date(2026, 5, 14),
        )
        assert len(w) == 1
        assert "Yangi qator yo'q" in w[0]

    def test_three_day_old_re_upload_fires_both(self):
        # Uploading Monday's file again on Thursday — both signals fire
        w = stale_upload_warnings(
            date_min="2026-05-22", date_max="2026-05-22",
            inserted=0, updated=40, today=TODAY,
        )
        assert len(w) == 2
        assert any("Eski ma'lumot" in x for x in w)
        assert any("Yangi qator yo'q" in x for x in w)


class TestTashkentNowDefault:
    def test_defaults_today_to_tashkent_when_not_passed(self):
        # No exception on default-today path
        result = stale_upload_warnings(
            date_min="1900-01-01", date_max="1900-01-01",
            inserted=10, updated=5,
        )
        # 125-year-old data definitely triggers stale-date
        assert len(result) == 1
        assert "Eski ma'lumot" in result[0]


@pytest.mark.parametrize("date_max,today,grace,should_warn", [
    ("2026-05-25", date(2026, 5, 25), 1, False),  # today
    ("2026-05-24", date(2026, 5, 25), 1, False),  # yesterday — within grace
    ("2026-05-23", date(2026, 5, 25), 1, True),   # 2 days old — over grace
    ("2026-05-23", date(2026, 5, 25), 2, False),  # 2 days old — within grace=2
    ("2026-05-26", date(2026, 5, 25), 1, False),  # future date — no warning
])
def test_grace_boundary(date_max, today, grace, should_warn):
    w = stale_upload_warnings(
        date_min=date_max, date_max=date_max,
        inserted=10, updated=5,
        today=today, grace_days=grace,
    )
    if should_warn:
        assert len(w) == 1
    else:
        assert w == []
