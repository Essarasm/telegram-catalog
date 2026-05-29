"""Backfill: re-run a 1C clients XLS through the post-fix apply_clients_upload.

Pre-2026-05-29 the importer's normalize_phone() collapsed multi-phone cells
into one blob and kept only the last 9 digits, losing every other number in
the cell (see Error Log MULTI_PHONE_CELL_TRUNCATION). The new parser splits
properly and lands extras in raqam_02/03 (fill-only) + ism_02/03.

This script feeds a 1C XLS through the new path so the ~280 multi-phone
clients get repaired in one shot. Idempotent — re-running produces no
changes once the row already has the correct phone slots.

Usage:
    python3 tools/backfill_multi_phone_cells.py /tmp/clients_28.05.26.xls
"""
import sys


def main(path: str) -> None:
    with open(path, "rb") as f:
        data = f.read()
    from backend.services.import_clients import apply_clients_upload
    result = apply_clients_upload(data, filename_hint=path)
    print(result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: backfill_multi_phone_cells.py <path-to-1c-clients.xls>")
        sys.exit(1)
    main(sys.argv[1])
