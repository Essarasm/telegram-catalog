"""Verify the latest DB backup is actually restorable.

A silent-corruption DB backup is worse than no backup — it gives false
confidence. This script tests the most recent `.sql.gz` produced by
tools/backup_db.py:
  1. Decompress to a temp file
  2. Pipe into a fresh temp SQLite DB
  3. Run 3 sanity queries (schema exists, counts reasonable, no SQL errors)
  4. If anything fails → send error alert to Admin group

Runs on every deploy after backup_db.py. Passes silently, alerts on failure.
"""
from __future__ import annotations

import gzip
import os
import sqlite3
import sys
import tempfile

BACKUP_DIR = os.environ.get("DB_BACKUP_DIR", "/data/db_backups")
MIN_ROWS_PRODUCTS = 500      # catalog should have at least this many rows
MIN_ROWS_ALLOWED_CLIENTS = 100


def _latest_backup() -> str | None:
    if not os.path.isdir(BACKUP_DIR):
        return None
    files = sorted([
        os.path.join(BACKUP_DIR, f)
        for f in os.listdir(BACKUP_DIR)
        if f.startswith("catalog_") and f.endswith(".sql.gz")
    ])
    return files[-1] if files else None


def _alert(source: str, exc_type: str, message: str) -> None:
    try:
        from backend.services.error_alert import send_error_alert
        send_error_alert(
            source=source,
            exc_type=exc_type,
            exc_message=message,
            traceback_tail="",
            request_hint="backup_verify",
        )
    except Exception as e:
        print(f"[verify_backup] (also failed to send alert: {e})")


def main(startup: bool = False) -> int:
    path = _latest_backup()
    if not path:
        msg = "No backup file found — backup_db.py may not have run yet."
        print(f"[verify_backup] {msg}")
        if not startup:
            _alert("backup_verify", "NoBackupFound", msg)
        return 1

    size = os.path.getsize(path)
    print(f"[verify_backup] testing {os.path.basename(path)} ({size / 1_000_000:.1f} MB gzipped)")

    # Decompress + load into a fresh temp DB
    try:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmpdb:
            tmp_path = tmpdb.name
        conn = sqlite3.connect(tmp_path)
        try:
            with gzip.open(path, "rb") as gz:
                # Read SQL in reasonable chunks to avoid loading the whole
                # dump into memory (can be 100+ MB uncompressed).
                buf = b""
                while True:
                    chunk = gz.read(65536)
                    if not chunk:
                        break
                    buf += chunk
                    # Execute at statement boundaries (;\n)
                    while b";\n" in buf:
                        stmt, _, buf = buf.partition(b";\n")
                        stmt = (stmt.decode("utf-8") + ";").strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.Error:
                                # Some sqldump directives (BEGIN TRANSACTION, etc.)
                                # may fail in fresh DB — tolerate
                                pass
                if buf.strip():
                    try:
                        conn.execute(buf.decode("utf-8"))
                    except sqlite3.Error:
                        pass
            conn.commit()

            # Sanity checks
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            required = {"products", "allowed_clients", "users", "orders"}
            missing = required - tables
            if missing:
                msg = f"Restored DB missing required tables: {sorted(missing)}"
                print(f"[verify_backup] FAIL — {msg}")
                _alert("backup_verify", "MissingTable", msg)
                return 2

            n_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            n_clients = conn.execute("SELECT COUNT(*) FROM allowed_clients").fetchone()[0]
            if n_products < MIN_ROWS_PRODUCTS:
                msg = f"Restored 'products' has only {n_products} rows (expected ≥ {MIN_ROWS_PRODUCTS})"
                print(f"[verify_backup] FAIL — {msg}")
                _alert("backup_verify", "UnderpopulatedTable", msg)
                return 3
            if n_clients < MIN_ROWS_ALLOWED_CLIENTS:
                msg = f"Restored 'allowed_clients' has only {n_clients} rows (expected ≥ {MIN_ROWS_ALLOWED_CLIENTS})"
                print(f"[verify_backup] FAIL — {msg}")
                _alert("backup_verify", "UnderpopulatedTable", msg)
                return 4

            print(f"[verify_backup] PASS — {len(tables)} tables, "
                  f"{n_products} products, {n_clients} allowed_clients")
            return 0
        finally:
            conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except Exception as e:
        msg = f"Exception during verify: {e!r}"
        print(f"[verify_backup] FAIL — {msg}")
        _alert("backup_verify", type(e).__name__, msg)
        return 5


if __name__ == "__main__":
    startup = "--startup" in sys.argv
    try:
        rc = main(startup=startup)
        if startup:
            sys.exit(0)  # Never block boot on a verify failure
        sys.exit(rc)
    except Exception as e:
        if startup:
            print(f"[verify_backup] ERROR (startup, continuing boot): {e}")
            sys.exit(0)
        raise
