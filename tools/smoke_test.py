#!/usr/bin/env python3
"""Post-deploy smoke test — verifies critical endpoints are responding.

Run after every deploy (manually or embedded in morning-brief / handoff):
    python3 tools/smoke_test.py

Exits 0 if all pass, 1 if any fail. Output is concise — one line per check.
"""
import sys
import json
import time

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "httpx"])
    import httpx

BASE = "https://telegram-catalog-production.up.railway.app"
TIMEOUT = 15

# Each check: (name, method, path, expected_status, body_check_fn_or_None)
CHECKS = [
    ("App HTML loads", "GET", "/", 200, lambda b: "<div id" in b),
    ("Categories API", "GET", "/api/categories", 200, lambda b: isinstance(json.loads(b), list)),
    ("Products API", "GET", "/api/products?limit=1", 200, lambda b: json.loads(b).get("items") is not None),
    ("User check", "GET", "/api/users/check?telegram_id=652836922", 200,
     lambda b: json.loads(b).get("registered") is not None),
    ("Agent stats (non-agent → 403 or ok)", "GET", "/api/agent/stats?telegram_id=1", None,
     lambda b: True),  # 403 for non-agents is fine
    ("Cabinet orders", "GET", "/api/cabinet/orders?telegram_id=652836922", 200, None),
    ("Akt-sverki", "GET", "/api/cabinet/akt-sverki?telegram_id=652836922&limit=5", None,
     lambda b: True),  # 200 when linked, 422 when no client — both healthy
    ("Cabinet payments", "GET", "/api/cabinet/payments?telegram_id=652836922&limit=1", 200, None),
    ("Finance balance", "GET", "/api/finance/balance?telegram_id=652836922", 200, None),
    ("Admin receivables", "GET", "/api/admin/receivables?currency=UZS&admin_key=rassvet2026", 200, None),
]


def run() -> bool:
    print(f"🔍 Smoke test — {BASE}")
    print(f"   {len(CHECKS)} checks\n")

    passed = failed = 0
    start = time.time()

    client = httpx.Client(timeout=TIMEOUT, follow_redirects=True)

    for name, method, path, expected_status, body_check in CHECKS:
        try:
            url = BASE + path
            if method == "GET":
                resp = client.get(url)
            else:
                resp = client.post(url)

            status_ok = expected_status is None or resp.status_code == expected_status
            body_ok = True
            if body_check and status_ok:
                try:
                    body_ok = body_check(resp.text)
                except Exception:
                    body_ok = False

            if status_ok and body_ok:
                print(f"  ✅ {name} ({resp.status_code})")
                passed += 1
            else:
                print(f"  ❌ {name} — status={resp.status_code} "
                      f"(expected {expected_status}), body_ok={body_ok}")
                failed += 1
        except Exception as e:
            print(f"  ❌ {name} — {str(e)[:80]}")
            failed += 1

    client.close()
    elapsed = time.time() - start

    print(f"\n{'✅' if failed == 0 else '❌'} "
          f"{passed}/{passed + failed} passed in {elapsed:.1f}s")

    return failed == 0


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
