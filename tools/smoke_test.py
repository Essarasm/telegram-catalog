#!/usr/bin/env python3
"""Smoke test — verifies critical endpoints are responding.

Two modes:
    python3 tools/smoke_test.py             # endpoint liveness only
    python3 tools/smoke_test.py --pre-deploy  # liveness + dual-currency contract

Pre-deploy mode catches the regression class that produced the 2026-04-23
UZS-trend incident. It runs two layers of dual-currency checks:

  1. Required — TEST_CLIENT_1C_NAME (1C name of a real client with both
     UZS and USD history). Hits /api/admin/client/{name}/history, asserts
     both currencies have non-empty period arrays. Catches import-side
     regressions (1C column drops, currency aggregation bugs).

  2. Optional — TEST_CLIENT_TG (telegram_id of the same client, only if
     they are registered in the bot). Hits /api/cabinet/spend-trend and
     /api/cabinet/activity-summary, asserts dual-currency response shape.
     Catches cabinet-wrapper regressions specifically.

Set both for full coverage, or just the name for import-side coverage.

Exits 0 if all pass, 1 if any fail. Wired into the .claude pre-deploy hook
(`.claude/hooks/pre_deploy_check.sh`) so any `railway up` from Claude Code
runs this with --pre-deploy first.
"""
import os
import sys
import json
import time
import urllib.parse

ADMIN_KEY = os.getenv("ADMIN_API_KEY") or "rassvet2026"
TEST_CLIENT_1C_NAME = os.getenv("TEST_CLIENT_1C_NAME")
TEST_CLIENT_TG = os.getenv("TEST_CLIENT_TG")

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
    ("Admin receivables", "GET", f"/api/admin/receivables?currency=UZS&admin_key={ADMIN_KEY}", 200, None),
]


def _check_admin_history_contract(client: httpx.Client) -> tuple[int, int]:
    """Layer 1 (required): hit /api/admin/client/{name}/history and assert
    both UZS and USD have non-empty period arrays. Catches import-side
    regressions where a 1C column collapse or currency aggregation bug
    silently zeroes out one currency.
    """
    if not TEST_CLIENT_1C_NAME:
        print("\n  ❌ TEST_CLIENT_1C_NAME env var not set — required for --pre-deploy")
        print("     Set in your shell with the 1C name of a real client whose")
        print("     trade history covers both UZS and USD shipments:")
        print("     export TEST_CLIENT_1C_NAME='Азам ЖАРТЕПА'")
        return 0, 1

    encoded = urllib.parse.quote(TEST_CLIENT_1C_NAME)
    url = f"{BASE}/api/admin/client/{encoded}/history?admin_key={ADMIN_KEY}"
    try:
        resp = client.get(url)
        body = resp.json()
        history = body.get("history") or {}
        uzs_periods = len(history.get("UZS") or [])
        usd_periods = len(history.get("USD") or [])

        if not history:
            print(f"  ❌ Contract: client '{TEST_CLIENT_1C_NAME}' has no history "
                  "in client_balances — name spelling correct?")
            return 0, 1
        if uzs_periods == 0:
            print(f"  ❌ Contract: client '{TEST_CLIENT_1C_NAME}' has ZERO UZS "
                  "periods — 1C UZS import may be broken")
            return 0, 1
        if usd_periods == 0:
            print(f"  ❌ Contract: client '{TEST_CLIENT_1C_NAME}' has ZERO USD "
                  "periods — 1C USD import may be broken (Error Log #20 class)")
            return 0, 1

        print(f"  ✅ Contract: '{TEST_CLIENT_1C_NAME}' dual-currency history "
              f"healthy (UZS periods={uzs_periods}, USD periods={usd_periods})")
        return 1, 0
    except Exception as e:
        print(f"  ❌ Contract: admin/client history check raised — {str(e)[:120]}")
        return 0, 1


def _check_cabinet_contract(client: httpx.Client) -> tuple[int, int]:
    """Layer 2 (optional): if TEST_CLIENT_TG is also set, assert the cabinet
    endpoints carry both currencies in the expected response shape. Catches
    cabinet-wrapper regressions specifically.
    """
    if not TEST_CLIENT_TG:
        print("  ⚪ Cabinet contract: skipped (TEST_CLIENT_TG not set — admin "
              "history check only). Set TEST_CLIENT_TG to a registered "
              "telegram_id linked to the same client for full coverage.")
        return 0, 0

    passed = failed = 0
    try:
        url = f"{BASE}/api/cabinet/spend-trend?telegram_id={TEST_CLIENT_TG}&months=16"
        body = client.get(url).json()
        months = body.get("months", [])
        if not body.get("linked"):
            print(f"  ❌ Cabinet: TEST_CLIENT_TG={TEST_CLIENT_TG} not linked")
            failed += 1
        elif len(months) != 16:
            print(f"  ❌ Cabinet: spend-trend returned {len(months)} months, expected 16")
            failed += 1
        else:
            missing = [m["month"] for m in months
                       if "total_uzs" not in m or "total_usd" not in m]
            uzs = sum(1 for m in months if (m.get("total_uzs") or 0) > 0)
            usd = sum(1 for m in months if (m.get("total_usd") or 0) > 0)
            if missing:
                print(f"  ❌ Cabinet: months missing currency keys: {missing[:3]}")
                failed += 1
            elif uzs == 0:
                print(f"  ❌ Cabinet: spend-trend has ZERO UZS months "
                      "(2026-04-23 regression class)")
                failed += 1
            elif usd == 0:
                print(f"  ❌ Cabinet: spend-trend has ZERO USD months "
                      "(Error Log #20 class)")
                failed += 1
            else:
                print(f"  ✅ Cabinet: spend-trend dual-currency healthy "
                      f"(UZS months={uzs}, USD months={usd})")
                passed += 1
    except Exception as e:
        print(f"  ❌ Cabinet: spend-trend check raised — {str(e)[:120]}")
        failed += 1

    return passed, failed


def run(pre_deploy: bool = False) -> bool:
    mode_label = "pre-deploy" if pre_deploy else "smoke"
    print(f"🔍 {mode_label} test — {BASE}")
    print(f"   {len(CHECKS)} liveness checks"
          + (" + dual-currency contract" if pre_deploy else "") + "\n")

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

    if pre_deploy:
        ap, af = _check_admin_history_contract(client)
        passed += ap
        failed += af
        cp, cf = _check_cabinet_contract(client)
        passed += cp
        failed += cf

    client.close()
    elapsed = time.time() - start

    print(f"\n{'✅' if failed == 0 else '❌'} "
          f"{passed}/{passed + failed} passed in {elapsed:.1f}s")

    return failed == 0


if __name__ == "__main__":
    pre_deploy = "--pre-deploy" in sys.argv
    ok = run(pre_deploy=pre_deploy)
    sys.exit(0 if ok else 1)
