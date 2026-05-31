#!/bin/bash
# Foundation audit — fast self-contained health check (~30 sec).
#
# Run this weekly (or whenever something feels off) for a quick snapshot
# of where drift might be accumulating. For a deep agent-driven audit
# across all 5 dimensions, use the /audit slash command instead — that
# spawns the foundation-auditor agent and produces a ranked report.
#
# What this checks (in order, cheap → expensive):
#   1. Pre-commit hook installed?
#   2. Working tree clean? Commits ahead of origin?
#   3. Schema version + critical row counts
#   4. Largest files in each language (god-module canary)
#   5. Bundle size (if frontend/dist exists)
#   6. Pre-commit guards effective? (synthetic dry-run)
#   7. Test suite passes? (full pytest run)
#   8. Latest Error Log entry — how stale?
#
# Designed to be runnable from any directory:
#   bash tools/audit.sh
#
# Foundation Audit 2026-05-10 produced the patterns and budgets this
# script enforces. See obsidian-vault/🐛 Error Log.md #46 for context.

set -u
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# $'...' lets bash interpret the escape so both `echo` and `printf` render correctly.
GREEN=$'\033[0;32m'
YELLOW=$'\033[0;33m'
RED=$'\033[0;31m'
RESET=$'\033[0m'
DIM=$'\033[2m'

ok=0
warn=0
fail=0

# Helper: emit a check result line.
check() {
    local status="$1"; shift
    case "$status" in
        OK)   printf "${GREEN}✅${RESET} %s\n" "$*"; ok=$((ok+1));;
        WARN) printf "${YELLOW}🟡${RESET} %s\n" "$*"; warn=$((warn+1));;
        FAIL) printf "${RED}❌${RESET} %s\n" "$*"; fail=$((fail+1));;
    esac
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Foundation audit — $(date '+%Y-%m-%d %H:%M %Z')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Pre-commit hook installed ──────────────────────────────────────────
echo ""
echo "── 1. Pre-commit hook"
if [ -L .git/hooks/pre-commit ] || [ -f .git/hooks/pre-commit ]; then
    if [ -L .git/hooks/pre-commit ]; then
        target="$(readlink .git/hooks/pre-commit)"
        if [[ "$target" == *"scripts/git-hooks/pre-commit"* ]]; then
            check OK "Hook installed (symlinked → tracked source)"
        else
            check WARN "Hook installed but points elsewhere: $target"
        fi
    else
        check WARN "Hook installed but as a copy, not a symlink — updates won't propagate. Run: bash scripts/install-git-hooks.sh"
    fi
else
    check FAIL "Pre-commit hook NOT installed. Run: bash scripts/install-git-hooks.sh"
fi

# ── 2. Git state ──────────────────────────────────────────────────────────
echo ""
echo "── 2. Git state"
dirty=$(git status --porcelain | grep -v -E '^\?\? (\.DS_Store|\.venv/|data/snapshots/)' | wc -l | tr -d ' ')
if [ "$dirty" = "0" ]; then
    check OK "Working tree clean"
else
    check WARN "$dirty uncommitted files (excluding .DS_Store, .venv/, data/snapshots/)"
fi

ahead=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo "?")
behind=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo "?")
if [ "$ahead" = "0" ] && [ "$behind" = "0" ]; then
    check OK "In sync with origin/main"
elif [ "$ahead" != "0" ] && [ "$behind" = "0" ]; then
    check WARN "$ahead commit(s) ahead of origin/main — push when ready"
elif [ "$ahead" = "0" ] && [ "$behind" != "0" ]; then
    check WARN "$behind commit(s) behind origin/main — pull when ready"
else
    check WARN "Diverged: $ahead ahead, $behind behind"
fi

# ── 3. Database health ────────────────────────────────────────────────────
echo ""
echo "── 3. Database (local data/catalog.db)"
if [ -f data/catalog.db ]; then
    schema_ver=$(sqlite3 data/catalog.db "SELECT MAX(version) FROM schema_version" 2>/dev/null || echo "?")
    expected_ver=$(grep -E '^SCHEMA_VERSION = ' backend/database.py | head -1 | grep -oE '[0-9]+' | head -1)
    if [ "$schema_ver" = "$expected_ver" ]; then
        check OK "schema_version: $schema_ver (matches SCHEMA_VERSION constant)"
    else
        check WARN "schema_version: $schema_ver (code expects $expected_ver — migration pending)"
    fi
else
    check WARN "No local DB at data/catalog.db (not a problem if you only run via Railway)"
fi

# ── 4. File-size hotspots ─────────────────────────────────────────────────
echo ""
echo "── 4. Top 5 largest source files (god-module canary)"
echo "    Python:"
find backend bot -name "*.py" -not -path "*/__pycache__/*" 2>/dev/null \
    | xargs wc -l 2>/dev/null | sort -rn | head -6 | tail -5 \
    | awk '{printf "        %s lines  %s\n", $1, $2}'
echo "    JS/JSX:"
find frontend/src -type f \( -name "*.jsx" -o -name "*.js" \) 2>/dev/null \
    | xargs wc -l 2>/dev/null | sort -rn | head -6 | tail -5 \
    | awk '{printf "        %s lines  %s\n", $1, $2}'

# Flag any file over 2000 lines as worth refactoring.
# Exemption: backend/database.py is intentionally monolithic per CLAUDE.md's
# schema-version doctrine — single source of truth for all CREATE TABLE +
# init_db() migrations. Splitting it would fragment the schema-version
# discipline. (Other large files are tracked in Notion Command Center's
# "Later (deferred until trigger)" bucket — they warn on purpose until a
# trigger fires.)
big=$(find backend bot frontend/src -type f \( -name "*.py" -o -name "*.jsx" -o -name "*.js" \) -not -path "*/__pycache__/*" -not -path "backend/database.py" 2>/dev/null \
    | xargs wc -l 2>/dev/null | awk '$1 > 2000 && $2 != "total" && $2 != "backend/database.py" {print $2}' | head -3)
if [ -n "$big" ]; then
    echo "    ${YELLOW}🟡${RESET} Files over 2000 lines (consider refactoring):"
    echo "$big" | awk '{printf "          %s\n", $0}'
    warn=$((warn+1))
fi

# ── 5. Bundle size ────────────────────────────────────────────────────────
echo ""
echo "── 5. Frontend bundle"
if [ -d frontend/dist/assets ]; then
    if python3 tools/check_bundle_size.py >/tmp/audit_bundle.out 2>&1; then
        cat /tmp/audit_bundle.out | tail -3 | head -2 | sed 's/^/    /'
        check OK "Bundle within budget"
    else
        cat /tmp/audit_bundle.out | head -10 | sed 's/^/    /'
        check FAIL "Bundle exceeds budget — see CLAUDE.md Hard Constraints"
    fi
else
    check WARN "frontend/dist not built — bundle budget not verified this run"
fi

# ── 6. Test suite ─────────────────────────────────────────────────────────
echo ""
echo "── 6. Test suite (pytest)"
if [ -f .venv/bin/python ]; then
    py=".venv/bin/python"
elif command -v python3.10 >/dev/null; then
    py="python3.10"
else
    py="python3"
fi
test_out=$($py -m pytest -q --tb=no 2>&1 | tail -1)
if echo "$test_out" | grep -qE 'passed' && ! echo "$test_out" | grep -qE 'failed|error'; then
    check OK "Tests: $test_out"
else
    check FAIL "Tests: $test_out"
fi

# ── 7. Pre-commit guards effective? ──────────────────────────────────────
echo ""
echo "── 7. Pre-commit guards (sanity check)"
if grep -q "SKIP_SCHEMA_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_CHATID_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_AUTHCALL_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_CHATTYPE_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_INLINE_FETCH_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_STATUS_VOCABULARY_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_REALORDERS_CURRENCY_FILTER_CHECK" scripts/git-hooks/pre-commit 2>/dev/null \
   && grep -q "SKIP_BUNDLE_CHECK" scripts/git-hooks/pre-commit 2>/dev/null; then
    check OK "All seven foundation guards present in tracked hook"
else
    check FAIL "Foundation guards missing from scripts/git-hooks/pre-commit"
fi

# ── 8. Error Log freshness ────────────────────────────────────────────────
echo ""
echo "── 8. Error Log"
errlog="../obsidian-vault/🐛 Error Log.md"
if [ -f "$errlog" ]; then
    latest=$(grep -E '^### #[0-9]+' "$errlog" | tail -1 | head -c 80)
    if [ -n "$latest" ]; then
        check OK "Latest entry: $latest"
    else
        check WARN "Error Log exists but no #NN entries found"
    fi
else
    check WARN "Error Log not found at $errlog"
fi

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  Summary: ${GREEN}%d ok${RESET}  ${YELLOW}%d warn${RESET}  ${RED}%d fail${RESET}\n" "$ok" "$warn" "$fail"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
if [ "$fail" -gt 0 ]; then
    echo "❌ Audit found failures. For a deeper analysis, run /audit (foundation-auditor agent)."
    exit 1
elif [ "$warn" -gt 0 ]; then
    echo "🟡 Audit clean but with warnings worth reviewing."
    exit 0
else
    echo "✅ Foundation healthy."
    exit 0
fi
