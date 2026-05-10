#!/bin/bash
# Install project git hooks into .git/hooks/.
# Run once after cloning, or after pulling a hook update:
#   bash scripts/install-git-hooks.sh
#
# The hooks live in scripts/git-hooks/ (tracked) and are symlinked into
# .git/hooks/ (not tracked — git doesn't ship hooks on clone).
set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [ ! -d "scripts/git-hooks" ]; then
    echo "❌ scripts/git-hooks/ not found. Are you in the project root?"
    exit 1
fi

# Symlink each hook in scripts/git-hooks/ into .git/hooks/.
# Symlink (not copy) so future updates to the tracked file take effect
# immediately — no re-install needed when scripts/git-hooks/* changes.
for hook in scripts/git-hooks/*; do
    name="$(basename "$hook")"
    ln -sf "../../$hook" ".git/hooks/$name"
    echo "✅ installed: .git/hooks/$name → $hook"
done

cat <<'EOF'

Pre-commit guards now active. Override env vars when you genuinely need them:
  SKIP_SCHEMA_CHECK=1     bypass: CREATE/ALTER TABLE outside backend/database.py
  SKIP_CHATID_CHECK=1     bypass: os.getenv("..._CHAT_ID","...") outside group_config.py
  SKIP_FRONTEND_BUILD=1   skip frontend rebuild (docs-only commits)

Both guards close drift classes the foundation audit caught (Error Log #20, #46).
EOF
