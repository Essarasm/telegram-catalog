"""Boot-smoke: `bot/main.py` must import cleanly (Error Log #87).

The rest of the suite never imports bot/main, so a break in what it pulls from
bot.shared (e.g. a re-export removed by a refactor + ruff --fix) ships GREEN and
crash-loops the bot on boot. This imports bot.main in a FRESH process with a
format-valid dummy BOT_TOKEN — the same import prod does at startup — so that
whole class of break fails CI instead of reaching prod.
"""
import os
import subprocess
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_bot_main_imports_cleanly():
    env = {
        **os.environ,
        # aiogram validates only token FORMAT at Bot() construction (no network).
        "BOT_TOKEN": "123456789:AABBccddeeffgghhiijjkkllmmnnooppqqrss",
        # keep the import's DB makedirs() off the real /data volume.
        "DATABASE_PATH": "/tmp/bot_boot_smoke.db",
    }
    r = subprocess.run(
        [sys.executable, "-c", "import bot.main"],
        cwd=_REPO_ROOT, env=env, capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, (
        "bot/main.py failed to import — this would crash-loop the bot on boot "
        f"(Error Log #87). stderr tail:\n{r.stderr[-2000:]}"
    )
