#!/usr/bin/env python3
"""One-shot: convert every {id}.png/.jpg in IMAGES_DIR to {id}.webp@q=80.

Three phases (zero broken-image window):
  1. Encode every source → .webp (sources still on disk).
  2. sync_images.sync() flips DB image_path to .webp for every product.
  3. Delete the original PNG/JPG files.

Usage on Railway:
    railway ssh "python tools/reprocess_images_webp.py"
    railway ssh "python tools/reprocess_images_webp.py --dry-run"

Idempotent — re-running skips files whose .webp already exists.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.services.convert_to_webp import reprocess_directory


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default=os.getenv("IMAGES_DIR", "/data/images"),
                    help="Images directory (default: $IMAGES_DIR or /data/images)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Count files that would be converted; write nothing.")
    args = ap.parse_args()

    images_dir = Path(args.dir)
    print(f"[reprocess_webp] dir={images_dir}  dry_run={args.dry_run}", flush=True)

    result = reprocess_directory(images_dir, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))

    if not result.get("ok"):
        return 1
    if result.get("errors", 0) > 0:
        # Surface non-zero exit so the operator notices in railway ssh logs.
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
