"""Sync product images: scan images/ dir and update image_path in DB.

Run on every startup (after import) to link committed image files to products.
Image filenames must be: {product_id}.jpg (e.g., 6832.jpg)
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from backend.database import get_db

IMAGES_DIR = Path(os.getenv("IMAGES_DIR", "./images"))


def sync():
    if not IMAGES_DIR.exists():
        print("sync_images: No images/ directory found — skipping.")
        return

    # Find all image files named as {product_id}.{ext}; prefer .webp when both exist
    # so a partially-converted dir flips DB to .webp first, then PNGs can be deleted.
    ext_priority = {'.webp': 0, '.png': 1, '.jpg': 2, '.jpeg': 2}
    candidates: dict[int, tuple[int, str]] = {}
    for f in IMAGES_DIR.iterdir():
        ext = f.suffix.lower()
        if ext not in ext_priority:
            continue
        try:
            pid = int(f.stem)
        except ValueError:
            continue  # skip non-numeric filenames
        prio = ext_priority[ext]
        if pid not in candidates or prio < candidates[pid][0]:
            candidates[pid] = (prio, f.name)
    image_files = {pid: name for pid, (_, name) in candidates.items()}

    if not image_files:
        print("sync_images: No product image files found in images/.")
        return

    conn = get_db()

    # Clear any stale image_path references (files that no longer exist)
    conn.execute("UPDATE products SET image_path = NULL WHERE image_path IS NOT NULL")

    # Set image_path for each matched product
    updated = 0
    for pid, filename in image_files.items():
        cursor = conn.execute(
            "UPDATE products SET image_path = ? WHERE id = ?",
            (filename, pid),
        )
        if cursor.rowcount > 0:
            updated += 1

    conn.commit()
    conn.close()
    print(f"sync_images: Linked {updated} product images (from {len(image_files)} files).")


if __name__ == "__main__":
    sync()
