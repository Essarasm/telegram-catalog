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

    # Find all image files named as {product_id}.jpg
    image_files = {}
    for f in IMAGES_DIR.iterdir():
        if f.suffix.lower() in ('.jpg', '.jpeg', '.png', '.webp'):
            stem = f.stem
            try:
                pid = int(stem)
                image_files[pid] = f.name
            except ValueError:
                continue  # skip non-numeric filenames

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
