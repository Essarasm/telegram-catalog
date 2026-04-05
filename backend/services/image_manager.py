"""Batch upload and manage product images.

Usage:
  python -m backend.services.image_manager /path/to/images/folder

Expected: image filenames should match product codes or contain product names.
Supports: .jpg, .jpeg, .png, .webp
Images are resized to max 800x800 and compressed for fast loading.
"""
import os
import sys
import re
from pathlib import Path
from PIL import Image, ImageOps

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from backend.database import get_db

MAX_SIZE = (800, 800)
IMAGES_DIR = os.getenv("IMAGES_DIR", "./images")
QUALITY = 85


def process_image(src_path: str, dest_path: str):
    """Resize, apply EXIF orientation, and compress an image."""
    img = Image.open(src_path)
    # Apply EXIF orientation so phone photos display correctly
    img = ImageOps.exif_transpose(img)
    img.thumbnail(MAX_SIZE, Image.LANCZOS)
    if img.mode in ('RGBA', 'P'):
        img = img.convert('RGB')
    img.save(dest_path, 'JPEG', quality=QUALITY, optimize=True)


def rotate_image(image_path: str, degrees: int = 90):
    """Rotate an existing image counter-clockwise by given degrees and re-save."""
    img = Image.open(image_path)
    img = img.rotate(degrees, expand=True)
    if img.mode in ('RGBA', 'P'):
        img = img.convert('RGB')
    img.save(image_path, 'JPEG', quality=QUALITY, optimize=True)


def import_images(source_dir: str):
    """Import images from a directory, matching to products by filename."""
    conn = get_db()
    products = conn.execute("SELECT id, code, name FROM products").fetchall()
    os.makedirs(IMAGES_DIR, exist_ok=True)

    matched = 0
    for f in Path(source_dir).iterdir():
        if f.suffix.lower() not in ('.jpg', '.jpeg', '.png', '.webp'):
            continue

        fname = f.stem.strip()
        product = None

        # Try matching by code first
        for p in products:
            if p['code'] and fname == p['code'].strip():
                product = p
                break

        # Try matching by name (fuzzy)
        if not product:
            fname_lower = fname.lower()
            for p in products:
                if fname_lower in p['name'].lower() or p['name'].lower() in fname_lower:
                    product = p
                    break

        if product:
            dest = os.path.join(IMAGES_DIR, f"{product['id']}.jpg")
            process_image(str(f), dest)
            conn.execute(
                "UPDATE products SET image_path = ? WHERE id = ?",
                (f"{product['id']}.jpg", product['id']),
            )
            matched += 1
            print(f"  ✓ {f.name} → {product['name']}")
        else:
            print(f"  ✗ {f.name} — no matching product found")

    conn.commit()
    print(f"\nMatched {matched} images out of {sum(1 for _ in Path(source_dir).iterdir())} files")
    conn.close()


def generate_placeholder():
    """Generate a simple placeholder image for products without photos."""
    os.makedirs(IMAGES_DIR, exist_ok=True)
    placeholder_path = os.path.join(IMAGES_DIR, "placeholder.jpg")
    if os.path.exists(placeholder_path):
        return

    img = Image.new('RGB', (400, 400), color=(243, 244, 246))
    img.save(placeholder_path, 'JPEG', quality=90)
    print(f"Placeholder created: {placeholder_path}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        import_images(sys.argv[1])
    else:
        generate_placeholder()
        print("Usage: python -m backend.services.image_manager /path/to/images/")
        print("  Or run without args to generate placeholder image.")
