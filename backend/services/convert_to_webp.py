"""Image WebP conversion — shared by upload intake and the one-shot reprocessor.

WebP at q=80, method=6 (max-effort encode):
- ~5-6× smaller than PNG for our cutout product photos
- preserves transparency (RGBA) — matches the bgremoval pipeline output
- universally supported in Telegram Mini App browsers (Android Chromium, iOS WKWebView)
"""
from __future__ import annotations

import io
from pathlib import Path

from PIL import Image

WEBP_QUALITY = 80
WEBP_METHOD = 6
SOURCE_EXTS = ('.png', '.jpg', '.jpeg')


def encode_webp_from_bytes(src_bytes: bytes, dest_path: Path) -> None:
    """Decode any supported source bytes and write WebP to dest_path."""
    with Image.open(io.BytesIO(src_bytes)) as img:
        img.load()
        img.save(dest_path, "WEBP", quality=WEBP_QUALITY, method=WEBP_METHOD)


def encode_webp_from_file(src_path: Path, dest_path: Path | None = None) -> Path:
    """Convert one image file on disk to WebP. Returns the WebP path written."""
    if dest_path is None:
        dest_path = src_path.with_suffix('.webp')
    with Image.open(src_path) as img:
        img.load()
        img.save(dest_path, "WEBP", quality=WEBP_QUALITY, method=WEBP_METHOD)
    return dest_path


def reprocess_directory(images_dir: Path, *, dry_run: bool = False) -> dict:
    """Convert every {id}.png/.jpg to {id}.webp in images_dir and remove sources.

    Three-phase to keep the catalog serving images throughout:
      1. Encode every source → .webp (sources still on disk; DB still points to .png).
      2. Run sync_images.sync() → DB image_path flips to .webp for every product.
      3. Delete source PNG/JPG files (DB no longer references them).

    Idempotent — re-running skips files whose .webp already exists.
    Set dry_run=True to count without writing/deleting.
    """
    if not images_dir.exists():
        return {"ok": False, "error": f"{images_dir} does not exist"}

    sources: list[Path] = []
    for f in images_dir.iterdir():
        if not f.is_file():
            continue
        if f.suffix.lower() not in SOURCE_EXTS:
            continue
        try:
            int(f.stem)
        except ValueError:
            continue
        sources.append(f)

    encoded = skipped_already_webp = errors_count = 0
    error_files: list[dict] = []
    bytes_before = bytes_after = 0

    for src in sorted(sources):
        webp = src.with_suffix('.webp')
        if webp.exists() and webp.stat().st_size > 0:
            skipped_already_webp += 1
            continue
        if dry_run:
            encoded += 1
            bytes_before += src.stat().st_size
            continue
        try:
            encode_webp_from_file(src, webp)
            bytes_before += src.stat().st_size
            bytes_after += webp.stat().st_size
            encoded += 1
        except Exception as e:
            if webp.exists():
                webp.unlink(missing_ok=True)
            errors_count += 1
            error_files.append({"file": src.name, "error": repr(e)[:200]})

    deleted = 0
    if not dry_run and encoded > 0:
        from backend.services.sync_images import sync as sync_images
        sync_images()

        for src in sources:
            webp = src.with_suffix('.webp')
            if webp.exists() and webp.stat().st_size > 0:
                try:
                    src.unlink()
                    deleted += 1
                except OSError as e:
                    errors_count += 1
                    error_files.append({"file": src.name, "error": f"delete failed: {e!r}"[:200]})

    return {
        "ok": True,
        "dry_run": dry_run,
        "encoded": encoded,
        "skipped_already_webp": skipped_already_webp,
        "deleted_sources": deleted,
        "errors": errors_count,
        "error_files": error_files[:20],
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "saved_mb": round((bytes_before - bytes_after) / (1024 * 1024), 1) if bytes_before else 0,
    }
