"""Photo-state read helper (dual-source — see .claude/rules/12-dual-source-columns.md).

A product's "does it have a photo?" question has TWO writer-owned compartments:

  1. `products.image_path` — the FINAL, catalog-ready WebP (trimmed /
     background-removed by the offline pipeline, uploaded via
     /api/admin/upload-images or /upload-single-image, linked by sync_images).
  2. `photo_batch_items.status` — the RAW capture state from the catalog-group
     /foto workflow. An employee replying with a File marks the item
     'photographed'; the raw original lands in Google Drive for offline
     trimming. It is NOT catalog-ready and is deliberately NOT written to
     image_path.

Before this helper, the Product Cleanup tab read `image_path` only and was
structurally blind to compartment 2 — a freshly photographed product (raw in
Drive, awaiting trim) looked identical to one nobody had touched. This helper
is the single precedence point so every reader agrees.

Precedence (highest first):
  has_image          — image_path is non-empty (final photo exists; trumps all)
  pending_processing — no final photo, but a raw /foto capture exists (in trim queue)
  skipped            — no final photo, employee tapped ⏭ Skip in /foto
  missing            — none of the above; genuinely needs a photo
"""
from __future__ import annotations

from typing import Optional


def photo_state(
    image_path: Optional[str],
    has_photographed: bool,
    has_skipped: bool,
) -> str:
    """Return one of: 'has_image' | 'pending_processing' | 'skipped' | 'missing'.

    `has_photographed` / `has_skipped` reflect whether the product has ANY
    photo_batch_items row in that status (across all batches). 'photographed'
    wins over 'skipped' — a later capture supersedes an earlier skip.
    """
    if image_path and image_path.strip():
        return "has_image"
    if has_photographed:
        return "pending_processing"
    if has_skipped:
        return "skipped"
    return "missing"
