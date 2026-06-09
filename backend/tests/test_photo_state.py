"""Test matrix for photo_state() — every empty/full combination per compartment.

Canonical dual-source-columns test shape (see .claude/rules/12-dual-source-columns.md
Rule 3). Two compartments here: image_path (final) + photo_batch status
(photographed / skipped). image_path-set always wins, so the photographed/skipped
axes only matter when image_path is empty.
"""
from backend.services.photo_state import photo_state


# image_path set → always has_image, regardless of batch status
def test_image_set_trumps_everything():
    assert photo_state("123.webp", False, False) == "has_image"
    assert photo_state("123.webp", True, False) == "has_image"
    assert photo_state("123.webp", False, True) == "has_image"
    assert photo_state("123.webp", True, True) == "has_image"


# image_path empty → fall through to batch status
def test_empty_image_photographed_is_pending():
    assert photo_state(None, True, False) == "pending_processing"
    assert photo_state("", True, False) == "pending_processing"


def test_photographed_beats_skipped():
    # A later capture supersedes an earlier skip.
    assert photo_state(None, True, True) == "pending_processing"


def test_empty_image_skipped_only():
    assert photo_state(None, False, True) == "skipped"


def test_empty_image_nothing_is_missing():
    assert photo_state(None, False, False) == "missing"
    assert photo_state("", False, False) == "missing"


def test_whitespace_image_path_is_not_a_photo():
    # Legacy/padding values must not read as a real image.
    assert photo_state("   ", False, False) == "missing"
    assert photo_state("  ", True, False) == "pending_processing"
