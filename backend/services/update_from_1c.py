"""
Update catalog from a new 1C export file.

This is the weekly update script. It:
1. Runs producer_script.py on the raw 1C data to assign producers
2. Regenerates the Catalog Clean sheet
3. Re-imports everything into the SQLite database

Usage:
    python3 backend/services/update_from_1c.py /path/to/FINAL.xlsx

The FINAL.xlsx should already have the "1c raw data" sheet with fresh data.
producer_script.py should be run BEFORE this script (or this script runs import directly).
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from backend.services.import_products import import_from_catalog_clean


def update_database(xlsx_path):
    """Re-import all products from the Catalog Clean sheet."""
    if not os.path.exists(xlsx_path):
        print(f"Error: File not found: {xlsx_path}")
        sys.exit(1)

    print(f"Updating database from: {xlsx_path}")
    print("Reading Catalog Clean sheet...")
    import_from_catalog_clean(xlsx_path)
    print("\nUpdate complete!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 update_from_1c.py <path_to_FINAL.xlsx>")
        print("\nThe file must contain a 'Catalog Clean' sheet.")
        print("Run producer_script.py first to populate the producers column.")
        sys.exit(1)

    update_database(sys.argv[1])
