#!/usr/bin/env python3
"""Bulk import all historical balance files to the live API.

Usage:
    python scripts/bulk_import_balances.py /path/to/FInances/

This reads all .xls files from the UZS/ and USD/ subdirectories
and uploads them to the /api/finance/bulk-import endpoint.

Can also be run directly against the database (offline mode):
    DATABASE_PATH=./data/catalog.db python scripts/bulk_import_balances.py /path/to/FInances/ --offline
"""
import os
import sys
import glob
import argparse


def collect_files(base_dir):
    """Collect all .xls files from UZS/ and USD/ subdirectories."""
    files = []
    for subdir in ['UZS', 'USD']:
        pattern = os.path.join(base_dir, subdir, '*.xls')
        found = sorted(glob.glob(pattern))
        files.extend(found)
    return files


def import_offline(files):
    """Import files directly via Python (no server needed)."""
    # Add project root to path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)

    from backend.database import init_db
    from backend.services.import_balances import bulk_import_balances

    init_db()

    file_list = []
    for filepath in files:
        with open(filepath, 'rb') as f:
            file_list.append((os.path.basename(filepath), f.read()))

    print(f"Importing {len(file_list)} files...")
    result = bulk_import_balances(file_list)

    print(f"\nResults:")
    print(f"  Files processed: {result['files_processed']}")
    print(f"  Files failed: {result['files_failed']}")
    print(f"  Total inserted: {result['total_inserted']}")
    print(f"  Total updated: {result['total_updated']}")
    print(f"  Total matched to app: {result['total_matched']}")
    print(f"  DB total clients: {result['db_total_clients']}")
    print(f"  DB total periods: {result['db_total_periods']}")

    if result['errors']:
        print(f"\nErrors:")
        for e in result['errors']:
            print(f"  {e['file']}: {e['error']}")

    print(f"\nPer-file breakdown:")
    for r in result['results']:
        print(f"  {r['currency']} {r['period']}: {r['clients']} clients, {r['matched']} matched — {r['file']}")

    return result


def import_online(files, api_url, admin_key=None):
    if admin_key is None:
        admin_key = os.getenv("ADMIN_API_KEY") or "rassvet2026"
    """Import files via the live API endpoint."""
    try:
        import httpx
    except ImportError:
        print("httpx not installed. Install with: pip install httpx")
        sys.exit(1)

    file_tuples = []
    for filepath in files:
        with open(filepath, 'rb') as f:
            file_tuples.append(('files', (os.path.basename(filepath), f.read(), 'application/vnd.ms-excel')))

    print(f"Uploading {len(file_tuples)} files to {api_url}...")

    with httpx.Client(timeout=120) as client:
        resp = client.post(
            f"{api_url}/api/finance/bulk-import",
            files=file_tuples,
            data={"admin_key": admin_key},
        )
        result = resp.json()

    if not result.get("ok"):
        print(f"Error: {result.get('error', 'Unknown')}")
        return result

    print(f"\nResults:")
    print(f"  Files processed: {result['files_processed']}")
    print(f"  Files failed: {result['files_failed']}")
    print(f"  Total inserted: {result['total_inserted']}")
    print(f"  Total updated: {result['total_updated']}")
    print(f"  Total matched to app: {result['total_matched']}")
    print(f"  DB total clients: {result['db_total_clients']}")
    print(f"  DB total periods: {result['db_total_periods']}")

    if result.get('errors'):
        print(f"\nErrors:")
        for e in result['errors']:
            print(f"  {e['file']}: {e['error']}")

    return result


def main():
    parser = argparse.ArgumentParser(description='Bulk import balance files')
    parser.add_argument('directory', help='Path to FInances/ directory')
    parser.add_argument('--offline', action='store_true',
                       help='Import directly via Python (no server needed)')
    parser.add_argument('--api-url', default='https://telegram-catalog-production.up.railway.app',
                       help='API base URL for online mode')
    args = parser.parse_args()

    files = collect_files(args.directory)
    if not files:
        print(f"No .xls files found in {args.directory}/UZS/ or {args.directory}/USD/")
        sys.exit(1)

    print(f"Found {len(files)} files:")
    for f in files:
        print(f"  {os.path.basename(f)}")

    if args.offline:
        import_offline(files)
    else:
        import_online(files, args.api_url)


if __name__ == '__main__':
    main()
