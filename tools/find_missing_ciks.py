#!/usr/bin/env python3
"""
Find CIKs that have been processed through item extraction (step 4)
but not through cleaning (step 3).

Usage:
    python tools/find_missing_ciks.py
    python tools/find_missing_ciks.py > missing_ciks.txt
"""

from pathlib import Path
import sys

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl_10k.config import INTERIM_ITEMS_DIR, INTERIM_CLEANED_DIR


def find_missing_ciks():
    """
    Returns CIKs that exist in items folder but not in cleaned_filings folder.
    """
    # Get all CIK directories in items folder
    items_ciks = set()
    if INTERIM_ITEMS_DIR.exists():
        items_ciks = {d.name for d in INTERIM_ITEMS_DIR.iterdir() if d.is_dir()}

    # Get all CIK directories in cleaned_filings folder
    cleaned_ciks = set()
    if INTERIM_CLEANED_DIR.exists():
        cleaned_ciks = {d.name for d in INTERIM_CLEANED_DIR.iterdir() if d.is_dir()}

    # Find missing CIKs (in items but not in cleaned_filings)
    missing = sorted(items_ciks - cleaned_ciks)

    return missing, len(items_ciks), len(cleaned_ciks)


if __name__ == "__main__":
    missing, total_items, total_cleaned = find_missing_ciks()

    print(f"CIKs with items: {total_items}")
    print(f"CIKs with cleaned filings: {total_cleaned}")
    print(f"CIKs missing cleaned filings: {len(missing)}\n")

    if missing:
        print("Missing CIKs (comma-separated for CLI use):")
        print(",".join(missing))
        print(f"\nTotal: {len(missing)} CIKs")
    else:
        print("All CIKs have been cleaned!")
