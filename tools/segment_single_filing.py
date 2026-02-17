"""
Segment a single 10-K filing into individual items.

Usage:
    python tools/segment_single_filing.py --cik 0000320193 --filing 0000320193-23-000077
    python tools/segment_single_filing.py --cik 320193 --filing 0000320193-23-000077

This script reads a cleaned filing and extracts all items (Item 1, 1A, 1B, etc.)
into separate text files in the items directory.
"""

import argparse
from pathlib import Path
from itertools import islice
from etl_10k.config import INTERIM_CLEANED_DIR, INTERIM_ITEMS_DIR
from etl_10k.text.segment import item_segmentation_list


def segment_single_filing(cik: str, filing: str) -> None:
    """
    Segment a single 10-K filing into individual item files.

    Args:
        cik: Company CIK (can be padded or unpadded)
        filing: Accession number (e.g., 0000320193-23-000077)
    """
    # Try both padded and unpadded CIK versions
    cik_clean = str(cik).strip().lstrip('0') or '0'
    cik_padded = cik_clean.zfill(10)

    # Try to find the filing in cleaned_filings directory
    source_path = None
    for cik_variant in [cik_clean, cik_padded]:
        candidate = INTERIM_CLEANED_DIR / cik_variant / '10-K' / filing
        if candidate.exists():
            source_path = candidate
            used_cik = cik_variant
            break

    if source_path is None:
        print(f"❌ Filing not found for CIK {cik}, accession {filing}")
        print(f"   Tried paths:")
        print(f"   - {INTERIM_CLEANED_DIR / cik_clean / '10-K' / filing}")
        print(f"   - {INTERIM_CLEANED_DIR / cik_padded / '10-K' / filing}")
        return

    # Check if full-submission.txt exists
    filepath = source_path / "full-submission.txt"
    if not filepath.exists():
        print(f"❌ full-submission.txt not found at {filepath}")
        return

    print(f"📂 Found filing: {source_path}")
    print(f"📄 Reading: {filepath}")

    try:
        # Get item segmentation structure (with verbose output for debugging)
        item_segmentation = item_segmentation_list(filepath, verbose=True)
        page_list = [i['item_line'] for i in item_segmentation]
        page_list.append(11849)  # End marker

        print(f"✂️  Found {len(item_segmentation)} items to segment")

        # Create output directory in items folder
        output_dir = INTERIM_ITEMS_DIR / used_cik / '10-K' / filing
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"💾 Output directory: {output_dir}")
        print()

        # Extract and save each item
        items_saved = 0
        for n, i in enumerate(item_segmentation):
            start, end = page_list[n], page_list[n + 1]

            with filepath.open("r", encoding="utf-8", errors="replace") as f:
                lines = list(islice(f, start - 1, end - 1))

            chunk = "".join(lines)
            filename = f"item{i['item_num']}.txt"

            full_path = output_dir / filename
            with open(full_path, "w", encoding='utf-8') as f:
                f.write(chunk)

            items_saved += 1
            print(f"  ✓ Saved: {filename} ({len(chunk):,} chars, lines {start}-{end})")

        print()
        print(f"✅ Successfully segmented {items_saved} items for CIK {used_cik} | {filing}")
        print(f"📁 Items saved to: {output_dir}")

    except Exception as e:
        print(f"❌ Error segmenting filing: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description="Segment a single 10-K filing into individual items",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/segment_single_filing.py --cik 320193 --filing 0000320193-23-000077
  python tools/segment_single_filing.py --cik 0000320193 --filing 0000320193-23-000077
        """
    )

    parser.add_argument(
        "--cik",
        required=True,
        help="Company CIK number (can be padded or unpadded)"
    )

    parser.add_argument(
        "--filing",
        required=True,
        help="Accession number (e.g., 0000320193-23-000077)"
    )

    args = parser.parse_args()

    segment_single_filing(args.cik, args.filing)


if __name__ == "__main__":
    main()
