"""
Integrated download and clean pipeline for SEC filings.

This module combines downloading, cleaning, and optional deletion of raw HTML files
to minimize storage requirements. Instead of downloading all raw HTML first (~400 GB),
then cleaning separately, this pipeline processes each CIK in a download → clean → delete
cycle, keeping peak storage to ~1 GB (8 workers in flight).

Usage:
    from etl_10k.edgar.clean_downloader import download_clean_delete

    # Delete raw HTML after cleaning (default)
    download_clean_delete(ciks)

    # Keep raw HTML for debugging
    download_clean_delete(ciks, keep_raw=True)
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from etl_10k.edgar.downloader import download_for_cik
from etl_10k.text.clean import clean_html, print_10X, cleaning_items
from etl_10k.config import RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, MAX_WORKERS_DOWNLOADS
from pathlib import Path
import shutil


def verify_cleaned_file(cleaned_path: Path, min_size_bytes: int = 1000) -> bool:
    """
    Verify cleaned file meets quality criteria before allowing raw deletion.

    Safety checks:
    1. File exists
    2. File size >= 1 KB (reasonable minimum)
    3. File is readable (valid UTF-8)
    4. File has substantial content (not just whitespace)

    Args:
        cleaned_path: Path to cleaned file
        min_size_bytes: Minimum acceptable size (default: 1 KB)

    Returns:
        bool: True if safe to delete raw HTML, False otherwise
    """
    # Check 1: File exists
    if not cleaned_path.exists():
        return False

    # Check 2: File size is reasonable
    try:
        file_size = cleaned_path.stat().st_size
        if file_size < min_size_bytes:
            return False
    except OSError:
        return False

    # Check 3: File is readable and has content
    try:
        with open(cleaned_path, 'r', encoding='utf-8') as f:
            sample = f.read(100)  # Read first 100 chars

            # Check 4: Content is not just whitespace
            if len(sample.strip()) < 50:
                return False

        return True

    except Exception:
        return False


def download_clean_and_delete_for_cik(cik: str, keep_raw: bool = False):
    """
    Download, clean, and optionally delete raw HTML for a single CIK.

    This function performs the full cycle for one CIK:
    1. Download all 10-K filings for the CIK
    2. Clean each filing (HTML → text)
    3. Verify cleaned file quality
    4. Delete raw HTML (if keep_raw=False and verification passes)

    Args:
        cik: Company CIK number (string)
        keep_raw: If True, preserve raw HTML files (default: False)

    Returns:
        tuple: (cik, status, stats_dict)
            cik: The CIK processed
            status: "ok", "not_found", or "error"
            stats: {"downloaded": int, "cleaned": int, "deleted": int, "errors": int}
    """
    stats = {"downloaded": 0, "cleaned": 0, "deleted": 0, "errors": 0}

    # Step 1: Download all filings for this CIK
    cik, download_status, download_err = download_for_cik(cik)

    if download_status != "ok":
        return cik, download_status, stats

    # Step 2: Clean and optionally delete each filing
    try:
        raw_cik_path = RAW_EDGAR_DIR / cik / "10-K"
        cleaned_cik_path = INTERIM_CLEANED_DIR / cik / "10-K"

        if not raw_cik_path.exists():
            return cik, "error", stats

        # Process each accession (filing)
        for accession_dir in raw_cik_path.iterdir():
            if not accession_dir.is_dir():
                continue

            raw_file = accession_dir / "full-submission.txt"
            if not raw_file.exists():
                continue

            stats["downloaded"] += 1

            # Step 3: Clean the filing
            try:
                # Read raw HTML
                with open(raw_file, 'r', encoding='utf-8') as f:
                    file_content = f.read()

                # Apply cleaning pipeline (same as step 03)
                cleaned_content = cleaning_items(clean_html(file_content))

                # Write cleaned file
                cleaned_dir = cleaned_cik_path / accession_dir.name
                cleaned_dir.mkdir(parents=True, exist_ok=True)
                cleaned_file = cleaned_dir / "full-submission.txt"
                print_10X(cleaned_file, cleaned_content)

                stats["cleaned"] += 1

                # Step 4: Verify and delete (if keep_raw=False)
                if not keep_raw:
                    if verify_cleaned_file(cleaned_file):
                        try:
                            shutil.rmtree(accession_dir)
                            stats["deleted"] += 1
                        except OSError as e:
                            print(f"  ⚠️  Deletion failed: {accession_dir.name}: {e}")
                            stats["errors"] += 1
                    else:
                        print(f"  ⚠️  Verification failed, keeping: {accession_dir.name}")
                        stats["errors"] += 1

            except Exception as e:
                print(f"  ❌ Cleaning error ({accession_dir.name}): {type(e).__name__}")
                stats["errors"] += 1
                # Keep raw file on error
                continue

        return cik, "ok", stats

    except Exception as e:
        print(f"  ❌ Fatal error (CIK {cik}): {type(e).__name__}: {e}")
        return cik, "error", stats


def download_clean_delete(ciks, keep_raw: bool = False):
    """
    Download, clean, and optionally delete raw HTML for multiple CIKs.

    This is the main entry point for the integrated pipeline. It manages parallel
    workers that each perform the download → clean → delete cycle for their assigned CIKs.

    Args:
        ciks: Iterable of CIK strings to process
        keep_raw: If True, preserve raw HTML files (default: False for storage savings)

    Storage impact:
        - With keep_raw=False: Peak ~1 GB (8 workers in flight), final ~40 GB (cleaned only)
        - With keep_raw=True: Peak ~440 GB (all raw + cleaned)
    """
    ciks_list = list(ciks)
    total = len(ciks_list)

    print(f"Found {total} unique CIKs")
    print(f"Keep raw HTML: {keep_raw}")
    print(f"Workers: {MAX_WORKERS_DOWNLOADS}")
    print("="*60)

    # Track statistics
    stats_summary = {"ok": 0, "not_found": 0, "error": 0}
    total_downloaded = 0
    total_cleaned = 0
    total_errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOADS) as executor:
        futures = {
            executor.submit(download_clean_and_delete_for_cik, cik, keep_raw): cik
            for cik in ciks_list
        }

        for idx, future in enumerate(as_completed(futures), start=1):
            cik, status, stats = future.result()

            # Print progress
            kept_msg = "(kept)" if keep_raw else f"→ deleted {stats['deleted']}"
            print(f"[{idx}/{total}] CIK {cik}: {status} | "
                  f"Files: {stats['downloaded']} → {stats['cleaned']} {kept_msg}")

            stats_summary[status] += 1
            total_downloaded += stats.get("downloaded", 0)
            total_cleaned += stats.get("cleaned", 0)
            total_errors += stats.get("errors", 0)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"CIKs processed: {total}")
    print(f"  ✓ Success: {stats_summary['ok']}")
    print(f"  ⚠ Not found: {stats_summary['not_found']}")
    print(f"  ❌ Errors: {stats_summary['error']}")
    print(f"\nFilings downloaded: {total_downloaded}")
    print(f"Filings cleaned: {total_cleaned}")
    if total_errors > 0:
        print(f"Filing errors: {total_errors}")