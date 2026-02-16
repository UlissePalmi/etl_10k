"""
Integrated download and clean pipeline for SEC filings.

This module combines downloading, cleaning, and optional deletion of raw HTML files.

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
from etl_10k.config import RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, MAX_WORKERS_DOWNLOADS, MAX_WORKERS
from pathlib import Path
from queue import Queue
import shutil
import threading
import time


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


def clean_and_delete_single_filing(cik: str, accession_dir: Path, keep_raw: bool = False):
    """
    Clean and optionally delete raw HTML for a single 10-K filing.

    Args:
        cik: Company CIK number (string)
        accession_dir: Path to the accession directory containing the filing
        keep_raw: If True, preserve raw HTML files (default: False)

    Returns:
        tuple: (cik, accession, status, stats_dict)
            cik: The CIK processed
            accession: The accession number
            status: "ok" or "error"
            stats: {"cleaned": int, "deleted": int}
    """
    stats = {"cleaned": 0, "deleted": 0}
    accession = accession_dir.name

    try:
        # Zero-pad CIK to match sec-edgar-downloader's directory naming
        padded_cik = str(cik).zfill(10)
        cleaned_cik_path = INTERIM_CLEANED_DIR / padded_cik / "10-K"

        raw_file = accession_dir / "full-submission.txt"
        if not raw_file.exists():
            return cik, accession, "error", stats

        # Read raw HTML
        with open(raw_file, 'r', encoding='utf-8') as f:
            file_content = f.read()

        # Apply cleaning pipeline
        cleaned_content = cleaning_items(clean_html(file_content))

        # Write cleaned file
        cleaned_dir = cleaned_cik_path / accession
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        cleaned_file = cleaned_dir / "full-submission.txt"
        print_10X(cleaned_file, cleaned_content)

        stats["cleaned"] = 1

        # Verify and delete (if keep_raw=False)
        if not keep_raw:
            if verify_cleaned_file(cleaned_file):
                try:
                    shutil.rmtree(accession_dir)
                    stats["deleted"] = 1

                    # Clean up empty parent directories
                    try:
                        raw_cik_path = accession_dir.parent
                        if raw_cik_path.exists() and not any(raw_cik_path.iterdir()):
                            raw_cik_path.rmdir()

                        cik_folder = raw_cik_path.parent
                        if cik_folder.exists() and not any(cik_folder.iterdir()):
                            cik_folder.rmdir()
                    except OSError:
                        pass

                except OSError as e:
                    print(f"  ⚠️  Deletion failed: {accession}: {e}")
                    return cik, accession, "error", stats
            else:
                print(f"  ⚠️  Verification failed, keeping: {accession}")
                return cik, accession, "error", stats

        return cik, accession, "ok", stats

    except Exception as e:
        print(f"  ❌ Cleaning error ({accession}): {type(e).__name__}")
        return cik, accession, "error", stats


def download_clean_delete(ciks, keep_raw: bool = False):
    """
    Download, clean, and optionally delete raw HTML for multiple CIKs using producer-consumer pattern.

    Uses two separate thread pools for optimal resource utilization:
    - Download pool (8 workers): Network I/O bound, respects SEC rate limits
    - Cleaning pool (MAX_WORKERS): CPU bound, processes files as they're downloaded

    This allows downloads and cleaning to happen in parallel, significantly improving throughput.

    Args:
        ciks: Iterable of CIK strings to process
        keep_raw: If True, preserve raw HTML files (default: False for storage savings)
    """
    ciks_list = list(ciks)
    total = len(ciks_list)

    print(f"Found {total} unique CIKs")
    print(f"Keep raw HTML: {keep_raw}")
    print(f"Download workers: {MAX_WORKERS_DOWNLOADS} | Cleaning workers: {MAX_WORKERS}")
    print("="*60)

    # Shared queue for downloaded CIKs ready for cleaning
    # -- Like a conveyer belt: producer adds downloaded cik and consumer removes cleaned ciks (ordered list)  
    download_queue = Queue()
    '''
    queue.put(cik)           # Producer adds
    queue.get()              # Consumer takes
    queue.task_done()        # Signal "I'm done with this item"
    '''

    # Thread-safe statistics tracking
    stats_lock = threading.Lock()
    stats_summary = {"ok": 0, "not_found": 0, "error": 0, "download_ok": 0}
    total_downloaded = 0
    total_cleaned = 0
    total_errors = 0
    completed_count = 0

    # Track queue waiting time
    total_wait_time = 0.0
    wait_count = 0

    # Track download timing (Step 2: actual HTTP request + file I/O)
    total_download_time = 0.0
    download_count = 0

    # Producer: Download worker
    def download_worker(cik):
        """Download files and enumerate filings to add to queue"""
        cik_result, status, download_duration = download_for_cik(cik)

        # Track download timing for successful downloads
        if status == "ok":
            with stats_lock:
                nonlocal total_download_time, download_count
                total_download_time += download_duration
                download_count += 1

            # Enumerate all downloaded filings and add each to queue
            padded_cik = str(cik_result).zfill(10)
            raw_cik_path = RAW_EDGAR_DIR / padded_cik / "10-K"

            if raw_cik_path.exists():
                filings_added = 0
                for accession_dir in raw_cik_path.iterdir():
                    if accession_dir.is_dir():
                        raw_file = accession_dir / "full-submission.txt"
                        if raw_file.exists():
                            download_queue.put((cik_result, accession_dir))
                            filings_added += 1

                return cik_result, "download_ok", filings_added, download_duration
            else:
                return cik_result, "error", 0, 0.0
        elif status == "not_found":
            return cik_result, "not_found", 0, 0.0
        else:
            return cik_result, "error", 0, 0.0

    # Consumer: Cleaning worker
    def clean_worker():
        """Process individual filings from the download queue"""
        while True:    # When a worker finishes running a thread it keeps going in the while loop and picks up another filing
            # Measure queue waiting time
            wait_start = time.time()
            item = download_queue.get() # Thread sleeps until something is added to the queue
            wait_end = time.time()
            wait_duration = wait_end - wait_start

            # Track waiting time (before poison pill check to capture all waits)
            with stats_lock:
                nonlocal total_wait_time, wait_count
                total_wait_time += wait_duration
                wait_count += 1

            if item is None:  # Poison pill to signal shutdown
                download_queue.task_done()
                break

            cik, accession_dir = item

            try:
                cik_result, accession, status, stats = clean_and_delete_single_filing(cik, accession_dir, keep_raw)

                with stats_lock:
                    nonlocal completed_count, total_cleaned, total_errors
                    completed_count += 1

                    if status == "ok":
                        stats_summary["ok"] += 1
                    else:
                        stats_summary["error"] += 1

                    total_cleaned += stats.get("cleaned", 0)
                    total_errors += (1 if status == "error" else 0)

                    # Print progress
                    kept_msg = "(kept)" if keep_raw else f"→ deleted {stats.get('deleted', 0)}"
                    status_icon = "✓" if status == "ok" else "❌"
                    print(f"[{completed_count}] {status_icon} CIK {cik_result} | {accession}: {status} | "
                          f"cleaned={stats.get('cleaned', 0)} {kept_msg}")

            except Exception as e:
                with stats_lock:
                    completed_count += 1
                    stats_summary["error"] += 1
                    total_errors += 1
                    print(f"[{completed_count}] ❌ CIK {cik}: error | {type(e).__name__}: {e}")

            finally:
                download_queue.task_done()

    # Start cleaning workers (consumers)
    cleaning_threads = []
    # Creates a thread for each worker
    for _ in range(MAX_WORKERS): 
        t = threading.Thread(target=clean_worker, daemon=True) 
        t.start()
        cleaning_threads.append(t)

    # Start download workers (producers)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOADS) as download_executor:
        download_futures = {
            download_executor.submit(download_worker, cik): cik
            for cik in ciks_list
        }

        # Track download completion
        ciks_processed = 0
        for future in as_completed(download_futures):
            cik, status, filings_count, duration = future.result()
            ciks_processed += 1

            if status == "download_ok":
                with stats_lock:
                    stats_summary["download_ok"] += 1
                    total_downloaded += filings_count
                    print(f"[{ciks_processed}/{total}] CIK {cik}: downloaded {filings_count} filings in {duration:.2f}s → queued for cleaning")
            elif status == "not_found":
                with stats_lock:
                    stats_summary["not_found"] += 1
                    print(f"[{ciks_processed}/{total}] CIK {cik}: not_found | No filings available")
            elif status == "error":
                with stats_lock:
                    stats_summary["error"] += 1
                    print(f"[{ciks_processed}/{total}] CIK {cik}: error | Download failed")

    # Wait for all cleaning to finish
    download_queue.join()

    # Signal cleaning workers to shut down
    for _ in range(MAX_WORKERS):
        download_queue.put(None)  # Poison pill

    for t in cleaning_threads:
        t.join()

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"CIKs processed: {total}")
    print(f"  ✓ Downloaded successfully: {stats_summary['download_ok']}")
    print(f"  ⚠ Not found: {stats_summary['not_found']}")
    print(f"  ❌ Download errors: {stats_summary.get('error', 0)}")
    print(f"\nFilings downloaded: {total_downloaded}")
    print(f"Filings cleaned successfully: {stats_summary['ok']}")
    print(f"Filings cleaned: {total_cleaned}")
    if total_errors > 0:
        print(f"Filing cleaning errors: {total_errors}")

    # Print download timing statistics
    print(f"\n{'='*60}")
    print("DOWNLOAD TIMING ANALYSIS (Step 2: HTTP + File I/O)")
    print(f"{'='*60}")
    print(f"Total download time: {total_download_time:.2f}s")
    print(f"Number of successful downloads: {download_count}")
    if download_count > 0:
        avg_download_time = total_download_time / download_count
        print(f"Average download time per CIK: {avg_download_time:.2f}s")
    print(f"Number of download workers: {MAX_WORKERS_DOWNLOADS}")

    # Print queue waiting statistics
    print(f"\n{'='*60}")
    print("QUEUE WAITING TIME ANALYSIS")
    print(f"{'='*60}")
    print(f"Total queue wait time: {total_wait_time:.2f}s")
    print(f"Number of queue.get() calls: {wait_count}")
    if wait_count > 0:
        avg_wait = total_wait_time / wait_count
        print(f"Average wait per get(): {avg_wait:.4f}s ({avg_wait*1000:.2f}ms)")
    print(f"Number of cleaning workers: {MAX_WORKERS}")
    if wait_count > 0:
        total_possible_work_time = total_wait_time
        print(f"Total idle time across all workers: {total_possible_work_time:.2f}s")
        print(f"Average idle time per worker: {total_possible_work_time/MAX_WORKERS:.2f}s")