from concurrent.futures import ThreadPoolExecutor, as_completed
from etl_10k.config import FORM, START_DATE, RAW_DIR, SEC_REQUESTS_PER_SECOND
from etl_10k.config import MAX_WORKERS_DOWNLOADS as MAX_WORKERS
from sec_edgar_downloader import Downloader
from etl_10k.edgar.rate_limiter import TokenBucketRateLimiter
import time

# Create a single shared downloader instance to reuse HTTP connections
# IMPORTANT: Replace with your actual company name and email address
# The SEC requires proper identification and may ban IPs with generic user-agents
_downloader = Downloader("YourActualCompanyName", "your.email@yourdomain.com", str(RAW_DIR))

# Shared rate limiter coordinating across ALL download workers
# Uses configured rate from config.py (default: 9.5 req/sec for safety margin)
# Capacity = 1.0 prevents burst accumulation (maintains steady 9.5 req/sec)
_rate_limiter = TokenBucketRateLimiter(rate=SEC_REQUESTS_PER_SECOND, capacity=1.0)

def download_for_cik(cik: str):
    """
    Download SEC filings for a given CIK using `sec-edgar-downloader`.

    Uses a shared rate limiter to ensure compliance with SEC's 10 requests per
    second rate limit across all download workers and avoid IP bans.

    Returns:
        tuple: (cik, status, duration_seconds)
    """
    try:
        # Step 1: Acquire token from shared rate limiter
        _rate_limiter.acquire() # Acts like a traffic light that slows down all threads to prevent too many requests at once

        # Step 2: Measure actual download time (HTTP request + file I/O)
        download_start = time.time()
        print(f"Starting {FORM} for CIK {cik}")
        _downloader.get(FORM, cik, after=START_DATE)
        download_duration = time.time() - download_start

        return cik, "ok", download_duration
    except ValueError:
        return cik, "not_found", 0.0
    except Exception:
        return cik, "error", 0.0

def download(ciks):
    """
    Download filings for a collection of CIKs using multithreading.

    The function submits one task per CIK to a ThreadPoolExecutor and consumes
    results as tasks finish using `as_completed`. It prints a progress counter
    and summarizes not-found CIKs and errors at the end.
    """
    total = len(ciks)
    print(f"Found {total} unique CIKs")

    not_found = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_for_cik, cik): cik for cik in ciks}

        # as_completed lets them run in parallel; we consume results as they finish
        for idx, future in enumerate(as_completed(futures), start=1):
            cik, status, err = future.result()
            print(f"[{idx}/{total}] CIK {cik}: {status}")
            if status == "not_found":
                not_found.append(cik)
            elif status == "error":
                errors.append((cik, err))

    if not_found:
        print("\nCIKs not found:")
        for cik in not_found:
            print(" ", cik)

    if errors:

        print("\nCIKs with errors:")
        for cik, err in errors:
            print(f" {cik}: {err}")
    return




