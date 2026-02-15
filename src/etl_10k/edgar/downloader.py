from concurrent.futures import ThreadPoolExecutor, as_completed
from etl_10k.config import FORM, START_DATE, RAW_DIR
from etl_10k.config import MAX_WORKERS_DOWNLOADS as MAX_WORKERS
from sec_edgar_downloader import Downloader
import time

# Create a single shared downloader instance to reuse HTTP connections
# IMPORTANT: Replace with your actual company name and email address
# The SEC requires proper identification and may ban IPs with generic user-agents
_downloader = Downloader("YourActualCompanyName", "your.email@yourdomain.com", str(RAW_DIR))

def download_for_cik(cik: str):
    """
    Download SEC filings for a given CIK using `sec-edgar-downloader`.

    Includes a small delay between requests to ensure compliance with SEC's
    10 requests per second rate limit and avoid IP bans.
    """
    print(f"Starting {FORM} for CIK {cik}")
    try:
        # Small delay to help respect SEC rate limits (10 req/sec = 0.1s between requests)
        time.sleep(0.12)  # Slightly conservative to account for variance
        _downloader.get(FORM, cik, after=START_DATE)
        return cik, "ok", None
    except ValueError as e:
        return cik, "not_found", str(e)
    except Exception as e:
        return cik, "error", str(e)

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




