from etl_10k.pipeline import steps
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and clean SEC filings (integrated pipeline)"
    )
    parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep raw HTML files after cleaning (default: delete to save space)"
    )
    parser.add_argument(
        "--cik",
        type=str,
        help="Single CIK to process (optional)"
    )
    parser.add_argument(
        "--ciks",
        type=str,
        help="Comma-separated list of CIKs (optional)"
    )

    args = parser.parse_args()

    # Parse CIKs
    ciks = None
    if args.cik:
        ciks = [args.cik]
    elif args.ciks:
        ciks = [c.strip() for c in args.ciks.split(",")]

    steps.step_02_download_filings(ciks=ciks, keep_raw=args.keep_raw)