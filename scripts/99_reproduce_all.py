from __future__ import annotations
from etl_10k.config import ensure_project_dirs, CIK_LIST
from etl_10k.edgar.cik_index import _load_ciks
from etl_10k.pipeline import steps as s

"""
Entry point for reproducing the full pipeline end-to-end.

Runs the SEC download → cleaning → Item 1A extraction → feature construction →
returns merge → panel build → model estimation steps, with optional step ranges
controlled by CLI arguments.
"""

def main():
    args = s._parse_args()
    ensure_project_dirs()
    ciks = _load_ciks(args)

    steps = {
        0: ("build_universe", lambda: s.step_00_build_universe(args.start_year, args.end_year)),
        1: ("pull_returns", s.step_01_pull_returns),
        2: ("download_filings", lambda: s.step_02_download_filings(ciks, keep_raw=args.keep_raw)),
        3: ("clean_filings", lambda: s.step_03_clean_filings(ciks, args.delete)),
        4: ("extract_item1a", lambda: s.step_04_extract_item1a(ciks)),
        5: ("compute_features", lambda: s.step_05_compute_features(ciks, args.delete)),
        6: ("build_panel", s.step_06_build_panel)
    }

    print(f"Running pipeline for: {('ALL CIKs' if ciks is None else f'{len(ciks)} CIK(s)')}")

    if args.from_step > args.to_step:
        raise ValueError("--from-step must be <= --to-step")

    for i in range(args.from_step, args.to_step + 1):
        name, fn = steps[i]
        print(f"\n=== Step {i}: {name} ===")
        try:
            fn()
        except Exception as e:
            raise RuntimeError(f"Failed at step {i}: {name}") from e

    print("\nDone.")

if __name__ == "__main__":
    main()
