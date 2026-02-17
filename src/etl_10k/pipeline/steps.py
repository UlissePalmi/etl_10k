from etl_10k.config import ensure_project_dirs, RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, FEATURES_FIELDS, FEATURES_FILE, INTERIM_ITEMS_DIR, FINAL_DATASET, RETURNS_FILE, CIK_LIST
from etl_10k.edgar import cik_index as cl, downloader as sd
from etl_10k.text import clean as hc, segment as si, tokenizer as sm
from etl_10k.wrds import crsp_returns as cr
from etl_10k.datasets import build_panel as bp
from typing import Iterable, List, Optional
from pathlib import Path
import pandas as pd
import argparse
import shutil
import csv

def _digits_only(x: str) -> str:
    return "".join(ch for ch in x if ch.isdigit())

def _resolve_cik_dirs(base_dir: Path, ciks: Optional[Iterable[str]]) -> List[str]:
    """
    Resolve which CIK directory names to process under `base_dir`.
    - If ciks is None: return all subdirectory names.
    - If provided: try both padded/unpadded representations and pick the one that exists.
    """
    if ciks is None:
        return sorted([p.name for p in base_dir.iterdir() if p.is_dir()])
    #print(ciks)
    resolved = []
    for cik in ciks:
        raw = str(cik).strip()
        digits = _digits_only(raw)

        candidates = []
        if digits:
            candidates.extend([digits, digits.zfill(10), digits.lstrip("0") or digits])
        else:
            candidates.append(raw)

        picked = None
        for cand in dict.fromkeys(candidates):  # unique, preserve order
            if (base_dir / cand).exists():
                picked = cand
                break

        # Fallback: if nothing exists yet (e.g., first run), keep padded form for consistency
        if picked is None:
            picked = digits.zfill(10) if digits else raw

        resolved.append(picked)
    return resolved
    
def _parse_args():
    """
    Parse CLI arguments for running the end-to-end pipeline.

    Supports selecting a single CIK or a comma-separated list, year range,
    and a step interval to run.
    """
    p = argparse.ArgumentParser(
        description="Reproduce the full risk-factor predictability pipeline."
    )

    g = p.add_mutually_exclusive_group()
    g.add_argument("--cik", type=str, help="Single CIK (digits). Example: 320193")
    g.add_argument("--ciks", type=str, help="Comma-separated CIKs. Example: 320193,789019")

    p.add_argument("--start-year", type=int, default=2006)
    p.add_argument("--end-year", type=int, default=2026)

    p.add_argument("--from-step", type=int, default=0, choices=range(0, 8))
    p.add_argument("--to-step", type=int, default=7, choices=range(0, 8))
    p.add_argument("--delete", type=bool, default=False)
    p.add_argument(
        "--keep-raw",
        action="store_true",
        help="Keep raw HTML files after cleaning in step 2 (default: delete to save space)"
    )

    return p.parse_args()

def step_00_build_universe(start_year: int = 2006 , end_year: int = 2026) -> None:
    """
    Create the CIK universe used for the pipeline.

    Builds `cik_list.csv` from SEC index files if it does not already exist.
    """
    ensure_project_dirs()
    print("Starting cik_list.csv file generation... ")
    cl.cik_list_builder(start_year, end_year) if not CIK_LIST.exists() else None

def step_01_pull_returns() -> None:
    """
    Pull monthly return data from WRDS/CRSP for the CIK universe.

    Saves the combined return panel to `RETURNS_FILE`.
    """    
    cr.df_with_returns()
    cr.update_cik_list()

def step_02_download_filings(ciks: Optional[Iterable[str]] = None, keep_raw: bool = False):
    """
    Download and clean SEC filings for the requested CIKs.

    This integrated pipeline downloads, cleans, and optionally deletes raw HTML
    in a single pass to minimize storage requirements (~400 GB → ~1 GB peak).

    Args:
        ciks: CIKs to download (None = all from cik_list.csv)
        keep_raw: If True, preserve raw HTML after cleaning (default: False)

    If keep_raw=False (default), raw HTML files are automatically deleted after
    successful cleaning and verification, saving ~400 GB of storage.
    """
    if ciks is None:
        ciks = cl.load_unique_ciks()

    # Use integrated download+clean+delete pipeline
    from etl_10k.edgar.clean_downloader import download_clean_delete
    download_clean_delete(ciks, keep_raw=keep_raw)

def step_03_clean_filings(ciks: Optional[Iterable[str]] = None, delete: bool = False) -> None:
    """
    Clean downloaded SEC filings into standardized text files.

    If `ciks` is None, processes all CIK folders found in the raw directory.
    """
    print(ciks)
    ciks_dirs = _resolve_cik_dirs(RAW_EDGAR_DIR, ciks)
    hc.clean_worker(ciks_dirs)
    print(delete)
    if delete == True:
        deleted = 0
        for p in RAW_EDGAR_DIR.iterdir():
            if p.is_dir():
                shutil.rmtree(p)
                deleted += 1
        print(f"Deleted folders: {deleted}")
    
def step_04_segment_items(ciks: Optional[Iterable[str]] = None) -> None:
    """
    Segment all items from cleaned filings.

    If `ciks` is None, processes all CIK folders found in the cleaned directory.
    """
    ciks_dirs = _resolve_cik_dirs(INTERIM_CLEANED_DIR, ciks)
    si.try_exercize(ciks_dirs)
    
def step_05_compute_features(ciks: Optional[Iterable[str]] = None, delete: bool = False) -> None:
    """
    Compute sentiment features from extracted Item 1A text.

    Writes row-level results into `FEATURES_FILE`.
    """
    ciks_dirs = _resolve_cik_dirs(INTERIM_ITEMS_DIR, ciks)
    #print(ciks_dirs)
    with open(FEATURES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FEATURES_FIELDS)
        writer.writeheader()
        sm.concurrency_runner(writer, ciks_dirs)    

    if delete == True:
        deleted = 0
        for p in INTERIM_CLEANED_DIR.iterdir():
            if p.is_dir():
                shutil.rmtree(p)
                deleted += 1
        for p in INTERIM_ITEMS_DIR.iterdir():
            if p.is_dir():
                shutil.rmtree(p)
                deleted += 1
        print(f"Deleted folders: {deleted}")


def step_06_build_panel() -> None:
    """
    Merge text features with returns to create the final modeling dataset.

    Produces `FINAL_DATASET` with past/future window returns added.
    """
    sim_df, return_df = bp.datatype_setup(pd.read_csv(FEATURES_FILE), pd.read_csv(RETURNS_FILE))
    #sim_df = bp.feature_engineering(sim_df)

    sim_df = bp.merge_return(sim_df, return_df, months=12, period="past")
    sim_df = bp.merge_return(sim_df, return_df, months=6, period="future")
    print(sim_df)
    sim_df.to_csv(FINAL_DATASET, index=False)
