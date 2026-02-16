from etl_10k.config import CIK_LIST, RAW_CIKS_DIR, MAX_WORKERS
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_unique_ciks():
    """
    Load a list of CIKs from the Excel file specified in `CIK_LIST`.
    """
    df = pd.read_csv(CIK_LIST)
    ciks = df["CIK"].astype(str).str.strip()
    return ciks.tolist()

def _load_ciks(args):
    if args.cik:
        return [args.cik.strip()]
    elif args.ciks:
        return [x.strip() for x in args.ciks.split(",") if x.strip()]
    else:
        return None

def inputLetter():
    """
    Prompt the user to choose between using a saved CIK list or entering a single CIK.

    The function repeatedly prompts until the user enters either:
      - 'l' (use list), or
      - 't' (enter ticker/CIK manually).
    """
    letter = input("Select List (L) or Enter Ticker (T)...").lower()
    while letter != 'l' and letter != 't':
        letter = input("Invalid... enter L or T...").lower()
    return letter

def load_master_to_dataframe(year: int, qtr: int) -> pd.DataFrame:
    """
    Download master.idx for a given year/quarter and return it as a DataFrame
    with columns: CIK, Company Name, Form Type.
    """
    from io import StringIO

    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"

    HEADERS = {
    "User-Agent": "Name name@domain.com",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    }

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    text = r.text

    # Find where the actual data starts (after the header lines)
    lines = text.splitlines()
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith('---'):  # The separator line before data
            data_start = i + 1
            break

    # Join the data lines back together
    data_text = '\n'.join(lines[data_start:])

    # Use pandas to parse the pipe-delimited data directly
    df = pd.read_csv(
        StringIO(data_text),
        sep='|',
        names=['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename'],
        dtype={'CIK': str},
        usecols=['CIK', 'Company Name', 'Form Type'],  # Only load columns we need
        na_filter=False,  # Faster when we don't need NaN detection
        engine='c'  # Use the faster C parser
    )

    # Filter for 10-K forms
    return df[df['Form Type'] == '10-K']

def cik_list_builder(start_year, end_year, max_workers=MAX_WORKERS):
    """
    Build and save a master list of unique 10-K CIKs over a range of years.

    For each year in [start_year, end_year) and each quarter (1-4), the function
    downloads the SEC `master.idx` index in parallel, filters for 10-K filings,
    concatenates all results, removes duplicate CIKs, and writes the final list to
    `RAW_CIKS_DIR / "cik_list.csv"`.

    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (exclusive)
        max_workers: Maximum number of parallel downloads (default: 8)
    """
    # Create list of all (year, quarter) tasks
    tasks = [
        (year, qtr)
        for year in range(start_year, end_year)
        for qtr in range(1, 5)
    ]

    cik_df = []

    # Download quarters in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(load_master_to_dataframe, year, qtr): (year, qtr)
            for year, qtr in tasks
        }

        # Collect results as they complete
        for future in as_completed(future_to_task):
            year, qtr = future_to_task[future]
            try:
                df = future.result()
                print(f"Downloaded {year}, QTR {qtr} ({len(df)} 10-K filings)")
                cik_df.append(df)
            except Exception as e:
                print(f"Error downloading {year} Q{qtr}: {e}")

    cik_df = pd.concat(cik_df, ignore_index=True)
    cik_df = cik_df.drop_duplicates(subset='CIK', keep='first').sort_values('CIK')

    CIK_LIST = RAW_CIKS_DIR / "cik_list.csv"
    cik_df.to_csv(CIK_LIST, index=False)
