# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Manager & Environment

This project uses **`uv`** (not pip). Python 3.13 is required (`.python-version`).

```bash
uv sync                  # Install dependencies
uv run python <script>   # Run a script in the venv
uv add <package>         # Add a dependency
```

## Running the Pipeline

The pipeline has 7 steps (0–6). Run via CLI with flexible step selection:

```bash
# Full reproduction
uv run python scripts/99_reproduce_all.py

# Specific steps for specific CIKs
uv run python scripts/99_reproduce_all.py --cik 320193 --from-step 2 --to-step 4

# Key CLI flags
--cik <CIK>             # Single CIK (required for step 5 to prevent memory exhaustion)
--ciks <CIK,CIK,...>    # Comma-separated CIKs
--start-year <YEAR>     # Default: 2006
--end-year <YEAR>       # Default: 2026
--from-step <0-6>       # Step to start from
--to-step <0-6>         # Step to end at
--keep-raw              # Don't delete raw HTML after cleaning (default: delete)
--financials            # Extract financial statement tables during step 2 (default: off)
```

### Step 5 Memory & CPU Safeguard

Step 5 (compute_features) is CPU-bound and memory-intensive. The pipeline:
- **Auto-monitors RAM** and adjusts worker count to prevent memory exhaustion
- **Requires explicit CIK selection** (`--cik` or `--ciks`) to avoid silent "process all 27 CIKs" crashes
- **CPU usage at 100%** is expected — it means workers are fully utilized
- Processing all CIKs in parallel requires 31+ GB RAM; single CIK uses ~2-3 GB

Best practice: process CIKs one at a time or in small batches (2-3 at a time).

Individual step scripts in `scripts/` (e.g., `python scripts/04_extract_item1a.py`).

## Architecture

### Data Flow

```
SEC EDGAR HTML → [Step 2] Download + Clean + Extract financials (integrated)
→ Cleaned text + Financial statements (Excel, with --financials)
→ [Step 4] Item 1A segmentation → Per-filing item text
→ [Step 5] Feature computation → text_features/features.csv
→ [Step 6] Merge with CRSP returns → data/processed/panel/final_dataset.csv
```

Step 0 builds the CIK universe from SEC master indices. Step 1 fetches CRSP stock returns from WRDS (requires WRDS credentials).

### Key Directory Paths (from `config.py`)

- `data/raw/sec-edgar-filings/` — Downloaded 10-K HTML
- `data/interim/cleaned_filings/` — Cleaned text files
- `data/interim/items/<CIK>/10-K/<accession>/item1A.txt` — Extracted Item 1A sections
- `data/interim/financial_statements/<CIK>/<accession>/financial_statements.xlsx` — Financial statement tables (with `--financials`)
- `data/interim/text_features/features.csv` — Computed features (52 columns)
- `data/interim/returns/returns.csv` — CRSP monthly returns
- `data/processed/panel/final_dataset.csv` — Final merged panel

### Parallelization Strategy

- **CPU-bound** (clean, segment, feature computation): `ProcessPoolExecutor` at 90% of CPU cores (`MAX_WORKERS`)
- **I/O-bound** (downloads): `ThreadPoolExecutor` with 15 workers
- **Producer-consumer** in `clean_downloader.py`: download workers feed a queue, cleaning workers consume it — enabling ~400 GB → ~1 GB peak storage via auto-deletion of raw HTML after verification

### SEC Rate Limiting

`edgar/rate_limiter.py` implements a thread-safe token bucket at **9.5 req/sec** (under SEC's 10 req/sec limit). All download workers share one limiter instance.

## Key Modules

| Module | Purpose |
|--------|---------|
| `config.py` | All paths, settings, `FEATURES_FIELDS` (52 output columns), `ensure_project_dirs()` |
| `edgar/clean_downloader.py` | Integrated download + clean + verify + delete pipeline |
| `edgar/extract_financial_statements.py` | Parses SGML `FilingSummary.xml`, extracts R-file HTML tables, converts to Excel |
| `text/clean.py` | HTML → plain text pipeline (XBRL removal, tag stripping, Item heading normalization) |
| `text/segment.py` | Item 1A extraction via regex heading detection + candidate ranking by character span |
| `text/segment_fallback.py` | Fallback segmentation for malformed filings |
| `text/tokenizer.py` | All text features: Jaccard, Levenshtein (token-level), VADER, LM sentiment |
| `text/complexity.py` | Fog Index, Flesch Reading Ease, syllable counting |
| `text/lm_dict.py` | Loughran-McDonald sentiment dictionary loader |
| `datasets/build_panel.py` | Merges features + returns, builds past/future return windows (6m, 12m) |
| `pipeline/steps.py` | Step orchestration + CLI argument parsing |

## Segmentation Algorithm (`text/segment.py`)

The Item 1A extractor achieves ~99.7% accuracy via:
1. Regex scan builds an `item_dict` of all "Item X" headings in the file
2. Estimate number of complete 10-K sections (table of contents + body)
3. Generate candidate item sequences; select the one with the largest character span
4. Append "orphan" items (e.g., Item 16) found only in body, not table of contents
5. Falls back to `segment_fallback.py` on failure

## Text Features (`text/tokenizer.py`)

Features computed between consecutive Item 1A filings (year N vs N−1):
- **Jaccard similarity**: token-level overlap between two texts
- **Sentiment**: Loughran-McDonald categories (negative, positive, uncertainty, litigious, strong_modal, weak_modal, constraining, complexity)
- **Complexity**: Fog Index, Flesch Reading Ease, syllable counts, avg sentence/word length
- **VADER**: NLTK sentiment intensity compound score
- **Length metrics**: token counts, length deltas
- **Delta sentiments**: year-over-year changes in all sentiment categories

Computation is CPU-intensive due to:
1. Tokenization + dictionary lookups across 86K+ words for each text
2. Complexity metrics (Fog Index requires syllable counting)
3. 25 parallel workers processing multiple CIKs simultaneously

## Financial Statements Extraction

During Step 2, with the `--financials` flag, the pipeline extracts financial statement tables from raw SGML filings and consolidates them by statement type:

**Per-filing extraction**:
- Parses `FilingSummary.xml` (embedded in SGML) to get authoritative R-file → statement name mappings
- Extracts main `<table class="report">` elements from each R-file HTML
- Handles parenthetical negatives: `(123)` → `-123.0`
- Saves to: `data/interim/financial_statements/<CIK>/<accession>/financial_statements.xlsx`
- One Excel sheet per statement (Balance Sheet, Income Statement, Cash Flows, Notes, etc.)
- Gracefully skips pre-XBRL filings (before 2009)

**Consolidation** (automatic with `--financials`):
- Groups sheets by name across all 10-K filings for the same CIK
- Creates one Excel per statement type: `data/interim/financial_statements/<CIK>/Balance Sheet.xlsx`, `Income Statement.xlsx`, etc.
- Sheet names in consolidated files = fiscal years (2024, 2025, etc., parsed from accession numbers)
- Deletes per-filing folders after consolidation

**Note**: Financial extraction works on **raw SGML files**, not cleaned text. Use `--keep-raw` to preserve raw HTML if debugging.

**Standalone CLI**:
```bash
# Extract financials for specific CIK without running full pipeline
uv run python utils/extract_financial_statements.py --cik 0001234567
```

## Step 2 Behavior

Step 2 (`download_filings`) always downloads and cleans 10-K filings. With `--financials`:
- Also extracts financial statement tables from raw SGML (per-filing extraction)
- **Automatically consolidates** statements by name across all years for each CIK
  - Creates one Excel per statement name (e.g., `Balance Sheet.xlsx`, `Income Statement.xlsx`)
  - Each consolidated Excel contains sheets named by fiscal year (2022, 2023, 2024, etc.)
  - Deletes per-filing folders after consolidation
  - Handles duplicate/sub-tables by grouping: `BALANCE SHEET` + `BALANCE SHEET_2` → both in same Excel as `2024` and `2024_2` sheets
- Output: cleaned text + consolidated financial statement Excels in `data/interim/financial_statements/<CIK>/`
- Without `--financials`: only cleaned text is produced

## Loughran-McDonald Dictionary Setup

Step 5 requires the **Loughran-McDonald Master Dictionary** (86K+ words with sentiment tags).

**First time setup**:
1. Download from: https://www3.nd.edu/~mcdonald/Word_Lists.html
2. Convert to CSV if needed (format: Word, Sequence, Count, WordProp, AvgProp, StdDev, DocCount, Negative, Positive, Uncertainty, Litigious, StrongModal, WeakModal, Constraining, Complexity, Syllables, Source)
3. Save to: `data/raw/lm_dict/Loughran-McDonald_MasterDictionary_1993-2024.csv`

If missing, step 5 will fail. A minimal stub dictionary exists for testing, but production runs require the full official dictionary.

## Development Utilities

```bash
# Test segmentation on a single filing
uv run python tools/segment_single_filing.py <path-to-filing>
```

`item_splitter.py` in the root is a legacy predecessor to `text/segment.py` — do not modify it.

## Known Issues & Dead Code

- `text/tokenizer.py`: `levenshtein_tokens()` function (line 175) is defined but never called — legacy code from earlier feature iteration
- Financial statement consolidation strips statement names to 31 chars for Excel compatibility, which may create duplicate sheet names (e.g., "SUMMARY OF SIGNIFICANT ACCO_2", "SUMMARY OF SIGNIFICANT ACCO_3")
