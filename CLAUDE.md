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
python scripts/99_reproduce_all.py

# Specific steps for specific CIKs
python -m etl_10k.pipeline.steps --cik 320193 --from-step 2 --to-step 4

# Key CLI flags
--cik <CIK>             # Single CIK
--ciks <CIK,CIK,...>    # Comma-separated CIKs
--start-year <YEAR>     # Default: 2006
--end-year <YEAR>       # Default: 2026
--from-step <0-6>       # Step to start from
--to-step <0-6>         # Step to end at
--keep-raw              # Don't delete raw HTML after cleaning (default: delete)
```

Individual step scripts in `scripts/` (e.g., `python scripts/04_extract_item1a.py`).

## Architecture

### Data Flow

```
SEC EDGAR HTML → [Step 2] Download + Clean (integrated) → Cleaned text
→ [Step 4] Item 1A segmentation → Per-filing item text
→ [Step 5] Feature computation → text_features/features.csv
→ [Step 6] Merge with CRSP returns → data/processed/panel/final_dataset.csv
```

Step 0 builds the CIK universe from SEC master indices. Step 1 fetches CRSP stock returns from WRDS (requires WRDS credentials).

### Key Directory Paths (from `config.py`)

- `data/raw/sec-edgar-filings/` — Downloaded 10-K HTML
- `data/interim/cleaned_filings/` — Cleaned text files
- `data/interim/items/<CIK>/10-K/<accession>/item1A.txt` — Extracted Item 1A sections
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
- **Edit distance**: token-level Levenshtein (memory-efficient row-by-row DP)
- **Jaccard similarity**: overlap between token sets
- **Sentiment**: Loughran-McDonald categories (negative, positive, uncertainty, litigious, modal, constraining)
- **Complexity**: Fog Index, Flesch-Kincaid, avg sentence/word length
- **VADER**: mean compound score
- **Length metrics**: word counts, length deltas

## Development Utilities

```bash
# Test segmentation on a single filing
python tools/segment_single_filing.py <path-to-filing>
```

`item_splitter.py` in the root is a legacy predecessor to `text/segment.py` — do not modify it.
