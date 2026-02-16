from pathlib import Path
import os

# ------------------ Directories ------------------ 

ROOT_DIR = Path(__file__).resolve().parents[2]                             # Returns absolute path of risk_factor_pred folder

# ------------------ DATA Directories ------------------ 

DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_EDGAR_DIR = RAW_DIR / "sec-edgar-filings"
RAW_CIKS_DIR = RAW_DIR / "ciks_index"
RAW_LM_DICT_DIR = RAW_DIR / "lm_dict"
INTERIM_CLEANED_DIR = INTERIM_DIR / "cleaned_filings"
INTERIM_ITEM1A_DIR = INTERIM_DIR / "item1a"
INTERIM_FEATURES_DIR = INTERIM_DIR / "text_features"
INTERIM_RETURNS_DIR = INTERIM_DIR / "returns"

PROCESSED_PANEL_DIR = PROCESSED_DIR / "panel"

# ------------------------------------------------------ 

CIK_LIST = RAW_CIKS_DIR / "cik_list.csv"                                     # csv containing list of CIKS

FEATURES_FILE = INTERIM_FEATURES_DIR / "features.csv"
RETURNS_FILE = INTERIM_RETURNS_DIR / "returns.csv"
FINAL_DATASET = PROCESSED_PANEL_DIR / "final_dataset.csv"

# ---------- SETTINGS ----------
FORM       = "10-K"                                                 # or "10-K", "10-KT", etc.
START_DATE = "2006-01-01"                                           # filings per CIK, only released after 2006

# Auto-detect optimal number of workers based on CPU cores
# Uses 90% of available cores (leaves room for OS and other processes)
CPU_CORES = os.cpu_count() or 1
MAX_WORKERS = max(1, int(CPU_CORES * 0.9))                         # Auto-scales: 25 workers on 28-core, 7 workers on 8-core, etc.

# SEC download workers (rate limited by TokenBucketRateLimiter at 9.5 req/sec)
# With rate limiter in place, we can use more workers for better I/O parallelism
# Workers handle: token acquisition (rate limited) + HTTP request + file I/O + enumeration
MAX_WORKERS_DOWNLOADS = 15                                          # Rate limiter ensures SEC compliance

# SEC rate limit: 10 req/sec, using 9.5 for safety margin
# Shared across all download workers via rate limiter to avoid IP bans
SEC_REQUESTS_PER_SECOND = 9.5                                       # Conservative: 5% margin under SEC's limit
# -------------------------------

def ensure_project_dirs() -> None:
    for p in [
        RAW_EDGAR_DIR,
        RAW_CIKS_DIR,

        INTERIM_CLEANED_DIR,
        INTERIM_ITEM1A_DIR,
        INTERIM_FEATURES_DIR,
        INTERIM_RETURNS_DIR,

        PROCESSED_PANEL_DIR
    ]:
        p.mkdir(parents=True, exist_ok=True)


FEATURES_FIELDS = [
    "cik", 
    "date_a", 
    "date_b", 
    "jac_sim", 
    "len_a", 
    "len_b",
    "lm_negative_a",
    "lm_negative_b",
    "lm_positive_a",
    "lm_positive_b",
    "lm_litigious_a",
    "lm_litigious_b",
    "lm_complexity_a",
    "lm_complexity_b",
    "lm_strong_modal_a",
    "lm_strong_modal_b",
    "lm_weak_modal_a",
    "lm_weak_modal_b",
    "lm_uncertainty_a",
    "lm_uncertainty_b",
    "lm_constraining_a",
    "lm_constraining_b",      
    "nltk_sentiment_a",
    "nltk_sentiment_b",
    "fog_index_a",
    "flesch_ease_a",
    "flesch_grade_a",
    "avg_sentence_length_a",
    "avg_word_length_a",
    "num_words_a",
    "num_sentences_a",
    "pct_complex_words_a",
    "byte_a",
    "fog_index_b",
    "flesch_ease_b",
    "flesch_grade_b",
    "avg_sentence_length_b",
    "avg_word_length_b",
    "num_words_b",
    "num_sentences_b",
    "pct_complex_words_b",
    "byte_b"
    ]
