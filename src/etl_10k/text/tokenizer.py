from concurrent.futures import ProcessPoolExecutor, as_completed
from nltk.sentiment import SentimentIntensityAnalyzer
from etl_10k.config import INTERIM_ITEMS_DIR, MAX_WORKERS, INTERIM_CLEANED_DIR
from etl_10k.text import lm_dict, complexity as cx
import nltk
import sys
import re
        

nltk.download("vader_lexicon", quiet=True)
_sia = SentimentIntensityAnalyzer()

# --------------------------------------------------------------------------------------------------------------------
#                                                MAKE COMPS FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------

def check_date(folder):
    """
    Read a filing folder and extract the submission date for that accession.
    Returns a dict with "year", "month", "day", and "filing", or None if not found.
    """
    filing = folder.name
    file = folder / "full-submission.txt"
    with open(file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            hay = line.lower()
            if filing in hay:
                date = hay.partition(":")[2].lstrip()
                break
        else:
            # Filing ID not found in file — return None to skip this filing
            return None
    info_dict = {
        "year": date[:4],
        "month": date[4:6],
        "day": date[6:8],
        "filing": filing
    }
    return info_dict

def order_filings(records):
    """
    Sort filing records by release date (newest to oldest).
    Returns a list of [filing_id, filing_date] pairs.
    """
    records_sorted = sorted(
        records,
        key=lambda r: (int(r["year"]), int(r["month"]), int(r["day"])),
        reverse=True,
    )

    out = []
    for r in records_sorted:
        filing_id = r["filing"]
        filing_date = f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r['day']):02d}"
        out.append([filing_id, filing_date])
    return out

def make_comps(cik):
    """
    Build consecutive Item 1A comparison pairs for a single CIK.

    Uses available `item1A.txt` filings, orders them by date, and returns
    a list of {date1, filing1, date2, filing2} dicts.
    """
    date_data = []
    folders_path = INTERIM_ITEMS_DIR / cik / "10-K"
    checkdate_path = INTERIM_CLEANED_DIR / cik / "10-K"

    for i in folders_path.iterdir():
        if not (i / "item1A.txt").is_file():
            continue

        # Check if cleaned filing exists before trying to read it
        cleaned_file = checkdate_path / i.name / "full-submission.txt"
        if not cleaned_file.is_file():
            continue

        date_info = check_date(checkdate_path / i.name)
        if date_info is not None:
            date_data.append(date_info)

    # Skip CIKs with no valid filings (not yet processed through steps 2-4)
    if not date_data:
        return []

    print(cik)

    ordered_filings = order_filings(date_data)

    comps_list = []
    for n in range(1, len(ordered_filings)):
        comps_list.append({
            "date1": ordered_filings[n - 1][1],
            "filing1": ordered_filings[n - 1][0],
            "date2": ordered_filings[n][1],
            "filing2": ordered_filings[n][0]
        })
    return comps_list

def concurrency_runner(writer, ciks):
    """
    Orchestrate feature computation for multiple CIKs using multiprocessing.
    Runs `worker()` per CIK and writes the resulting rows to the output CSV.
    """
    #try:
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, cik): cik for cik in ciks}

        for fut in as_completed(futures):
            cik = futures[fut]
            try:
                rows = fut.result()
                writer.writerows(rows)
            except Exception as e:
                print(f"  Skipping {cik}: {type(e).__name__}: {e}")
    #except:
        #print("Skipped")

# ---------------------------------------------------------------------------------------

def worker(cik):
    """
    Compute feature rows for all consecutive filing comparisons for a single CIK.
    Returns a list of row dictionaries for writing to the output file.
    """
    path = INTERIM_ITEMS_DIR / cik
    if not path.exists():
        return []
    comps = make_comps(cik)
    rows = []
    [rows.append(process_comps(comp, cik)) for comp in comps]
    return rows

def process_comps(comp, cik):
    """
    Load two Item 1A texts for a comparison pair and compute feature metrics.
    Returns the output dictionary produced by `var_builder()`.
    """
    filingNew, filingOld = comp["filing1"], comp["filing2"]
    fileNew = INTERIM_ITEMS_DIR / cik / "10-K" / filingNew / "item1A.txt"
    fileOld = INTERIM_ITEMS_DIR / cik / "10-K" / filingOld / "item1A.txt"
    textNew = fileNew.read_text(encoding="utf-8", errors="ignore")
    textOld = fileOld.read_text(encoding="utf-8", errors="ignore")
    return var_builder(textNew, textOld, comp, cik)

# --------------------------------------------------------------------------------------------------------------------
#                                                VARIABLES FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------

'''
def tokenize(text: str) -> list[str]:
    """
    Returns list of all elements in the string in lowercase.
    """
    _WORD_RE = re.compile(r"[A-Za-z']+")
    return _WORD_RE.findall(text.lower())
'''
def tokenize(text: str):
    """
    Extract all words from text as uppercase tokens.
    Returns list of consecutive letter sequences (A-Z).
    """
    return re.findall(r"[A-Za-z]+", text.upper())

def mean_vader_compound(words) -> float:
    """
    Compute the average VADER compound score over a list of words.
    Returns 0.0 if the input list is empty.
    """
    compounds = []
    for w in words:
        w = (w or "").strip()
        scores = {"compound": 0.0} if not w else _sia.polarity_scores(w)
        compounds.append(scores["compound"])
    return sum(compounds) / len(compounds) if len(compounds) != 0 else 0

def jaccard_similarity(A, B) -> float:
    """
    Compute Jaccard similarity between the token sets of two texts.
    """
    A = set(A)
    B = set(B)
    if not A and not B:
        return 1.0
    return len(A & B) / len(A | B)

def var_builder(text_a, text_b, dict, cik):
    """
    Compute disclosure-change features between two Item 1A texts.
    Returns a dictionary with Jaccard similarity, sentiment, complexity, and VADER scores.
    """
    A, B = tokenize(text_a), tokenize(text_b)
    #print(f"text_a: {type(text_a)}, A: {type(A)}")
    scores_a = lm_dict.lm_tone(A)
    scores_b = lm_dict.lm_tone(B)
    comp_dict_a = cx.complexity(text_a, 'a')
    comp_dict_b = cx.complexity(text_b, 'b')
    feature_dict = {
        "cik": cik, 
        "date_a": dict["date1"], 
        "date_b": dict["date2"], 
        "jac_sim": jaccard_similarity(A,B), 
        "len_a": len(A), 
        "len_b": len(B),
        "lm_negative_a": scores_a['negative'] / len(A),
        "lm_positive_a": scores_a['positive'] / len(A),
        "lm_litigious_a": scores_a['litigious'] / len(A),
        "lm_complexity_a": scores_a['complexity'] / len(A),
        "lm_strong_modal_a": scores_a['strong_modal'] / len(A),
        "lm_weak_modal_a": scores_a['weak_modal'] / len(A),
        "lm_uncertainty_a": scores_a['uncertainty'] / len(A),
        "lm_constraining_a": scores_a['constraining'] / len(A),
        "nltk_sentiment_a": mean_vader_compound(A),
        }
    delta_dict = {
        "delta_lm_negative": scores_a['negative'] / len(A) - scores_b['negative'] / len(B),
        "delta_lm_positive": scores_a['positive'] / len(A) - scores_b['positive'] / len(B),
        "delta_lm_litigious": scores_a['litigious'] / len(A) - scores_b['litigious'] / len(B),
        "delta_lm_complexity": scores_a['complexity'] / len(A) - scores_b['complexity'] / len(B),
        "delta_lm_strong_modal": scores_a['strong_modal'] / len(A) - scores_b['strong_modal'] / len(B),
        "delta_lm_weak_modal": scores_a['weak_modal'] / len(A) - scores_b['weak_modal'] / len(B),
        "delta_lm_uncertainty": scores_a['uncertainty'] / len(A) - scores_b['uncertainty'] / len(B),
        "delta_lm_constraining": scores_a['constraining'] / len(A) - scores_b['constraining'] / len(B),
        "delta_nltk_sentiment": mean_vader_compound(A) - mean_vader_compound(B)
    }
    feature_dict.update(comp_dict_a)
    feature_dict.update(delta_dict)
    feature_dict.update(comp_dict_b)
    return feature_dict
