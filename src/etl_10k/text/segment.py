from concurrent.futures import ProcessPoolExecutor, as_completed
from etl_10k.config import INTERIM_CLEANED_DIR, INTERIM_ITEMS_DIR, MAX_WORKERS
from itertools import islice
import re

def _normalize_ws(s: str) -> str:
    """
    Replaces common non-breaking spaces with regular spaces, collapses runs of
    whitespace to a single space, and strips leading/trailing whitespace.
    """
    s = s.replace("\xa0", " ").replace("\u2007", " ").replace("\u202f", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def before_dot(s: str) -> str:
    """
    Return everything before the first '.'; if no dot, return the string unchanged.
    """
    i = s.find('.')
    return s[:i] if i != -1 else s

def clean_item_number(s: str) -> str:
    """
    Clean item number by removing parenthetical suffixes only if followed by a period,
    and stripping trailing non-alphanumeric characters (e.g. dashes used instead of dots).
    Examples:
        "9A(T)." -> "9A."
        "9A(T)" -> "9A(T)"
        "1B(A)." -> "1B."
        "7A-"   -> "7A"
        "9B-"   -> "9B"
    """
    s = re.sub(r'\([^)]*\)\.', '.', s)   # remove parenthetical suffix before dot
    s = re.sub(r'[^A-Za-z0-9]+$', '', s) # strip trailing non-alphanumeric (e.g. "-")
    return s

def item_dict_builder(path):
    """
    Build an ordered list of detected 10-K 'Item' with their line number.

    Reads the file at 'path', scans line-by-line for headings that look like
    'Item 1.', 'Item 1A.', etc., and returns a list of dictionaries containing:
      - item_num: normalized item token (e.g., "1", "1A")
      - item_line: 1-indexed line number where the heading appears
    Consecutive duplicate item tokens are removed (deduped) to reduce noise.
    """

    text = path.read_text(encoding="utf-8", errors="ignore")
    out = []
    HEAD_RE = re.compile(r'^\s*(?P<kind>items?)\b\s*(?P<rest>[0-9].*)$', re.IGNORECASE)                                # Regex to find lines to split

    for i, raw in enumerate(text.splitlines(), start=1):
        line = _normalize_ws(raw)
        if not line:
            continue
        m = HEAD_RE.match(line)
        if not m or not m.group('rest'):
            continue
        label = m.group('rest')

        # Clean parenthetical suffixes like (T) from "9A(T)." -> "9A."
        cleaned_label = clean_item_number(label)

        out.append({
            'item_num': before_dot(_normalize_ws(cleaned_label).split()[0]).upper(),
            'item_line': i,
        })

    # dedupe consecutive duplicates
    deduped = []
    last = None
    for row in out:
        key = row['item_num'].lower()
        if key != last:
            deduped.append(row)
        last = key

    return deduped

def number_of_rounds(item_dict, bool):
    """
    Extract numeric item components and estimate how many full 'rounds' of items exist.

    For each entry in `item_dict:` list[dict], extracts only digits from the `item_num` field and
    converts them to integers. 
    Then estimates the number of repeated "rounds" of the table-of-contents items by
    counting occurrences of `max_num` and `max_num - 1`.
    """
    out = []
    for items in item_dict:
        digits = "".join(ch for ch in items.get("item_num") if ch.isdigit() and ch)
        out.append(digits)
    listAllItems = [int(i) for i in out] # make a list of all the item numbers (w/out letters)

    # sometimes "Item 400" exists
    while max(listAllItems) > 20:
        listAllItems.remove(max(listAllItems))
    
    max_num = max(listAllItems)

    # Double check the number of rounds is correct
    rounds = [i for i in listAllItems if i==max_num]
    rounds2 = [i for i in listAllItems if i==max_num-1]
    
    # print(f"rounds: {rounds}; rounds2: {rounds2}")

    last_ele = max_num if len(rounds) >= len(rounds2) else max_num - 1
    rounds = rounds2 if len(rounds) > len(rounds2) else rounds
    
    # print(f"last ele: {last_ele}")

    if bool == True:
        return len(rounds)
    else:
        return last_ele

def table_content_builder(filepath):
    """
    Builds a `tableContent` with a list of all the items in the 10K
    
    Returns: list[str]
    eg ['1', '1A', '1B', '1C', '2', ...]
    """
    item_dict = item_dict_builder(filepath)
    last_ele = number_of_rounds(item_dict, bool=False)
    tableContent = ["1", "1A", "1B", "1C", "1D", "2", "3", "4", "5", "6", "7", "7A", "8"]
    letters_tuple = ("","A","B","C")
    for n in range(int(tableContent[-1])+1,last_ele+1):
        n = str(n)
        for l in letters_tuple:
            tableContent.append(n + l)

    return tableContent

def _append_orphans(selected, item_dict):
    """
    Append body-only items (those with no TOC entry) that appear after the last
    selected item. This recovers items like Item 16 that exist only in the body.
    """
    if not selected:
        return selected
    last_line = selected[-1]['item_line']
    selected_nums = {r['item_num'] for r in selected}
    seen = set()
    for r in item_dict:
        if r['item_line'] > last_line and r['item_num'] not in selected_nums and r['item_num'] not in seen:
            selected.append(r)
            seen.add(r['item_num'])
    return selected

def item_segmentation_list(filepath, verbose=False):
    """
    Makes a list of dict that contains the actual items and where they should be segmented.

    Retreves the table of content of the 10-K with the table_content_builder function.
    Retreves the list of all the possible items and their location with the item_dict_builder function.

    First, builds multiple candidate sequences of item headings by scanning in item_dict.
    Secondly, Selects the candidate that is most probably the item list

    Args:
        filepath: Path to the filing to segment
        verbose: If True, print debug information (default: False)

    Returns list[dict]: The selected sequence (list of dicts with 'Item number' and 'Item line').
    """
    tableContent = table_content_builder(filepath)
    item_dict = item_dict_builder(filepath)

    list_lines = []
    last_ele = 0
    for _ in range(number_of_rounds(item_dict, bool=True)):
        lines = []
        for itemTC in tableContent:
            for r in item_dict:
                if itemTC == r.get('item_num') and r.get('item_line') > last_ele:
                    lines.append(r)
                    last_ele = r['item_line']
                    break
        list_lines.append(lines)

    # ----- Choose candidate with greatest character span -----

    if verbose:
        print(f"Table of Contents: {tableContent}")
        print(f"Item Dictionary: {item_dict}")
        print(f"Candidate lists: {list_lines}")

    if len(list_lines) == 1:
        return _append_orphans(list_lines[0], item_dict)

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    def _line_start_offsets(text: str):
        starts = [0]
        for line in text.splitlines(keepends=True):
            starts.append(starts[-1] + len(line))
        return starts

    def _normalize_line_index(item_line: int, num_lines: int) -> int:
        if item_line is None:
            return 0
        if 1 <= item_line <= num_lines:  # likely 1-based
            return item_line - 1
        return max(0, min(item_line, num_lines - 1))

    line_start_char = _line_start_offsets(text)
    num_lines = len(text.splitlines())

    best_i = 0
    best_span = float("-inf")

    for i, cand in enumerate(list_lines):
        start_line = _normalize_line_index(cand[0]["item_line"], num_lines)
        end_line = _normalize_line_index(cand[-1]["item_line"], num_lines)

        span_chars = line_start_char[end_line] - line_start_char[start_line]

        if span_chars > best_span:
            best_span = span_chars
            best_i = i

    return _append_orphans(list_lines[best_i], item_dict)

def print_items(cik):
    """
    Write per-item text files by slicing the input document between detected item headings.

    For each entry in `final_split`, this function:
      - takes the line range from its `line_no` to the next item heading's `line_no`,
      - writes the extracted chunk to `p/item<ITEM>.txt` (e.g., item1A.txt).

    Parameters
    ----------
    filepath : pathlib.Path
        Path to the full cleaned filing text (e.g., clean-full-submission.txt).
    final_split : list[dict]
        Selected sequence of headings (output of `final_list`), containing 'item_n' and 'line_no'.
    p : pathlib.Path
        Output directory where item files will be written (typically the filing folder).
    """
    # Read from cleaned_filings directory
    source_path = INTERIM_CLEANED_DIR / cik / '10-K'

    filing_completed = 0
    filing_failed = 0

    for filing in source_path.iterdir():
        filepath = source_path / filing / "full-submission.txt"
        try:
            item_segmentation = item_segmentation_list(filepath)
            page_list = [i['item_line'] for i in item_segmentation]
            num_lines = sum(1 for _ in filepath.open("r", encoding="utf-8", errors="replace"))
            page_list.append(num_lines + 1)

            # Write to items directory
            output_dir = INTERIM_ITEMS_DIR / cik / '10-K' / filing.name
            output_dir.mkdir(parents=True, exist_ok=True)

            for n, i in enumerate(item_segmentation):
                start, end = page_list[n], page_list[n+1]
                with filepath.open("r", encoding="utf-8", errors="replace") as f:
                    lines = list(islice(f, start - 1, end-1))
                chunk = "".join(lines)
                filename = f"item{i['item_num']}.txt"

                full_path = output_dir / filename
                with open(full_path, "w", encoding='utf-8') as f:
                    f.write(chunk)

            filing_completed += 1
            print(f"✓ CIK {cik} | {filing.name}: segmentation complete")
        except Exception as e:
            filing_failed += 1
            print(f"[FAILED] {cik} / {filing.name}: {type(e).__name__} - {e}")

    return filing_completed, filing_failed

def try_exercize(ciks: list):
    """
    Runs print_items in parallel and tracks completion statistics
    """
    total_ciks = len(ciks)
    ciks_completed = 0
    ciks_failed = 0
    total_filings = 0
    filings_completed = 0
    filings_failed = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(print_items, cik): cik for cik in ciks}

        for fut in as_completed(futures):
            cik = futures[fut]
            try:
                f_completed, f_failed = fut.result()
                filings_completed += f_completed
                filings_failed += f_failed
                total_filings += f_completed + f_failed
                if f_failed == 0:
                    ciks_completed += 1
                else:
                    ciks_failed += 1
            except Exception as e:
                ciks_failed += 1
                print(f"[FAILED] {cik}: {type(e).__name__} - {e}")

    total_filings_pct = lambda n: f"({n/total_filings*100:.1f}%)" if total_filings > 0 else ""
    total_ciks_pct    = lambda n: f"({n/total_ciks*100:.1f}%)"    if total_ciks > 0    else ""

    print(f"\n{'='*60}")
    print(f"Segmentation Summary:")
    print(f"  CIKs     — total: {total_ciks} | completed: {ciks_completed} {total_ciks_pct(ciks_completed)}| failed: {ciks_failed} {total_ciks_pct(ciks_failed)}")
    print(f"  Filings  — total: {total_filings} | completed: {filings_completed} {total_filings_pct(filings_completed)}| failed: {filings_failed} {total_filings_pct(filings_failed)}")
    print(f"{'='*60}")
    return


