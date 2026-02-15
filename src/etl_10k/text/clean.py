from etl_10k.config import RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, MAX_WORKERS
from concurrent.futures import ProcessPoolExecutor, as_completed
import re

# --------------------------------------------------------------------------------------------------------------------
#                                              PRE-COMPILED REGEX PATTERNS
# --------------------------------------------------------------------------------------------------------------------

# XBRL and XML removal patterns
_XBRL_XML_BLOCKS_PATTERN = re.compile(r'(<XBRL.*?>.*?</XBRL>)|(<XML.*?>.*?</XML>)', re.DOTALL)
_IX_TAGS_PATTERN = re.compile(r'</?ix:.*?>')

# Head and attribute removal patterns
_HEAD_PATTERN = re.compile(r'<head>.*?</head>', re.DOTALL | re.IGNORECASE)
_STYLE_ATTR_PATTERN = re.compile(r'\sstyle=(["\']).*?\1', re.IGNORECASE)
_ID_ATTR_PATTERN = re.compile(r'\s+id=(["\']).*?\1', re.IGNORECASE)
_ALIGN_ATTR_PATTERN = re.compile(r'\s+align=(["\']).*?\1', re.IGNORECASE)

# Comment, image, and entity patterns
_HTML_COMMENT_PATTERN = re.compile(r'<!--.*?-->', re.DOTALL)
_IMG_TAG_PATTERN = re.compile(r'<img.*?>', re.IGNORECASE)
_NUMERIC_ENTITY_3DIGIT_PATTERN = re.compile(r'&#\d{3};')

# Empty tag patterns (for loop cleaning)
_EMPTY_P_PATTERN = re.compile(r'<p>\s*</p>', re.DOTALL | re.IGNORECASE)
_EMPTY_DIV_PATTERN = re.compile(r'<div>\s*</div>', re.IGNORECASE)

# Numeric entity pattern (general)
_NUMERIC_ENTITY_PATTERN = re.compile(r'&#(?:\d{1,8}|[xX][0-9A-Fa-f]{1,8});')

# Tag unwrapping patterns
_IX_OPENING_TAG_PATTERN = re.compile(r'<ix:[a-zA-Z0-9_:]+.*?>', re.IGNORECASE)
_IX_CLOSING_TAG_PATTERN = re.compile(r'</ix:[a-zA-Z0-9_:]+>', re.IGNORECASE)
_HTML_OPENING_TAG_PATTERN = re.compile(r'<html.*?>', re.IGNORECASE | re.DOTALL)
_HTML_CLOSING_TAG_PATTERN = re.compile(r'</html>', re.IGNORECASE)
_FONT_OPENING_TAG_PATTERN = re.compile(r'<font.*?>', re.IGNORECASE | re.DOTALL)
_FONT_CLOSING_TAG_PATTERN = re.compile(r'</font>', re.IGNORECASE)
_BR_TAG_PATTERN = re.compile(r'<br.*?>', re.IGNORECASE | re.DOTALL)
_HR_TAG_PATTERN = re.compile(r'<hr.*?>', re.IGNORECASE | re.DOTALL)
_B_OPENING_TAG_PATTERN = re.compile(r'<B>', re.IGNORECASE | re.DOTALL)
_B_CLOSING_TAG_PATTERN = re.compile(r'</B>', re.IGNORECASE)
_CENTER_OPENING_TAG_PATTERN = re.compile(r'<center>', re.IGNORECASE | re.DOTALL)
_CENTER_CLOSING_TAG_PATTERN = re.compile(r'</center>', re.IGNORECASE)
_A_OPENING_TAG_PATTERN = re.compile(r'<a.*?>', re.IGNORECASE | re.DOTALL)
_A_CLOSING_TAG_PATTERN = re.compile(r'</a>', re.IGNORECASE)
_TABLE_OPENING_TAG_PATTERN = re.compile(r'<table.*?>', re.DOTALL | re.IGNORECASE)
_TABLE_CLOSING_TAG_PATTERN = re.compile(r'</table>', re.DOTALL | re.IGNORECASE)
_TR_OPENING_TAG_PATTERN = re.compile(r'<tr.*?>', re.DOTALL | re.IGNORECASE)
_TR_CLOSING_TAG_PATTERN = re.compile(r'</tr>', re.DOTALL | re.IGNORECASE)
_TD_OPENING_TAG_PATTERN = re.compile(r'<td.*?>', re.DOTALL | re.IGNORECASE)
_TD_CLOSING_TAG_PATTERN = re.compile(r'</td>', re.DOTALL | re.IGNORECASE)

# Other tag patterns
_P_TAG_PATTERN = re.compile(r'<p.*?>', re.IGNORECASE)
_ALL_HTML_TAGS_PATTERN = re.compile(r'<.*?>')

# XBRLI patterns
_XBRLI_MEASURE_PATTERN = re.compile(r'<xbrli:([a-zA-Z0-9_:]+).*?>.*?</xbrli:\1>', re.DOTALL | re.IGNORECASE)

# SEC document patterns
_SEC_DOCUMENT_PATTERN = re.compile(r'<SEC-DOCUMENT>.*\Z', re.DOTALL)
_CONTENT_BEFORE_SEQUENCE2_PATTERN = re.compile(r'^.*?(?=<SEQUENCE>2)', re.DOTALL)

# Item heading patterns
_ITEM_HEAD_DETECT_PATTERN = re.compile(
    r'\s*items?\b\s*'
    r'\d+[A-Za-z]?'
    r'(?:\s*(?:and|to|through|-)\s*\d+[A-Za-z]?)*'
    r'\s*\.', re.IGNORECASE)
_ITEM_IGNORE_PRE_PATTERN = re.compile(r'\b(?:in|of|see|at|with|under|this|to)[ \t"""]*$', re.IGNORECASE)
_SPACE_BEFORE_NEWLINE_PATTERN = re.compile(r'[ \t]+\n')

# Item cleaning patterns
_ENSURE_SPACE_AFTER_ITEM_PATTERN = re.compile(r'\b(Items?)\b(?=\S)')
_ITEM_NUMBER_ONLY_PATTERN = re.compile(r'Item\s+\d+')
_ITEM_SUFFIX_PATTERN = re.compile(r'[A-Za-z]\.')

# Tag unwrapping replacements (consolidated for efficient sequential processing)
_TAG_REPLACEMENTS = [
    # ix tags
    (_IX_OPENING_TAG_PATTERN, '\n'),
    (_IX_CLOSING_TAG_PATTERN, ''),
    # html tags
    (_HTML_OPENING_TAG_PATTERN, '\n'),
    (_HTML_CLOSING_TAG_PATTERN, ''),
    # font tags
    (_FONT_OPENING_TAG_PATTERN, '\n'),
    (_FONT_CLOSING_TAG_PATTERN, ''),
    # line break and horizontal rule
    (_BR_TAG_PATTERN, ''),
    (_HR_TAG_PATTERN, ''),
    # bold tags
    (_B_OPENING_TAG_PATTERN, '\n'),
    (_B_CLOSING_TAG_PATTERN, ''),
    # center tags
    (_CENTER_OPENING_TAG_PATTERN, '\n'),
    (_CENTER_CLOSING_TAG_PATTERN, ''),
    # anchor tags
    (_A_OPENING_TAG_PATTERN, '\n'),
    (_A_CLOSING_TAG_PATTERN, ''),
    # table tags
    (_TABLE_OPENING_TAG_PATTERN, '\n'),
    (_TABLE_CLOSING_TAG_PATTERN, ''),
    # table row tags
    (_TR_OPENING_TAG_PATTERN, '\n'),
    (_TR_CLOSING_TAG_PATTERN, ''),
    # table cell tags
    (_TD_OPENING_TAG_PATTERN, '\n'),
    (_TD_CLOSING_TAG_PATTERN, ''),
]

# --------------------------------------------------------------------------------------------------------------------
#                                              REGEX FOR HTML CLEANING
# --------------------------------------------------------------------------------------------------------------------

def remove_xbrl_xml_blocks(html_content):
    """
    This function deletes:
      - entire <XBRL>...</XBRL> or <XML>...</XML> blocks,
      - individual inline XBRL tags like <ix:...> or </ix:...>.
    """
    clean_content = _XBRL_XML_BLOCKS_PATTERN.sub('', html_content)
    clean_content = _IX_TAGS_PATTERN.sub('', clean_content)
    return clean_content

def _ends_with_tag(piece: str) -> bool:
    """
    Heuristically determine whether a text fragment ends with an HTML tag.

    The check:
      - trims trailing whitespace,
      - confirms the string ends with '>',
      - searches the last up to 300 characters for the most recent '<',
      - ensures there is no newline between that '<' and the end.

    Returns True if the fragment likely ends with an HTML tag, otherwise False.
    """
    s = piece.rstrip()
    if not s.endswith(">"):
        return False
    tail = s[-300:] if len(s) > 300 else s
    i = tail.rfind("<")
    return i != -1 and "\n" not in tail[i:]

def _starts_with_tag(line: str) -> bool:
    """
    Returns True if the first non-whitespace character is '<', else False.
    """
    return line.lstrip().startswith("<")

def soft_unwrap_html_lines(html: str) -> str: # Removes /n if sentence is ongoing
    """
    Join lines that appear to be mid-sentence.

    Lines are joined when:
      - the current logical line does NOT end with an HTML tag, AND
      - the next line does NOT start with an HTML tag.

    When joining, the function enforces exactly one space at the boundary.
    """
    lines = html.splitlines()
    if not lines:
        return html

    out_lines = []

    parts = [lines[0].rstrip("\r")]
    cur_ends_with_tag = _ends_with_tag(parts[-1])

    for raw_next in lines[1:]:
        nxt = raw_next.rstrip("\r")
        next_starts_tag = _starts_with_tag(nxt)

        if (not cur_ends_with_tag) and (not next_starts_tag):
            # join: ensure exactly one space at the boundary
            if parts[-1] and parts[-1].endswith((" ", "\t")):
                parts[-1] = parts[-1].rstrip()
            parts.append(" ")
            parts.append(nxt.lstrip())
            # after joining, the 'current line' ends as nxt ends
            cur_ends_with_tag = _ends_with_tag(nxt)
        else:
            # flush current logical line
            out_lines.append("".join(parts))
            # start a new logical line
            parts = [nxt]
            cur_ends_with_tag = _ends_with_tag(nxt)

    # flush the last line
    out_lines.append("".join(parts))
    return "\n".join(out_lines)

def remove_head_with_regex(html_content):
    """
    Remove the <head>...</head> section from HTML content.
    """
    return _HEAD_PATTERN.sub('', html_content)

def remove_style_with_regex(html_content):
    """
    Strip inline 'style' attributes from all HTML tags.
    """
    return _STYLE_ATTR_PATTERN.sub('', html_content)

def remove_id_with_regex(html_content):
    """
    Strip 'id' attributes from all HTML tags.
    """
    return _ID_ATTR_PATTERN.sub('', html_content)

def remove_align_with_regex(html_content):
    """
    Strip 'align' attributes from all HTML tags.
    """
    return _ALIGN_ATTR_PATTERN.sub('', html_content)

def remove_part_1(html_content): # Cleans comments, tables, img, span
    """
    Operations performed:
      - remove HTML comments <!-- ... -->,
      - remove <img ...> tags,
      - replace certain HTML entities with ASCII equivalents,
      - remove numeric character references of the form '&#ddd;'.
    """
    html_content = _HTML_COMMENT_PATTERN.sub('', html_content)
    html_content = _IMG_TAG_PATTERN.sub('', html_content)

    html_content = html_content.replace('<span>', '').replace('</span>', '').replace('&#8217;', "'").replace('&#8220;', '"').replace('&#8221;', '"')
    html_content = html_content.replace('&nbsp;', ' ').replace('&#146;', "'")

    html_content = _NUMERIC_ENTITY_3DIGIT_PATTERN.sub(' ', html_content)

    return html_content

def loop_clean(html_content):
    """
    Iteratively remove empty <p>...</p> and <div>...</div> tags until stable.
    """
    max_iterations = 10  # Safety limit to prevent infinite loops
    for _ in range(max_iterations):
        pre_cleaning_content = html_content

        html_content = _EMPTY_P_PATTERN.sub('', html_content)
        html_content = _EMPTY_DIV_PATTERN.sub('', html_content)

        if html_content == pre_cleaning_content:
            break

    return html_content

def remove_numeric_entities(s):
    """
    Remove numeric HTML entities such as '&#123;' or '&#x1F4A9;'.
    """
    return _NUMERIC_ENTITY_PATTERN.sub('', s)

def unwrap_tags(html_content):
    """
    This function removes/replaces a set of tags commonly found in SEC filings,
    inserting newlines for structural tags and deleting closing tags. It also
    removes table-related tags (<table>, <tr>, <td>) by converting some to newlines.

    Uses consolidated pattern replacements for improved performance.
    """
    for pattern, replacement in _TAG_REPLACEMENTS:
        html_content = pattern.sub(replacement, html_content)
    return html_content

def clean_lines(text_content):
    """
    Drop empty lines and removes all leading and trailing whitespace.
    """
    cleaned_lines = [line.lstrip() for line in text_content.splitlines() if line.strip()]
    return '\n'.join(cleaned_lines)

def prepend_newline_to_p(html_content):
    """
    Insert a newline before every <p ...> tag to improve downstream line-based parsing.
    """
    return _P_TAG_PATTERN.sub(r'\n\g<0>', html_content)

def strip_all_html_tags(html_content):
    """
    Remove all HTML tags by deleting substrings matching '<...>'.
    """
    return _ALL_HTML_TAGS_PATTERN.sub('', html_content)

def remove_xbrli_measure(html_content):
    """
    Remove <xbrli:*>...</xbrli:*> blocks (e.g., <xbrli:measure>...</xbrli:measure>).
    """
    return _XBRLI_MEASURE_PATTERN.sub('', html_content)

def get_from_sec_document(html_content):
    """
    Trim content to start at the <SEC-DOCUMENT> marker if present.
    """
    match = _SEC_DOCUMENT_PATTERN.search(html_content)
    return match.group(0) if match else html_content

def get_content_before_sequence(html_content):
    """
    Keep content before the '<SEQUENCE>2' marker, if present.
    """
    match = _CONTENT_BEFORE_SEQUENCE2_PATTERN.search(html_content)
    return match.group() if match else html_content

def break_on_item_heads(text):
    """
    Insert a newline before detected 'Item <number>[suffix].' headings
    or 'Item <number> [text] <number>.
    """
    out = []
    last = 0
    for m in _ITEM_HEAD_DETECT_PATTERN.finditer(text):
        start = m.start()

        g = m.group(0)
        lead = len(g) - len(g.lstrip(" \t"))
        item_start = start + lead

        if start > 0 and text[start-1] != '\n':
            ctx = text[max(0, item_start - 40):item_start]

            if _ITEM_IGNORE_PRE_PATTERN.search(ctx):
                continue  # ignore it

            out.append(text[last:start])
            out.append('\n')
            last = start
    out.append(text[last:])
    s = ''.join(out)
    return _SPACE_BEFORE_NEWLINE_PATTERN.sub('\n', s)  # tidy spaces before newlines

def clean_html(file_content):
    """
    Perform end-to-end HTML-to-text cleaning for SEC filing content.
    """
    cleaned = soft_unwrap_html_lines(file_content)
    cleaned = get_from_sec_document(cleaned)
    
    cleaned = get_content_before_sequence(cleaned)
    cleaned = remove_head_with_regex(cleaned)
    
    cleaned = remove_style_with_regex(cleaned)
    cleaned = remove_id_with_regex(cleaned)
    cleaned = remove_align_with_regex(cleaned)
    
    cleaned = remove_part_1(cleaned)
    cleaned = unwrap_tags(cleaned)
    cleaned = remove_xbrli_measure(cleaned)
    
    cleaned = loop_clean(cleaned)

    cleaned = prepend_newline_to_p(cleaned)

    cleaned = strip_all_html_tags(cleaned)
    cleaned = remove_numeric_entities(cleaned)
    cleaned = break_on_item_heads(cleaned)
    cleaned = clean_lines(cleaned)
    return cleaned

def print_clean_txt(html_path):
    """
    Load a filing, clean it, and return the cleaned text.
    """
    try:
        with open(html_path, 'r', encoding='utf-8') as file:
            file_content = file.read()
        cleaned = clean_html(file_content)
    except FileNotFoundError:
        print(f"Error: The file '{html_path}' was not found.")
    return cleaned


# --------------------------------------------------------------------------------------------------------------------
#                                                Cleaning 'Items'
# --------------------------------------------------------------------------------------------------------------------

def cleaning_items(html_content):
    """
    Normalize broken 'Item' headings that are split across lines.
    """
    html_content = merge_I_tem(html_content)
    html_content = ensure_space_after_item(html_content)
    html_content = merge_item_with_number_line(html_content)
    return merge_item_number_with_suffix(html_content)

def merge_I_tem(content):
    """
    Merge cases where 'I' appears alone on a line and the next line starts with 'tem'.

    Example:
      Line i:   "I"
      Line i+1: "tem 1. Business"
      -> "Item 1. Business"
    """
    lines = content.splitlines()
    new_lines = []
    i = 0

    while i < len(lines):
        # Make sure there is a next line to look at
        if (
            lines[i].strip() == "I" and
            i + 1 < len(lines) and
            lines[i + 1].lstrip().startswith("tem")
        ):
            merged_line = "I" + lines[i + 1].lstrip()
            new_lines.append(merged_line)
            i += 2  # skip the next line because we've merged it
        else:
            new_lines.append(lines[i])
            i += 1
    return "\n".join(new_lines)

def ensure_space_after_item(text):
    """
    Ensure 'Item' or 'Items' is followed by a space when immediately followed by non-space.

    Example:
      'Item1A' -> 'Item 1A'
    """
    return _ENSURE_SPACE_AFTER_ITEM_PATTERN.sub(r'\1 ', text)

def merge_item_with_number_line(text):
    """
    Merge lines where 'Item'/'Items' is on its own line and the next line begins with a digit.

    Example:
      "Item"
      "1. Business"
      -> "Item 1. Business"
    """
    lines = text.splitlines()
    new_lines = []
    i = 0

    while i < len(lines):
        current = lines[i].strip()

        # Check if this line is exactly 'Item' or 'Items'
        if current in ("Item", "Items") and i + 1 < len(lines):
            next_raw = lines[i + 1]
            # Remove leading spaces to inspect the first real character
            next_stripped_leading = next_raw.lstrip()

            # Check if next line starts with a digit
            if next_stripped_leading and next_stripped_leading[0].isdigit():
                # Merge: 'Item' + space + next line (without leading spaces)
                merged = f"{current} {next_stripped_leading}"
                new_lines.append(merged)
                i += 2  # skip the next line (already merged)
                continue

        # Default: keep line as-is
        new_lines.append(lines[i])
        i += 1

    return "\n".join(new_lines)

def merge_item_number_with_suffix(text):
    """
    If a line is 'Item {number}' only, and the following line starts with either:
      - a single letter and a dot (e.g., 'A. Risk Factors')
      - or just a dot (e.g., '. Risk Factors')
    then merge them into one line: 'Item 1A. Risk Factors' or 'Item 1. Risk Factors'.
    """
    lines = text.splitlines()
    new_lines = []
    i = 0

    while i < len(lines):
        current_stripped = lines[i].strip()

        # Match 'Item {number}' (e.g., 'Item 1', 'Item 12')
        if _ITEM_NUMBER_ONLY_PATTERN.fullmatch(current_stripped) and i + 1 < len(lines):
            next_raw = lines[i + 1]
            next_stripped = next_raw.lstrip()

            # Next line starts with 'A.' or 'b.' etc, OR with just '.'
            if _ITEM_SUFFIX_PATTERN.match(next_stripped) or next_stripped.startswith('.'):
                merged = current_stripped + next_stripped  # e.g. 'Item 1' + 'A. Risk Factors'
                new_lines.append(merged)
                i += 2
                continue

        # Default: keep line as-is
        new_lines.append(lines[i])
        i += 1
    return "\n".join(new_lines)

# --------------------------------------------------------------------------------------------------------------------
#                                              MERGES THE FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------

def print_10X(SAVE_path, html_content):
    """
    Write cleaned filing text to disk.
    """
    with open(SAVE_path, "w", encoding='utf-8') as new_file:
        new_file.write(html_content)


def clean_worker(ciks):
    """
    Run the filing cleaning step in parallel across a list of CIKs.

    Uses a process pool to call `cleaner()` on each CIK for true parallel execution.
    ProcessPoolExecutor bypasses Python's GIL, enabling full CPU utilization.
    """
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(cleaner, cik): cik for cik in ciks}

        for fut in as_completed(futures):
            cik = futures[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"[FAILED] {cik}: {type(e).__name__} - {e}")

def cleaner(cik):
    """
    Clean raw 10-K filings for a single CIK and save cleaned text files.

    Iterates over accession folders in `RAW_EDGAR_DIR`, applies text cleaning,
    and writes outputs to the corresponding path under `INTERIM_CLEANED_DIR`.
    """
    try:
        output_filename = "full-submission.txt"
        folders_path = RAW_EDGAR_DIR / cik / "10-K"
        dst_root = INTERIM_CLEANED_DIR / cik / "10-K"
        for acc_dir in folders_path.iterdir():
            src_file = acc_dir / output_filename
            html_content = cleaning_items(print_clean_txt(src_file))

            dst_dir = dst_root / acc_dir.name
            dst_dir.mkdir(parents=True, exist_ok=True)

            dst_file = dst_dir / output_filename
            print(f"save path: {dst_file}")

            print_10X(dst_file, html_content)
    except:
        print(f"Cleaning Failed on {cik}")
    return
