from collections import Counter
from etl_10k.text.segment import (
    item_dict_builder,
    table_content_builder,
    number_of_rounds,
)

def fallback_build_candidates(filepath, verbose):
    """
    Fallback version of candidate building. Same round-by-round strategy as the
    main algorithm, but skips advancing last_ele for items that appear fewer times
    than the estimated number of rounds. This allows body-only items (no TOC entry)
    to be included in every candidate round rather than being consumed by round 1.

    Args:
        filepath: Path to the filing to segment
        verbose: If True, print the candidate lists for debugging

    Returns:
        list[list[dict]]: One candidate sequence per round, each a list of
        dicts with 'item_num' and 'item_line'.
    """
    tableContent = table_content_builder(filepath)
    item_dict = item_dict_builder(filepath)

    item_counts = Counter(r['item_num'] for r in item_dict)
    list_lines = []
    last_ele = 0
    rounds = number_of_rounds(item_dict, bool=True)
    for _ in range(rounds):
        lines = []
        for itemTC in tableContent:
            for r in item_dict:
                if itemTC == r.get('item_num') and r.get('item_line') > last_ele:
                    lines.append(r)
                    if item_counts[r['item_num']] >= rounds:
                        last_ele = r['item_line']
                    break
        list_lines.append(lines)

    if verbose:
        print(f"Candidates: {list_lines}")
    
    return list_lines
