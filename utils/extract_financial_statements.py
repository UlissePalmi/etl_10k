"""
CLI wrapper for financial statement extraction.

The core extraction functions live in src/etl_10k/edgar/extract_financial_statements.py.
This script provides a standalone CLI for extracting financial statements without running
the full pipeline (useful for reruns or one-off extractions).

Usage:
    uv run python utils/extract_financial_statements.py --cik 0001562088
    uv run python utils/extract_financial_statements.py --ciks 0001562088,0000320193
    uv run python utils/extract_financial_statements.py  # Extract all CIKs
"""

import sys
from pathlib import Path

# Import the core module
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from etl_10k.edgar.extract_financial_statements import main

if __name__ == "__main__":
    main()
