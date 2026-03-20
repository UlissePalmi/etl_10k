#!/usr/bin/env python3
"""
Clean the features dataset by removing obvious bad comparisons.

Filters out rows where either filing is too short, removes pairs that are
too far apart in time (>= 500 days), and saves the cleaned result.
"""

from pathlib import Path
import pandas as pd

ETL_10K = Path(__file__).parent.parent
FEATURES_FILE = ETL_10K / "data" / "interim" / "text_features" / "features.csv"


def remove_errors(df):
    """
    Remove obvious bad features rows and return a cleaned dataframe.

    Filters:
    - Very short filings: len_a < 75 and len_b < 75
    - Time gaps: days between filings >= 500 or <= 15
    """
    print(f"Starting with {len(df)} rows")

    # Remove filings that are too short
    df = df[(df["len_a"] >= 75) & (df["len_b"] >= 75)].copy()
    print(f"After removing short filings (<75 words): {len(df)} rows")

    # Convert dates to datetime
    df["date_a"] = pd.to_datetime(df["date_a"], format="mixed", dayfirst=True)
    df["date_b"] = pd.to_datetime(df["date_b"], format="mixed", dayfirst=True)

    # Calculate days between filings
    df['days'] = df['date_a'] - df['date_b']
    df['days'] = df['days'].dt.days.astype(int)

    # Remove pairs too far apart (>= 500 days)
    df = df[df['days'] < 500]
    print(f"After removing pairs >=500 days apart: {len(df)} rows")

    # Remove pairs too close together (<= 15 days)
    df = df[df['days'] > 15]
    print(f"After removing pairs <=15 days apart: {len(df)} rows")

    df = df.drop(columns='days')

    return df


if __name__ == "__main__":
    print(f"Reading features from: {FEATURES_FILE}\n")
    df = pd.read_csv(FEATURES_FILE)

    df_clean = remove_errors(df)

    print(f"\nSaving {len(df_clean)} cleaned rows to features.csv")
    df_clean.to_csv(FEATURES_FILE, index=False)
    print("✓ Done!")
