import pandas as pd

def feature_engineering(df):
    """
    Create model features from levenshtein, sentiment, and length-based inputs.

    Converts date columns to datetime and adds derived features used by the
    regression and classification models.
    """
    for col in ["date_a", "date_b"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["len_growth_pct"] = df['len_a'] / df['len_b'] - 1
    df["inc_len"] = df["len_a"] > df["len_b"]
    df = df.dropna()

    return df

def datatype_setup(sim_df, return_df):
    """
    Convert date columns to datetime and create gross returns (`retPlusOne`).
    """
    sim_df["date_a"] = pd.to_datetime(sim_df["date_a"],format="mixed",dayfirst=True)
    sim_df["date_b"] = pd.to_datetime(sim_df["date_b"],format="mixed",dayfirst=True)
    return_df["date"] = pd.to_datetime(return_df["date"])

    return_df['retPlusOne'] = return_df['ret'] + 1
    return sim_df, return_df

def merge_return(sim_df, return_df, months, period):
    """
    Merge feature rows with returns and compute window returns (past or future)
    over a `months` horizon, compounding `retPlusOne` within the window.
    """
    if period == 'future':
        sim_df["start_anchor"] = sim_df["date_a"]
        sim_df["end_anchor"]   = sim_df["start_anchor"] + pd.DateOffset(months=months)
    elif period == 'past':
        sim_df["end_anchor"]   = sim_df["date_a"]
        sim_df["start_anchor"] = sim_df["end_anchor"] - pd.DateOffset(months=months)
    
    sim_df = sim_df.reset_index().rename(columns={'index': 'sim_idx'}) 
    print(len(sim_df))
    # CIKs are turned into strings
    sim_df["cik"] = sim_df["cik"].astype(str).str.zfill(10)
    return_df = return_df.copy()
    return_df["cik"] = (return_df["cik"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(10))

    # Merge sim_df with returns on firm identifier
    merged = sim_df.merge(  
        return_df[["cik", "date", "retPlusOne"]],
        on="cik",
        how="left"
    )

    # Filter to dates in [prev_start, start_anchor)
    merged = merged[
        (merged['date'] >= merged['start_anchor']) &
        (merged['date'] <= merged['end_anchor'])     # use < if you want end exclusive
    ].sort_values(["sim_idx", "date"])

    # Compute return over that window for each original row
    prod_window = merged.groupby("sim_idx")["retPlusOne"].prod()
    sim_df[f"{period}_{months}m_ret"] = (sim_df["sim_idx"].map(prod_window) - 1) * 100
    
    
    col = f"{period}_{months}m_ret"
    print(sim_df[col].dtype)
    # Map back to sim_df as a new column
    sim_df = sim_df.dropna(subset=[f"{period}_{months}m_ret"])

    return sim_df.drop(columns=['start_anchor','end_anchor','sim_idx'])
