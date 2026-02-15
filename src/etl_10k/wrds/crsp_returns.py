from etl_10k.config import INTERIM_ITEM1A_DIR, CIK_LIST, RETURNS_FILE
import pandas as pd
from sqlalchemy import text
import wrds


def querymaker(cik):
    """
    Build the WRDS SQL query to pull monthly CRSP returns for a single CIK.

    Returns a query string filtered to common link types and a fixed date range.
    """
    query = f"""
    SELECT 
        c.cik, 
        c.conm as company_name, 
        m.date, 
        m.ret
    FROM 
        crsp.msf as m
    JOIN 
        crsp.ccmxpf_linktable as link
        ON m.permno = link.lpermno
    JOIN
        comp.company as c
        ON link.gvkey = c.gvkey
    WHERE 
        c.cik = '{cik}'            
        AND m.date >= '2006-01-01'      
        AND m.date <= '2025-10-31'
        AND link.linktype IN ('LU', 'LC')
        AND link.linkprim IN ('P', 'C')
        AND m.date >= link.linkdt
        AND (m.date <= link.linkenddt OR link.linkenddt IS NULL)
    """
    return query

def df_with_returns():
    """
    Download and combine monthly return data for all CIK folders in `INTERIM_ITEM1A_DIR`.

    For each CIK, runs the WRDS query, keeps (cik, date, ret), and concatenates results
    into a single dataframe.
    """
    list_cik_df = pd.read_csv(CIK_LIST)
    
    db = wrds.Connection(wrds_username='username')
    
    ciks = list_cik_df['CIK'].unique().tolist()
    ciks = [str(cik).zfill(10) for cik in ciks]
    
    print(ciks)
    
    dfs = []

    for cik in ciks:
        query = querymaker(cik)
        try:
            with db.engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn)

            dfs.append(df)
            print(f"{cik}: ok ({len(df)} rows)")
        except Exception as e:
            print(f"{cik}: error: {e}")
    cols = ["cik", "date", "ret"]
    dfs = [df.reindex(columns=cols) for df in dfs if df is not None and not df.empty]
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    df["cik"] = df["cik"].astype(str).str.lstrip("0")
    df.to_csv(RETURNS_FILE, index=False)

def update_cik_list():
    old_ciks_df = pd.read_csv(CIK_LIST)
    return_df = pd.read_csv(RETURNS_FILE)
    cik_list = return_df["cik"].unique().tolist()
    cik_list_int = [int(x) for x in cik_list]
    mask = old_ciks_df["CIK"].isin(cik_list_int)
    filtered = old_ciks_df[mask]
    filtered.to_csv(CIK_LIST, index=False)
