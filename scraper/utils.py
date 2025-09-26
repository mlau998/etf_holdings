import json
import re
import pandas as pd
import numpy as np

STD_COLUMNS = [
    "fund_ticker","as_of_date",
    "ticker","name","cusip","isin","sedol",
    "shares","weight_pct","market_value_usd","extras"
]

def _clean_id(x):
    if pd.isna(x): return x
    return re.sub(r"[\s\-]", "", str(x).upper())

def _clean_ticker(x):
    if pd.isna(x): return x
    t = str(x).strip().upper()
    return t if t else np.nan

def _to_numeric(s: pd.Series) -> pd.Series:
    return (s.astype(str)
             .str.replace(r"[,$%]", "", regex=True)
             .str.replace(",", "", regex=False)
             .pipe(pd.to_numeric, errors="coerce"))

def attach_extras(df: pd.DataFrame, keep_cols) -> pd.DataFrame:
    keep = set(keep_cols)
    extra_cols = [c for c in df.columns if c not in keep]
    if not extra_cols:
        df["extras"] = "{}"
        return df
    payloads = []
    for _, row in df.iterrows():
        payload = {c: row[c] for c in extra_cols if pd.notna(row[c]) and str(row[c]) != ""}
        payloads.append(json.dumps(payload, ensure_ascii=False))
    df["extras"] = payloads
    return df

def finalize_types(df: pd.DataFrame) -> pd.DataFrame:
    if "ticker" in df: df["ticker"] = df["ticker"].map(_clean_ticker)
    if "fund_ticker" in df: df["fund_ticker"] = df["fund_ticker"].map(_clean_ticker)
    for c in ["cusip","isin","sedol"]:
        if c in df: df[c] = df[c].map(_clean_id)
    for c in ["shares","market_value_usd","weight_pct"]:
        if c in df: df[c] = _to_numeric(df[c])
    if "as_of_date" in df:
        df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date.astype("string")
    return df

def concat_and_order(dfs):
    if not dfs:
        return pd.DataFrame(columns=STD_COLUMNS)
    cat = pd.concat(dfs, ignore_index=True)
    for col in STD_COLUMNS:
        if col not in cat.columns:
            cat[col] = pd.NA
    return cat[STD_COLUMNS]

