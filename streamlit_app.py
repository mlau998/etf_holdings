import os, json, shutil, subprocess, sys
from typing import Dict, Tuple, List
import pandas as pd
import streamlit as st

# ------------------ CONFIG ------------------
DATA_PATH = os.getenv("DATA_PATH", "data/holdings_latest.json")
PREV_PATH = os.getenv("PREV_PATH", "data/holdings_prev.json")  # snapshot file
SCRAPER_CMD = [
    sys.executable, "-m", "scraper.main",
    "--config", os.getenv("SCRAPER_CONFIG", "config.yml"),
    "--out",    os.getenv("SCRAPER_OUT",    "data/holdings_latest.csv"),
]

SHOW_COLUMNS = [
    "fund_ticker","as_of_date","ticker","name",
    "shares","weight_pct","market_value_usd","sector","country",
]
DISPLAY_NAMES = {
    "fund_ticker": "Fund Ticker",
    "as_of_date": "As of",
    "ticker": "Ticker",
    "name": "Security Name",
    "shares": "Shares",
    "weight_pct": "Portfolio Weight",
    "market_value_usd": "Market Value (USD)",
    "sector": "Sector",
    "country": "Country",
}
NUMERIC_COLS = ["shares", "weight_pct", "market_value_usd"]
DIFF_NUMERIC_COLS = ["shares", "weight_pct", "market_value_usd"]

# ------------------ UI ------------------
st.set_page_config(page_title="ETF Holdings", layout="wide")
st.title("ETF Holdings")

# ------------------ LOADERS ------------------
@st.cache_data(show_spinner=False)
def load_json(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("holdings_latest.json must be a JSON array")
    df = pd.DataFrame(data)
    keep = [c for c in SHOW_COLUMNS if c in df.columns]
    df = df[keep].copy()
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.rename(columns=DISPLAY_NAMES)
    return df

def _ident(row: pd.Series) -> str:
    cusip = str(row.get("cusip") or "").strip()
    isin  = str(row.get("isin") or "").strip()
    sedol = str(row.get("sedol") or "").strip()
    ticker = str(row.get("ticker") or "").strip()
    name   = str(row.get("name") or "").strip()
    return cusip or isin or sedol or f"{ticker}|{name}"

def _key_tuple(row: pd.Series) -> Tuple[str, str]:
    fund = str(row.get("fund_ticker") or row.get("Fund Ticker") or "").strip()
    raw = {
        "cusip":  row.get("cusip"),
        "isin":   row.get("isin"),
        "sedol":  row.get("sedol"),
        "ticker": row.get("ticker") or row.get("Ticker"),
        "name":   row.get("name") or row.get("Security Name"),
    }
    return (fund, _ident(pd.Series(raw)))

def _coerce_raw(df: pd.DataFrame) -> pd.DataFrame:
    if "Fund Ticker" in df.columns and "fund_ticker" not in df.columns:
        df = df.rename(columns={"Fund Ticker":"fund_ticker"})
    if "Security Name" in df.columns and "name" not in df.columns:
        df = df.rename(columns={"Security Name":"name"})
    if "Ticker" in df.columns and "ticker" not in df.columns:
        df = df.rename(columns={"Ticker":"ticker"})
    if "Portfolio Weight" in df.columns and "weight_pct" not in df.columns:
        df = df.rename(columns={"Portfolio Weight":"weight_pct"})
    if "Market Value (USD)" in df.columns and "market_value_usd" not in df.columns:
        df = df.rename(columns={"Market Value (USD)":"market_value_usd"})
    return df

def compute_diffs(new_df: pd.DataFrame, old_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    new_raw = _coerce_raw(new_df.copy()); old_raw = _coerce_raw(old_df.copy())
    new_map = { _key_tuple(r): r for _, r in new_raw.iterrows() }
    old_map = { _key_tuple(r): r for _, r in old_raw.iterrows() }

    new_keys, old_keys = set(new_map), set(old_map)
    added = pd.DataFrame([new_map[k] for k in sorted(new_keys - old_keys)]) if new_keys - old_keys else pd.DataFrame(columns=new_raw.columns)
    removed = pd.DataFrame([old_map[k] for k in sorted(old_keys - new_keys)]) if old_keys - new_keys else pd.DataFrame(columns=old_raw.columns)

    changed_records: List[Dict] = []
    for k in (new_keys & old_keys):
        nr, orow = new_map[k], old_map[k]
        changed = False
        rec = {"fund_ticker": nr.get("fund_ticker"), "ticker": nr.get("ticker"), "name": nr.get("name")}
        for c in DIFF_NUMERIC_COLS:
            nval = pd.to_numeric(nr.get(c), errors="coerce")
            oval = pd.to_numeric(orow.get(c), errors="coerce")
            if (pd.isna(nval) and pd.isna(oval)): continue
            if (pd.isna(nval) != pd.isna(oval)) or float(nval or 0) != float(oval or 0):
                changed = True
                rec[f"{c}_old"] = oval; rec[f"{c}_new"] = nval; rec[f"{c}_delta"] = (nval or 0) - (oval or 0)
        if changed: changed_records.append(rec)
    changed = pd.DataFrame(changed_records)

    rename = DISPLAY_NAMES.copy()
    rename.update({
        "weight_pct_old":"Portfolio Weight (old)","weight_pct_new":"Portfolio Weight (new)","weight_pct_delta":"Portfolio Weight (Δ)",
        "shares_old":"Shares (old)","shares_new":"Shares (new)","shares_delta":"Shares (Δ)",
        "market_value_usd_old":"Market Value USD (old)","market_value_usd_new":"Market Value USD (new)","market_value_usd_delta":"Market Value USD (Δ)",
    })
    return {
        "added": added.rename(columns=rename),
        "removed": removed.rename(columns=rename),
        "changed": changed.rename(columns=rename),
    }

# ------------------ LOAD CURRENT ------------------
try:
    df = load_json(DATA_PATH)
except Exception as e:
    st.error(str(e)); st.stop()

# ------------------ CONTROLS ------------------
left, mid, right = st.columns([1,1,2])

with left:
    if st.button("Refresh data (run scraper)", use_container_width=True):
        os.makedirs(os.path.dirname(PREV_PATH) or ".", exist_ok=True)
        if os.path.exists(DATA_PATH):
            shutil.copy2(DATA_PATH, PREV_PATH)
        try:
            subprocess.run(SCRAPER_CMD, check=True)
            load_json.clear()  # clear cache
            df = load_json(DATA_PATH)
            st.success("Refresh complete.")
        except subprocess.CalledProcessError as e:
            st.error(f"Scraper failed with exit code {e.returncode}")
        except Exception as e:
            st.error(str(e))

with mid:
    if st.button("Show changes vs previous", use_container_width=True):
        if not os.path.exists(PREV_PATH):
            st.warning("No previous snapshot found.")
        else:
            try:
                prev_df = load_json(PREV_PATH)
                diffs = compute_diffs(df, prev_df)
                a, r, c = st.tabs(["Added", "Removed", "Changed"])
                with a:
                    st.write(f"Added rows: {len(diffs['added'])}")
                    st.dataframe(diffs["added"], use_container_width=True, hide_index=True)
                with r:
                    st.write(f"Removed rows: {len(diffs['removed'])}")
                    st.dataframe(diffs["removed"], use_container_width=True, hide_index=True)
                with c:
                    st.write(f"Changed rows: {len(diffs['changed'])}")
                    st.dataframe(diffs["changed"], use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(str(e))

# ------------------ FILTERS ------------------
with st.expander("Filters", expanded=False):
    col1, col2, col3, col4 = st.columns(4)
    fund = "(All)"; sector = "(All)"
    if "Fund Ticker" in df.columns:
        fund = col1.selectbox("Fund", ["(All)"] + sorted(df["Fund Ticker"].dropna().unique().tolist()))
    ticker_q = col2.text_input("Ticker contains")
    name_q = col3.text_input("Name contains")
    if "Sector" in df.columns:
        sector = col4.selectbox("Sector", ["(All)"] + sorted(df["Sector"].dropna().unique().tolist()))

fdf = df.copy()
if "Fund Ticker" in fdf.columns and fund != "(All)":
    fdf = fdf[fdf["Fund Ticker"] == fund]
if ticker_q and "Ticker" in fdf.columns:
    fdf = fdf[fdf["Ticker"].astype(str).str.contains(ticker_q, case=False, na=False)]
if name_q and "Security Name" in fdf.columns:
    fdf = fdf[fdf["Security Name"].astype(str).str.contains(name_q, case=False, na=False)]
if "Sector" in fdf.columns and sector != "(All)":
    fdf = fdf[fdf["Sector"] == sector]

# ------------------ KPIs ------------------
k1, k2, k3 = st.columns(3)
k1.metric("Rows", f"{len(fdf):,}")
if "Market Value (USD)" in fdf.columns:
    k2.metric("Total Market Value", f"${int(fdf['Market Value (USD)'].fillna(0).sum()):,}")
if "Portfolio Weight" in fdf.columns:
    k3.metric("Avg Weight", f"{fdf['Portfolio Weight'].fillna(0).mean():.2f}%")

# ------------------ TABLE ------------------
st.dataframe(fdf, use_container_width=True, hide_index=True)

# ------------------ DOWNLOAD ------------------
st.download_button(
    "Download filtered CSV",
    fdf.to_csv(index=False).encode("utf-8"),
    file_name="holdings_filtered.csv",
    mime="text/csv",
)

# import os, json
# import pandas as pd
# import streamlit as st

# # --------- CONFIG ---------
# DATA_PATH = os.getenv("DATA_PATH", "data/holdings_latest.json")

# COLUMNS = [
#     "fund_ticker",
#     "as_of_date",
#     "ticker",
#     "name",
#     "shares",
#     "weight_pct",
#     "market_value_usd",
#     "sector",
#     "country",
# ]

# HEADER = {
#     "fund_ticker": "Fund Ticker",
#     "as_of_date": "As of",
#     "ticker": "Ticker",
#     "name": "Security Name",
#     "shares": "Shares",
#     "weight_pct": "Portfolio Weight",
#     "market_value_usd": "Market Value (USD)",
#     "sector": "Sector",
#     "country": "Country",
# }

# # --------- PAGE SETUP ---------
# st.set_page_config(page_title="Different Fund Holdings", layout="wide")
# st.title("Different Fund Holdings")

# # --------- LOAD DATA ---------
# @st.cache_data(show_spinner=False)
# def load_data(path: str) -> pd.DataFrame:
#     if not os.path.exists(path):
#         raise FileNotFoundError(f"Missing file: {path}")
#     with open(path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     if not isinstance(data, list):
#         raise ValueError("holdings.json must be a JSON array of objects")
#     df = pd.DataFrame(data)

#     # Keep only selected columns (if present)
#     keep = [c for c in COLUMNS if c in df.columns]
#     df = df[keep].copy()

#     # Types/format
#     if "shares" in df.columns:
#         df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
#     if "weight_pct" in df.columns:
#         df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
#     if "market_value_usd" in df.columns:
#         df["market_value_usd"] = pd.to_numeric(df["market_value_usd"], errors="coerce")

#     # Pretty headers
#     df = df.rename(columns=HEADER)
#     return df

# try:
#     df = load_data(DATA_PATH)
# except Exception as e:
#     st.error(str(e))
#     st.stop()

# # --------- FILTERS ---------
# with st.expander("Filters", expanded=False):
#     col1, col2, col3, col4 = st.columns(4)
#     fund = col1.selectbox("Fund", options=["(All)"] + sorted(df["Fund Ticker"].dropna().unique().tolist())) if "Fund Ticker" in df.columns else "(All)"
#     ticker_q = col2.text_input("Ticker contains")
#     name_q = col3.text_input("Name contains")
#     sector = col4.selectbox("Sector", options=["(All)"] + sorted(df["Sector"].dropna().unique().tolist())) if "Sector" in df.columns else "(All)"

# # apply filters
# fdf = df.copy()
# if "Fund Ticker" in fdf.columns and fund != "(All)":
#     fdf = fdf[fdf["Fund Ticker"] == fund]
# if ticker_q:
#     if "Ticker" in fdf.columns:
#         fdf = fdf[fdf["Ticker"].astype(str).str.contains(ticker_q, case=False, na=False)]
# if name_q:
#     if "Security Name" in fdf.columns:
#         fdf = fdf[fdf["Security Name"].astype(str).str.contains(name_q, case=False, na=False)]
# if "Sector" in fdf.columns and sector != "(All)":
#     fdf = fdf[fdf["Sector"] == sector]

# # --------- SUMMARY KPI ---------
# k1, k2, k3 = st.columns(3)
# with k1:
#     st.metric("Rows", f"{len(fdf):,}")
# with k2:
#     if "Market Value (USD)" in fdf.columns:
#         st.metric("Total Market Value", f"${int(fdf['Market Value (USD)'].fillna(0).sum()):,}")
# with k3:
#     if "Portfolio Weight" in fdf.columns:
#         st.metric("Avg Weight", f"{fdf['Portfolio Weight'].fillna(0).mean():.2f}%")

# # --------- TABLE ---------
# st.dataframe(
#     fdf,
#     use_container_width=True,
#     hide_index=True,
# )

# # --------- CSV DOWNLOAD ---------
# st.download_button(
#     label="Download filtered CSV",
#     data=fdf.to_csv(index=False).encode("utf-8"),
#     file_name="holdings_filtered.csv",
#     mime="text/csv",
# )