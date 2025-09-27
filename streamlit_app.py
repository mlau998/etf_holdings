import os, json
import pandas as pd
import streamlit as st

# --------- CONFIG ---------
DATA_PATH = os.getenv("DATA_PATH", "data/holdings.json")

COLUMNS = [
    "fund_ticker",
    "as_of_date",
    "ticker",
    "name",
    "shares",
    "weight_pct",
    "market_value_usd",
    "sector",
    "country",
]

HEADER = {
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

# --------- PAGE SETUP ---------
st.set_page_config(page_title="ETF Holdings", layout="wide")
st.title("ETF Holdings")

# --------- LOAD DATA ---------
@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("holdings.json must be a JSON array of objects")
    df = pd.DataFrame(data)

    # Keep only selected columns (if present)
    keep = [c for c in COLUMNS if c in df.columns]
    df = df[keep].copy()

    # Types/format
    if "shares" in df.columns:
        df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
    if "weight_pct" in df.columns:
        df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
    if "market_value_usd" in df.columns:
        df["market_value_usd"] = pd.to_numeric(df["market_value_usd"], errors="coerce")

    # Pretty headers
    df = df.rename(columns=HEADER)
    return df

try:
    df = load_data(DATA_PATH)
except Exception as e:
    st.error(str(e))
    st.stop()

# --------- FILTERS ---------
with st.expander("Filters", expanded=False):
    col1, col2, col3, col4 = st.columns(4)
    fund = col1.selectbox("Fund", options=["(All)"] + sorted(df["Fund Ticker"].dropna().unique().tolist())) if "Fund Ticker" in df.columns else "(All)"
    ticker_q = col2.text_input("Ticker contains")
    name_q = col3.text_input("Name contains")
    sector = col4.selectbox("Sector", options=["(All)"] + sorted(df["Sector"].dropna().unique().tolist())) if "Sector" in df.columns else "(All)"

# apply filters
fdf = df.copy()
if "Fund Ticker" in fdf.columns and fund != "(All)":
    fdf = fdf[fdf["Fund Ticker"] == fund]
if ticker_q:
    if "Ticker" in fdf.columns:
        fdf = fdf[fdf["Ticker"].astype(str).str.contains(ticker_q, case=False, na=False)]
if name_q:
    if "Security Name" in fdf.columns:
        fdf = fdf[fdf["Security Name"].astype(str).str.contains(name_q, case=False, na=False)]
if "Sector" in fdf.columns and sector != "(All)":
    fdf = fdf[fdf["Sector"] == sector]

# --------- SUMMARY KPI ---------
k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Rows", f"{len(fdf):,}")
with k2:
    if "Market Value (USD)" in fdf.columns:
        st.metric("Total Market Value", f"${int(fdf['Market Value (USD)'].fillna(0).sum()):,}")
with k3:
    if "Portfolio Weight" in fdf.columns:
        st.metric("Avg Weight", f"{fdf['Portfolio Weight'].fillna(0).mean():.2f}%")

# --------- TABLE ---------
st.dataframe(
    fdf,
    use_container_width=True,
    hide_index=True,
)

# --------- CSV DOWNLOAD ---------
st.download_button(
    label="Download filtered CSV",
    data=fdf.to_csv(index=False).encode("utf-8"),
    file_name="holdings_filtered.csv",
    mime="text/csv",
)