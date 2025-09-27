# streamlit_app.py
# UI polished + modal diffs + safe number formatting (no sprintf patterns).

import os, json, shutil, subprocess, sys
from typing import Dict, Tuple, List
import pandas as pd
import streamlit as st

# ------------------ CONFIG ------------------
DATA_PATH = os.getenv("DATA_PATH", "data/holdings_latest.json")
PREV_PATH = os.getenv("PREV_PATH", "data/holdings_prev.json")
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

# ------------------ PAGE + CSS ------------------
st.set_page_config(page_title="Fund Holdings", layout="wide")

def _inject_css():
    st.markdown(
        """
        <style>
        .app-header {
          padding: 20px 24px;
          background: linear-gradient(135deg, #eef2ff, #ffffff);
          border: 1px solid #e5e7eb;
          border-radius: 16px;
          box-shadow: 0 1px 2px rgba(0,0,0,0.04);
          margin-bottom: 12px;
        }
        .app-title { font-weight: 800; font-size: 28px; margin: 0; color: #111827; }
        .app-subtitle { color:#475569; margin-top:4px; font-size:14px }
        .stMetric { border-radius: 14px; padding: 10px; background: #ffffff; border: 1px solid #e5e7eb; }
        .block-container { padding-top: 24px; }
        .stExpander { border: 1px solid #e5e7eb; border-radius: 12px; }
        .stButton>button {
          border-radius: 12px; padding: 10px 14px; border: 1px solid #e5e7eb;
          font-weight: 600;
        }
        .primary-btn>button { background: #4F46E5; color: white; border: none; }
        .ghost-btn>button { background: #ffffff; }
        .stDataFrame { border: 1px solid #e5e7eb; border-radius: 12px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

_inject_css()

st.markdown(
    """
    <div class="app-header">
      <h1 class="app-title"> Fund Holdings</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

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
    return df

def _s(v):
    if v is None: return ""
    if isinstance(v, float) and pd.isna(v): return ""
    return str(v).strip()

def _ident(row: pd.Series) -> str:
    cusip  = _s(row.get("cusip"))
    isin   = _s(row.get("isin"))
    sedol  = _s(row.get("sedol"))
    ticker = _s(row.get("ticker"))
    name   = _s(row.get("name"))
    return cusip or isin or sedol or f"{ticker}|{name}"

def _key_tuple(row: pd.Series) -> Tuple[str, str]:
    fund = _s(row.get("fund_ticker") or row.get("Fund Ticker"))
    raw = {
        "cusip":  row.get("cusip"),
        "isin":   row.get("isin"),
        "sedol":  row.get("sedol"),
        "ticker": row.get("ticker") or row.get("Ticker"),
        "name":   row.get("name") or row.get("Security Name"),
    }
    return (fund, _ident(pd.Series(raw)))

def _coerce_raw(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ren = {}
    if "Fund Ticker" in out.columns and "fund_ticker" not in out.columns: ren["Fund Ticker"] = "fund_ticker"
    if "Security Name" in out.columns and "name" not in out.columns: ren["Security Name"] = "name"
    if "Ticker" in out.columns and "ticker" not in out.columns: ren["Ticker"] = "ticker"
    if "Portfolio Weight" in out.columns and "weight_pct" not in out.columns: ren["Portfolio Weight"] = "weight_pct"
    if "Market Value (USD)" in out.columns and "market_value_usd" not in out.columns: ren["Market Value (USD)"] = "market_value_usd"
    if ren: out = out.rename(columns=ren)
    for c in DIFF_NUMERIC_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

def compute_diffs(new_df: pd.DataFrame, old_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    new_raw = _coerce_raw(new_df)
    old_raw = _coerce_raw(old_df)

    new_map = { _key_tuple(r): r for _, r in new_raw.iterrows() }
    old_map = { _key_tuple(r): r for _, r in old_raw.iterrows() }

    new_keys, old_keys = set(new_map), set(old_map)
    added_df = pd.DataFrame([new_map[k] for k in sorted(new_keys - old_keys)]) if (new_keys - old_keys) else pd.DataFrame(columns=new_raw.columns)
    removed_df = pd.DataFrame([old_map[k] for k in sorted(old_keys - new_keys)]) if (old_keys - new_keys) else pd.DataFrame(columns=old_raw.columns)

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
    changed_df = pd.DataFrame(changed_records)

    rename = DISPLAY_NAMES.copy()
    rename.update({
        "weight_pct_old":"Portfolio Weight (old)","weight_pct_new":"Portfolio Weight (new)","weight_pct_delta":"Portfolio Weight (Δ)",
        "shares_old":"Shares (old)","shares_new":"Shares (new)","shares_delta":"Shares (Δ)",
        "market_value_usd_old":"Market Value USD (old)","market_value_usd_new":"Market Value USD (new)","market_value_usd_delta":"Market Value USD (Δ)",
    })
    added_df = added_df.rename(columns=DISPLAY_NAMES)
    removed_df = removed_df.rename(columns=DISPLAY_NAMES)
    changed_df = changed_df.rename(columns=rename)

    return {"added": added_df, "removed": removed_df, "changed": changed_df}

# ------------------ DISPLAY HELPERS ------------------
def style_changed(df: pd.DataFrame):
    fmt_map = {}
    if "Market Value USD (Δ)" in df.columns: fmt_map["Market Value USD (Δ)"] = "{:+,.0f}"
    if "Market Value USD (old)" in df.columns: fmt_map["Market Value USD (old)"] = "{:,.0f}"
    if "Market Value USD (new)" in df.columns: fmt_map["Market Value USD (new)"] = "{:,.0f}"
    if "Shares (Δ)" in df.columns: fmt_map["Shares (Δ)"] = "{:+,.0f}"
    if "Shares (old)" in df.columns: fmt_map["Shares (old)"] = "{:,.0f}"
    if "Shares (new)" in df.columns: fmt_map["Shares (new)"] = "{:,.0f}"
    if "Portfolio Weight (Δ)" in df.columns: fmt_map["Portfolio Weight (Δ)"] = "{:+.2f}%"
    if "Portfolio Weight (old)" in df.columns: fmt_map["Portfolio Weight (old)"] = "{:.2f}%"
    if "Portfolio Weight (new)" in df.columns: fmt_map["Portfolio Weight (new)"] = "{:.2f}%"

    def color_delta(v):
        try:
            if pd.isna(v): return ""
            if float(v) > 0: return "color: #16a34a;"
            if float(v) < 0: return "color: #dc2626;"
        except Exception:
            return ""
        return ""

    styler = df.style.format(fmt_map)
    for col in [c for c in df.columns if "Δ" in c]:
        styler = styler.applymap(color_delta, subset=[col])
    return styler

def prettify_numbers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Market Value (USD)" in out.columns:
        out["Market Value (USD)"] = out["Market Value (USD)"].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) else ""
        )
    if "Portfolio Weight" in out.columns:
        out["Portfolio Weight"] = out["Portfolio Weight"].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) else ""
        )
    if "Shares" in out.columns:
        out["Shares"] = out["Shares"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else ""
        )
    return out

# ------------------ MODAL DIALOG FOR DIFFS ------------------
@st.dialog("Changes Made", width="large")
def show_diffs_modal():
    try:
        current = load_json(DATA_PATH)
        if not os.path.exists(PREV_PATH):
            st.warning("No changes found.")
            return
        previous = load_json(PREV_PATH)
        diffs = compute_diffs(current, previous)

        view = st.radio("View", ["Added", "Removed", "Changed"], horizontal=True, key="diffs_view_radio")

        if view == "Added":
            if diffs["added"].empty:
                st.success("No new holdings added.")
            else:
                st.dataframe(prettify_numbers(diffs["added"]), width='stretch', hide_index=True)

        elif view == "Removed":
            if diffs["removed"].empty:
                st.info("No holdings removed.")
            else:
                st.dataframe(prettify_numbers(diffs["removed"]), width='stretch', hide_index=True)

        else:
            if diffs["changed"].empty:
                st.info("No changes in weight, shares, or market value.")
            else:
                st.dataframe(style_changed(diffs["changed"]), width='stretch', hide_index=True)
    except Exception as e:
        st.error(str(e))

# ------------------ LOAD CURRENT ------------------
try:
    df_raw = load_json(DATA_PATH)
except Exception as e:
    st.error(str(e)); st.stop()

# ------------------ CONTROLS (single row) ------------------
c1, c2, spacer = st.columns([1, 1, 6])
with c1:
    if st.button("Refresh Fund Data", key="btn_refresh"):
        os.makedirs(os.path.dirname(PREV_PATH) or ".", exist_ok=True)
        if os.path.exists(DATA_PATH):
            shutil.copy2(DATA_PATH, PREV_PATH)
        try:
            subprocess.run(SCRAPER_CMD, check=True)
            load_json.clear()
            df_raw = load_json(DATA_PATH)
            st.success("Refresh complete.")
        except subprocess.CalledProcessError as e:
            st.error(f"Scraper failed with exit code {e.returncode}")
        except Exception as e:
            st.error(str(e))

with c2:
    if st.button("Show Changes", key="btn_changes"):
        show_diffs_modal()

# ------------------ FILTERS (on RAW) ------------------
with st.expander("Filters", expanded=False):
    col1, col2, col3, col4 = st.columns(4)

    fund = "(All)"
    sector = "(All)"
    if "fund_ticker" in df_raw.columns:
        fund_opts = ["(All)"] + sorted(df_raw["fund_ticker"].dropna().astype(str).unique().tolist())
        fund = col1.selectbox("Fund", fund_opts)

    ticker_q = col2.text_input("Ticker contains", placeholder="e.g. AAPL")
    name_q   = col3.text_input("Name contains", placeholder="Any part of security name")

    if "sector" in df_raw.columns:
        sector_opts = ["(All)"] + sorted(df_raw["sector"].dropna().astype(str).unique().tolist())
        sector = col4.selectbox("Sector", sector_opts)

mask = pd.Series(True, index=df_raw.index)
if "fund_ticker" in df_raw.columns and fund != "(All)":
    mask &= (df_raw["fund_ticker"].astype(str) == fund)
if ticker_q and "ticker" in df_raw.columns:
    mask &= df_raw["ticker"].astype(str).str.contains(ticker_q, case=False, na=False)
if name_q and "name" in df_raw.columns:
    mask &= df_raw["name"].astype(str).str.contains(name_q, case=False, na=False)
if "sector" in df_raw.columns and sector != "(All)":
    mask &= (df_raw["sector"].astype(str) == sector)

subset_raw = df_raw[mask].copy()

# ------------------ KPIs (from filtered RAW) ------------------
k1, k2, k3 = st.columns(3)
k1.metric("Rows", f"{len(subset_raw):,}")
if "market_value_usd" in subset_raw.columns:
    total_mv = pd.to_numeric(subset_raw["market_value_usd"], errors="coerce").fillna(0).sum()
    k2.metric("Total Market Value", f"${int(total_mv):,}")
if "weight_pct" in subset_raw.columns:
    avg_w = pd.to_numeric(subset_raw["weight_pct"], errors="coerce").dropna().mean()
    k3.metric("Avg Weight", f"{avg_w:.2f}%")

# ------------------ TABLE ------------------
display_df = subset_raw.rename(columns=DISPLAY_NAMES)
st.dataframe(
    prettify_numbers(display_df),
    width='stretch',
    hide_index=True,
)

# ------------------ DOWNLOAD ------------------
st.download_button(
    "Download filtered CSV",
    display_df.to_csv(index=False).encode("utf-8"),
    file_name="holdings_filtered.csv",
    mime="text/csv",
    key="btn_download_csv",
)

# # streamlit_app.py
# # Full replacement: single-row controls, modal diff popup, filtered KPIs, pretty formatting.

# import os, json, shutil, subprocess, sys
# from typing import Dict, Tuple, List
# import pandas as pd
# import streamlit as st

# # ------------------ CONFIG ------------------
# DATA_PATH = os.getenv("DATA_PATH", "data/holdings_latest.json")
# PREV_PATH = os.getenv("PREV_PATH", "data/holdings_prev.json")  # snapshot file
# SCRAPER_CMD = [
#     sys.executable, "-m", "scraper.main",
#     "--config", os.getenv("SCRAPER_CONFIG", "config.yml"),
#     "--out",    os.getenv("SCRAPER_OUT",    "data/holdings_latest.csv"),
# ]

# SHOW_COLUMNS = [
#     "fund_ticker","as_of_date","ticker","name",
#     "shares","weight_pct","market_value_usd","sector","country",
# ]
# DISPLAY_NAMES = {
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
# NUMERIC_COLS = ["shares", "weight_pct", "market_value_usd"]
# DIFF_NUMERIC_COLS = ["shares", "weight_pct", "market_value_usd"]

# # ------------------ UI ------------------
# st.set_page_config(page_title="ETF Holdings", layout="wide")
# st.title("ETF Holdings")

# # ------------------ LOADERS ------------------
# @st.cache_data(show_spinner=False)
# def load_json(path: str) -> pd.DataFrame:
#     if not os.path.exists(path):
#         raise FileNotFoundError(f"Missing file: {path}")
#     with open(path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     if not isinstance(data, list):
#         raise ValueError("holdings_latest.json must be a JSON array")
#     df = pd.DataFrame(data)
#     keep = [c for c in SHOW_COLUMNS if c in df.columns]
#     df = df[keep].copy()
#     for c in NUMERIC_COLS:
#         if c in df.columns:
#             df[c] = pd.to_numeric(df[c], errors="coerce")
#     return df

# def _s(v):
#     if v is None: return ""
#     if isinstance(v, float) and pd.isna(v): return ""
#     return str(v).strip()

# def _ident(row: pd.Series) -> str:
#     cusip  = _s(row.get("cusip"))
#     isin   = _s(row.get("isin"))
#     sedol  = _s(row.get("sedol"))
#     ticker = _s(row.get("ticker"))
#     name   = _s(row.get("name"))
#     return cusip or isin or sedol or f"{ticker}|{name}"

# def _key_tuple(row: pd.Series) -> Tuple[str, str]:
#     fund = _s(row.get("fund_ticker") or row.get("Fund Ticker"))
#     raw = {
#         "cusip":  row.get("cusip"),
#         "isin":   row.get("isin"),
#         "sedol":  row.get("sedol"),
#         "ticker": row.get("ticker") or row.get("Ticker"),
#         "name":   row.get("name") or row.get("Security Name"),
#     }
#     return (fund, _ident(pd.Series(raw)))

# def _coerce_raw(df: pd.DataFrame) -> pd.DataFrame:
#     out = df.copy()
#     ren = {}
#     if "Fund Ticker" in out.columns and "fund_ticker" not in out.columns: ren["Fund Ticker"] = "fund_ticker"
#     if "Security Name" in out.columns and "name" not in out.columns: ren["Security Name"] = "name"
#     if "Ticker" in out.columns and "ticker" not in out.columns: ren["Ticker"] = "ticker"
#     if "Portfolio Weight" in out.columns and "weight_pct" not in out.columns: ren["Portfolio Weight"] = "weight_pct"
#     if "Market Value (USD)" in out.columns and "market_value_usd" not in out.columns: ren["Market Value (USD)"] = "market_value_usd"
#     if ren: out = out.rename(columns=ren)
#     for c in DIFF_NUMERIC_COLS:
#         if c in out.columns:
#             out[c] = pd.to_numeric(out[c], errors="coerce")
#     return out

# def compute_diffs(new_df: pd.DataFrame, old_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
#     new_raw = _coerce_raw(new_df)
#     old_raw = _coerce_raw(old_df)

#     new_map = { _key_tuple(r): r for _, r in new_raw.iterrows() }
#     old_map = { _key_tuple(r): r for _, r in old_raw.iterrows() }

#     new_keys, old_keys = set(new_map), set(old_map)
#     added_df = pd.DataFrame([new_map[k] for k in sorted(new_keys - old_keys)]) if (new_keys - old_keys) else pd.DataFrame(columns=new_raw.columns)
#     removed_df = pd.DataFrame([old_map[k] for k in sorted(old_keys - new_keys)]) if (old_keys - new_keys) else pd.DataFrame(columns=old_raw.columns)

#     changed_records: List[Dict] = []
#     for k in (new_keys & old_keys):
#         nr, orow = new_map[k], old_map[k]
#         changed = False
#         rec = {"fund_ticker": nr.get("fund_ticker"), "ticker": nr.get("ticker"), "name": nr.get("name")}
#         for c in DIFF_NUMERIC_COLS:
#             nval = pd.to_numeric(nr.get(c), errors="coerce")
#             oval = pd.to_numeric(orow.get(c), errors="coerce")
#             if (pd.isna(nval) and pd.isna(oval)): continue
#             if (pd.isna(nval) != pd.isna(oval)) or float(nval or 0) != float(oval or 0):
#                 changed = True
#                 rec[f"{c}_old"] = oval; rec[f"{c}_new"] = nval; rec[f"{c}_delta"] = (nval or 0) - (oval or 0)
#         if changed: changed_records.append(rec)
#     changed_df = pd.DataFrame(changed_records)

#     rename = DISPLAY_NAMES.copy()
#     rename.update({
#         "weight_pct_old":"Portfolio Weight (old)","weight_pct_new":"Portfolio Weight (new)","weight_pct_delta":"Portfolio Weight (Δ)",
#         "shares_old":"Shares (old)","shares_new":"Shares (new)","shares_delta":"Shares (Δ)",
#         "market_value_usd_old":"Market Value USD (old)","market_value_usd_new":"Market Value USD (new)","market_value_usd_delta":"Market Value USD (Δ)",
#     })
#     added_df = added_df.rename(columns=DISPLAY_NAMES)
#     removed_df = removed_df.rename(columns=DISPLAY_NAMES)
#     changed_df = changed_df.rename(columns=rename)

#     return {"added": added_df, "removed": removed_df, "changed": changed_df}

# # ------------------ DISPLAY HELPERS ------------------
# def format_display(df: pd.DataFrame) -> pd.DataFrame:
#     out = df.copy()
#     out = out.rename(columns=DISPLAY_NAMES)
#     if "Market Value (USD)" in out.columns:
#         out["Market Value (USD)"] = out["Market Value (USD)"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")
#     if "Portfolio Weight" in out.columns:
#         out["Portfolio Weight"] = out["Portfolio Weight"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
#     if "Shares" in out.columns:
#         out["Shares"] = out["Shares"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
#     return out

# def style_changed(df: pd.DataFrame):
#     fmt_map = {}
#     if "Market Value USD (Δ)" in df.columns: fmt_map["Market Value USD (Δ)"] = "{:+,.0f}"
#     if "Market Value USD (old)" in df.columns: fmt_map["Market Value USD (old)"] = "{:,.0f}"
#     if "Market Value USD (new)" in df.columns: fmt_map["Market Value USD (new)"] = "{:,.0f}"
#     if "Shares (Δ)" in df.columns: fmt_map["Shares (Δ)"] = "{:+,.0f}"
#     if "Shares (old)" in df.columns: fmt_map["Shares (old)"] = "{:,.0f}"
#     if "Shares (new)" in df.columns: fmt_map["Shares (new)"] = "{:,.0f}"
#     if "Portfolio Weight (Δ)" in df.columns: fmt_map["Portfolio Weight (Δ)"] = "{:+.2f}%"
#     if "Portfolio Weight (old)" in df.columns: fmt_map["Portfolio Weight (old)"] = "{:.2f}%"
#     if "Portfolio Weight (new)" in df.columns: fmt_map["Portfolio Weight (new)"] = "{:.2f}%"

#     def color_delta(v):
#         try:
#             if pd.isna(v): return ""
#             if float(v) > 0: return "color: green;"
#             if float(v) < 0: return "color: red;"
#         except Exception:
#             return ""
#         return ""

#     styler = df.style.format(fmt_map)
#     for col in [c for c in df.columns if "Δ" in c]:
#         styler = styler.applymap(color_delta, subset=[col])
#     return styler

# # ------------------ MODAL DIALOG FOR DIFFS ------------------
# @st.dialog("Changes vs previous", width="large")
# def show_diffs_modal():
#     try:
#         current = load_json(DATA_PATH)
#         if not os.path.exists(PREV_PATH):
#             st.warning("No previous snapshot found.")
#             return
#         previous = load_json(PREV_PATH)
#         diffs = compute_diffs(current, previous)

#         view = st.radio("View", ["Added", "Removed", "Changed"], horizontal=True, key="diffs_view_radio")

#         if view == "Added":
#             if diffs["added"].empty:
#                 st.success("No new holdings added.")
#             else:
#                 st.dataframe(format_display(diffs["added"]), use_container_width=True, hide_index=True)

#         elif view == "Removed":
#             if diffs["removed"].empty:
#                 st.info("No holdings removed.")
#             else:
#                 st.dataframe(format_display(diffs["removed"]), use_container_width=True, hide_index=True)

#         else:
#             if diffs["changed"].empty:
#                 st.info("No changes in weight, shares, or market value.")
#             else:
#                 st.dataframe(style_changed(diffs["changed"]), use_container_width=True, hide_index=True)
#     except Exception as e:
#         st.error(str(e))

# # ------------------ LOAD CURRENT ------------------
# try:
#     df_raw = load_json(DATA_PATH)
# except Exception as e:
#     st.error(str(e)); st.stop()

# # ------------------ CONTROLS (single row) ------------------
# c1, c2, spacer = st.columns([1, 1, 6])

# with c1:
#     if st.button("Refresh data (run scraper)", use_container_width=True, key="btn_refresh"):
#         os.makedirs(os.path.dirname(PREV_PATH) or ".", exist_ok=True)
#         if os.path.exists(DATA_PATH):
#             shutil.copy2(DATA_PATH, PREV_PATH)
#         try:
#             subprocess.run(SCRAPER_CMD, check=True)
#             load_json.clear()
#             df_raw = load_json(DATA_PATH)
#             st.success("Refresh complete.")
#         except subprocess.CalledProcessError as e:
#             st.error(f"Scraper failed with exit code {e.returncode}")
#         except Exception as e:
#             st.error(str(e))

# with c2:
#     if st.button("Show changes vs previous", use_container_width=True, key="btn_changes"):
#         show_diffs_modal()  # open the modal immediately

# # ------------------ FILTERS (on RAW) ------------------
# with st.expander("Filters", expanded=False):
#     col1, col2, col3, col4 = st.columns(4)

#     fund = "(All)"
#     sector = "(All)"
#     if "fund_ticker" in df_raw.columns:
#         fund_opts = ["(All)"] + sorted(df_raw["fund_ticker"].dropna().astype(str).unique().tolist())
#         fund = col1.selectbox("Fund", fund_opts)

#     ticker_q = col2.text_input("Ticker contains")
#     name_q   = col3.text_input("Name contains")

#     if "sector" in df_raw.columns:
#         sector_opts = ["(All)"] + sorted(df_raw["sector"].dropna().astype(str).unique().tolist())
#         sector = col4.selectbox("Sector", sector_opts)

# mask = pd.Series(True, index=df_raw.index)
# if "fund_ticker" in df_raw.columns and fund != "(All)":
#     mask &= (df_raw["fund_ticker"].astype(str) == fund)
# if ticker_q and "ticker" in df_raw.columns:
#     mask &= df_raw["ticker"].astype(str).str.contains(ticker_q, case=False, na=False)
# if name_q and "name" in df_raw.columns:
#     mask &= df_raw["name"].astype(str).str.contains(name_q, case=False, na=False)
# if "sector" in df_raw.columns and sector != "(All)":
#     mask &= (df_raw["sector"].astype(str) == sector)

# subset_raw = df_raw[mask].copy()

# # ------------------ KPIs (from filtered RAW) ------------------
# k1, k2, k3 = st.columns(3)
# k1.metric("Rows", f"{len(subset_raw):,}")

# if "market_value_usd" in subset_raw.columns:
#     total_mv = pd.to_numeric(subset_raw["market_value_usd"], errors="coerce").fillna(0).sum()
#     k2.metric("Total Market Value", f"${int(total_mv):,}")

# if "weight_pct" in subset_raw.columns:
#     avg_w = pd.to_numeric(subset_raw["weight_pct"], errors="coerce").dropna().mean()
#     k3.metric("Avg Weight", f"{avg_w:.2f}%")

# # ------------------ TABLE (pretty view of SAME filtered rows) ------------------
# display_df = subset_raw.rename(columns=DISPLAY_NAMES)
# st.dataframe(
#     format_display(display_df),
#     use_container_width=True,
#     hide_index=True,
# )

# # ------------------ DOWNLOAD ------------------
# st.download_button(
#     "Download filtered CSV",
#     display_df.to_csv(index=False).encode("utf-8"),
#     file_name="holdings_filtered.csv",
#     mime="text/csv",
#     key="btn_download_csv",
# )


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