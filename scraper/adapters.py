import io
import re
import csv
import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict
from .utils import attach_extras
import os, hashlib

DEFAULT_HEADERS = {
    "User-Agent": "HoldingsScraper/0.1 (+research use)",
    "Accept": "*/*",
    "Connection": "close",
}

CACHE_DIR = ".http_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(url: str) -> str:
    key = hashlib.sha256(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, key + ".bin")

def fetch_url(url: str, headers: Optional[Dict[str, str]] = None, use_cache: bool = True) -> bytes:
    cache_file = _cache_path(url)
    if use_cache and os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            return f.read()

    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)
    resp = requests.get(url, headers=h, timeout=30)
    resp.raise_for_status()
    data = resp.content

    if use_cache:
        with open(cache_file, "wb") as f:
            f.write(data)
    return data

def extract_text_with_selector(page_html: bytes, css_selector: str) -> Optional[str]:
    if not css_selector: return None
    soup = BeautifulSoup(page_html, "lxml")
    node = soup.select_one(css_selector)
    if not node: return None
    return node.get_text(strip=True)

def parse_asof_from_text(text: Optional[str]) -> Optional[str]:
    if not text: return None
    t = re.sub(r"\s+", " ", str(text).replace("\u00a0"," ")).strip()
    patterns = [
        r"as\s*of\s+([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",  # As of September 24, 2025
        r"as\s*of\s+(\d{1,2}-[A-Za-z]{3}-\d{4})",         # As of 24-Sep-2025
        r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",
        r"(\d{1,2}-[A-Za-z]{3}-\d{4})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, t, flags=re.I)
        if m:
            d = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(d): return d.date().isoformat()
    return None

DATE_COL_CANDIDATES = [
    "as_of_date","asofdate","effective_date","effectivedate",
    "date","report_date","reportdate"
]

def _infer_asof_from_df(df: pd.DataFrame) -> Optional[str]:
    for c in DATE_COL_CANDIDATES:
        if c in df.columns:
            s = df[c].dropna().astype(str)
            if not s.empty:
                d = pd.to_datetime(s.iloc[0], errors="coerce")
                if pd.notna(d):
                    return d.date().isoformat()
    return None

# -------- ETF via HTML table (e.g., GRNY) --------
def etf_from_html_table(page_url: str,
                        fund_id: str,
                        table_selector: str = "table",
                        as_of_selector: str = "") -> pd.DataFrame:
    html = fetch_url(page_url)
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one(table_selector)
    if table is None:
        raise RuntimeError(f"Table not found: {table_selector}")

    header_cells = table.select("thead th")
    if not header_cells:
        first_row = table.select_one("tr")
        header_cells = first_row.select("td") if first_row else []
    headers = [h.get_text(strip=True) for h in header_cells]

    rows = []
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if cells and len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    df = pd.DataFrame(rows)

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    colmap = {
        "ticker":"ticker","symbol":"ticker",
        "name":"name","holdingname":"name","security":"name","security_name":"name",
        "weight":"weight_pct","weight_%":"weight_pct","weight_percent":"weight_pct","marketvalue%":"weight_pct",
        "market_value":"market_value_usd","marketvalue":"market_value_usd",
        "shares":"shares","sharesquantity":"shares",
        "cusip":"cusip","fundcusip":"cusip",
        "isin":"isin","securityisin":"isin",
        "sedol":"sedol","securitysedol":"sedol",
    }
    df = df.rename(columns={c: colmap.get(c, c) for c in df.columns})

    for c in ["weight_pct","market_value_usd","shares"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                           .str.replace(r"[,%$]", "", regex=True)
                           .str.replace(",", "", regex=False))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    def _extract_asof(html_bytes, primary_sel: str):
        s = BeautifulSoup(html_bytes, "lxml")
        candidates = [primary_sel, "p.time-stamp.pt-3", "p.time-stamp", ".time-stamp"]
        for sel in candidates:
            if not sel: 
                continue
            node = s.select_one(sel)
            if node:
                txt = node.get_text(" ", strip=True)
                dt = parse_asof_from_text(txt)
                if dt: 
                    print(f"[GRNY] as-of via '{sel}': '{txt}' -> {dt}")
                    return dt
        # fallback: sears = BeautifulSoup(html_bytes, "lxml")ch whole page for any date
        txt = s.get_text(" ", strip=True)
        dt = parse_asof_from_text(txt)
        if dt: 
            print(f"[GRNY] as-of via <full-page>: -> {dt}")
        else:
            print(f"[GRNY] as-of not found. Tried {candidates}")
        return dt

    # call it
    as_of = _extract_asof(html, as_of_selector)

    core = ["fund_ticker","as_of_date",
        "ticker","name","cusip","isin","sedol",
        "shares","weight_pct","market_value_usd"]

    # index-align with the parsed table
    out = pd.DataFrame(index=df.index)
    out["fund_ticker"] = fund_id
    out["as_of_date"] = as_of if as_of else pd.NaT

    for c in core[2:]:
        out[c] = df[c] if c in df.columns else pd.NA

    # extras
    extra_cols = [c for c in df.columns if c not in out.columns]
    merged = out.join(df[extra_cols]) if extra_cols else out

    # pack + return only core + extras
    merged = attach_extras(merged, keep_cols=core)
    return merged[core + ["extras"]]

# -------- ETF via CSV (e.g., MPLY; generic) --------
def etf_from_csv(csv_url: str,
                 fund_id: str,
                 as_of_date: Optional[str] = None) -> pd.DataFrame:
    if not csv_url:
        raise ValueError(f"No CSV URL for {fund_id}")
    raw = fetch_url(csv_url)
    try:
        df = pd.read_csv(io.BytesIO(raw))
    except Exception:
        df = pd.read_csv(io.BytesIO(raw), sep=";")

    # normalize headers
    df.columns = [str(c).strip().lower().replace('"', '').replace(" ", "_") for c in df.columns]

    # drop columns you never want to keep (BEFORE extras calc)
    EXTRA_EXCLUDE = {"entityname", "entity_name"}
    drop_cols = [c for c in df.columns if c.lower() in EXTRA_EXCLUDE]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True, errors="ignore")

    # map to canonical names
    colmap = {
        "ticker":"ticker","symbol":"ticker",
        "name":"name","holdingname":"name","security":"name","security_name":"name",
        "weight":"weight_pct","weight_%":"weight_pct","weight_percent":"weight_pct","marketvalue%":"weight_pct",
        "market_value":"market_value_usd","marketvalue":"market_value_usd",
        "shares":"shares","sharesquantity":"shares",
        "cusip":"cusip","fundcusip":"cusip",
        "isin":"isin","securityisin":"isin",
        "sedol":"sedol","securitysedol":"sedol",
        "effectivedate":"as_of_date","effective_date":"as_of_date",
        "report_date":"as_of_date","reportdate":"as_of_date",
    }
    df = df.rename(columns={c: colmap.get(c, c) for c in df.columns})

    # numeric cleanup
    for c in ["weight_pct","market_value_usd","shares"]:
        if c in df.columns:
            df[c] = (df[c].astype(str)
                        .str.replace(r"[,%$]", "", regex=True)
                        .str.replace(",", "", regex=False))
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # infer as-of
    inferred = _infer_asof_from_df(df)
    as_of = as_of_date or inferred

    # assemble with index alignment
    core = ["fund_ticker","as_of_date",
            "ticker","name","cusip","isin","sedol",
            "shares","weight_pct","market_value_usd"]

    out = pd.DataFrame(index=df.index)
    out["fund_ticker"] = fund_id
    out["as_of_date"] = as_of if as_of else pd.NaT
    for c in core[2:]:
        out[c] = df[c] if c in df.columns else pd.NA

    # extras after drops
    extra_cols = [c for c in df.columns if c not in out.columns]
    merged = out.join(df[extra_cols]) if extra_cols else out

    merged = attach_extras(merged, keep_cols=core)
    return merged[core + ["extras"]]


# -------- IVES special-case CSV (odd preamble/headers) --------
def etf_from_ives(csv_url: str, fund_id: str) -> pd.DataFrame:
    raw = fetch_url(csv_url)
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.lstrip("\ufeff") for ln in text.split("\n")]

    # --- robust header detection: skip "Ticker Symbol:,IVES" preamble lines ---
    header_idx = None
    for i, ln in enumerate(lines[:200]):
        low = ln.lower()
        cols = [c.strip() for c in ln.split(",")]
        # must look like a real table header: >=4 columns AND includes 'ticker' AND one of known fields
        if ("ticker" in low) and len(cols) >= 4 and any(k in low for k in ["name","security","sedol","weight","market","shares"]):
            # explicitly skip "ticker symbol" preamble
            if "ticker symbol" in low and len(cols) < 4:
                continue
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("IVES: header row not found")
    
    # Moved debug print after DataFrame is created

    table_text = "\n".join(lines[header_idx:])
    print(f"[IVES] About to read table_text (first 5 lines):")
    for i, l in enumerate(table_text.splitlines()[:5]):
        print(f"[IVES] table_text[{i}]: {l}")
    try:
        df = pd.read_csv(io.StringIO(table_text), skip_blank_lines=True, quotechar='"')
    except Exception as e:
        print(f"[IVES] pandas read_csv failed: {e}\nTrying with sep=';' ...")
        try:
            df = pd.read_csv(io.StringIO(table_text), sep=';', skip_blank_lines=True, quotechar='"')
        except Exception as e2:
            print(f"[IVES] pandas read_csv with sep=';' also failed: {e2}")
            raise

    print(f"[IVES] df.columns={list(df.columns)}")
    print(f"[IVES] core_present={{k: (k in df.columns) for k in ['ticker','name','sedol','shares','market_value','weight']}}")

    # --- preamble above header: extract date and fund ticker ---
    preamble = "\n".join(lines[:header_idx])

    # as-of date
    as_of = None
    for pat in [r"([A-Za-z]+ \d{1,2}, \d{4})",   # September 24, 2025
        r"([A-Za-z]+ \d{1,2}, \d{4})",   # September 24, 2025
        r"(\d{1,2}/\d{1,2}/\d{4})",     # 09/24/2025
        r"(\d{4}-\d{2}-\d{2})",         # 2025-09-24
        r"(\d{1,2}-[A-Za-z]{3}-\d{4})"  # 24-Sep-2025
        ]:
        m = re.search(pat, preamble, flags=re.I)
        if m:
            d = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(d):
                as_of = d.date().isoformat()
                break

    # fund ticker from preamble; fallback to fund_id
    ft = None
    m = re.search(r"ticker\s*symbol\s*[:,\-]\s*['\"]?([A-Z0-9.\-]+)['\"]?", preamble, flags=re.I)
    if m:
        ft = m.group(1).upper()
    else:
        # handle CSV-like "Ticker Symbol:,IVES"
        for ln in lines[:header_idx]:
            if "ticker" in ln.lower() and "symbol" in ln.lower():
                parts = [p.strip().strip('"').strip("'") for p in ln.split(",") if p.strip()]
                if len(parts) >= 2 and re.fullmatch(r"[A-Za-z0-9.\-]+", parts[-1]):
                    ft = parts[-1].upper()
                    break
    if not ft:
        ft = fund_id


    table_text = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(table_text), skip_blank_lines=True, quotechar='"')
    except Exception as e:
        print(f"[IVES] pandas read_csv failed: {e}\nTrying with sep=';' ...")
        try:
            df = pd.read_csv(io.StringIO(table_text), sep=';', skip_blank_lines=True, quotechar='"')
        except Exception as e2:
            print(f"[IVES] pandas read_csv with sep=';' also failed: {e2}")
            raise

    print(f"[IVES] raw df shape={df.shape}")

    if df.empty:
        print("[IVES] DataFrame is empty after reading table_text. First 10 lines of table_text:")
        for i, l in enumerate(table_text.splitlines()[:10]):
            print(f"[IVES] table_text[{i}]: {l}")
        raise ValueError("IVES: empty table after header; aborting")

    # normalize headers
    df.columns = [c.strip().lower().replace('"','').replace(" ", "_") for c in df.columns]
    rename = {
        "ticker":"ticker","ticker_symbol":"ticker","symbol":"ticker",
        "name":"name","securityname":"name","security_name":"name","security":"name",
        "sedol":"sedol",
        "shares":"shares","sharesquantity":"shares","qty":"shares","quantity":"shares",
        "market_value":"market_value_usd","marketvalue":"market_value_usd",
        "market_value_$":"market_value_usd","marketvalue($)":"market_value_usd",
        "weight":"weight_pct","portfolio_weight":"weight_pct","portfolioweight":"weight_pct",
        "weight_percent":"weight_pct","weight_%":"weight_pct",
    }
    df = df.rename(columns={c: rename.get(c, c) for c in df.columns})

    # strip fully empty rows
    df = df.dropna(how="all")

    # coerce string cols for filtering
    for s in ["ticker","name"]:
        if s in df.columns:
            df[s] = df[s].astype(str).str.strip()

    # remove footer/notes
    bad_rows = pd.Series(False, index=df.index)
    if "ticker" in df.columns:
        bad_rows |= df["ticker"].str.match(r"(?i)disclosure|notes?|important|information|summary|as of|ticker\s*symbol")
    if "name" in df.columns:
        bad_rows |= df["name"].str.match(r"(?i)disclosure|notes?|important|information|summary")
    # rows with no portfolio data at all
    numeric_all_na = True
    for c in ["shares","weight_pct","market_value_usd"]:
        if c in df.columns:
            numeric_all_na = numeric_all_na & df[c].isna()
    bad_rows |= numeric_all_na.fillna(False)

    df = df[~bad_rows].reset_index(drop=True)

    # numeric cleanup
    for c in ["shares","market_value_usd","weight_pct"]:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                    .str.replace(r"[,$%]", "", regex=True)
                    .str.replace(",", "", regex=False)
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- normalized assembly with index alignment ---
    core = ["fund_ticker","as_of_date",
            "ticker","name","cusip","isin","sedol",
            "shares","weight_pct","market_value_usd"]

    out = pd.DataFrame(index=df.index)
    out["fund_ticker"] = ft  # not fund_id
    out["as_of_date"] = as_of if as_of else pd.NaT
    for c in core[2:]:
        out[c] = df[c] if c in df.columns else pd.NA

    extra_cols = [c for c in df.columns if c not in out.columns]
    merged = out.join(df[extra_cols]) if extra_cols else out

    # DEBUG: confirm core presence and row count
    print(f"[IVES] rows={len(merged)} core_present="
        f"{{k: (k in merged.columns) for k in {core}}}")

    merged = attach_extras(merged, keep_cols=core)
    return merged[core + ["extras"]]

