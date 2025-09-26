import re
import requests
import pandas as pd
from lxml import etree
from typing import Optional, Dict, Any, Tuple
import os, hashlib

CACHE_DIR = ".http_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(url: str) -> str:
    key = hashlib.sha256(url.encode()).hexdigest()
    return os.path.join(CACHE_DIR, key + ".bin")



SEC_HEADERS = {
    "User-Agent": "HoldingsScraper/0.1 (+research use)",
    "Accept": "application/json, text/plain, */*",
    "Connection": "close",
}

def _cik_digits(cik: str) -> str:
    only = re.sub(r"\D", "", cik)
    return only.lstrip("0") or "0"

def _submissions_url(cik: str) -> str:
    only = re.sub(r"\D", "", cik)
    return f"https://data.sec.gov/submissions/CIK{only.zfill(10)}.json"

def _filing_dir_index_url(cik_digits: str, accession_nodash: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{cik_digits}/{accession_nodash}/index.json"

def _fetch_json(url: str, use_cache: bool = True):
    cf = _cache_path(url)
    if use_cache and os.path.exists(cf):
        import json
        with open(cf, "rb") as f:
            return json.loads(f.read().decode("utf-8", errors="replace"))
    resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.text
    if use_cache:
        with open(cf, "wb") as f:
            f.write(data.encode("utf-8"))
    import json
    return json.loads(data)

def _fetch_bytes(url: str, use_cache: bool = True) -> bytes:
    cf = _cache_path(url)
    if use_cache and os.path.exists(cf):
        with open(cf, "rb") as f:
            return f.read()
    resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.content
    if use_cache:
        with open(cf, "wb") as f:
            f.write(data)
    return data


def latest_13f_info(cik: str) -> Optional[Dict[str, str]]:
    sub = _fetch_json(_submissions_url(cik))
    recent = sub.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    report_dates = recent.get("reportDate", [])
    for i, form in enumerate(forms):
        if str(form).upper().startswith("13F-HR"):
            return {"accession": accessions[i], "reportDate": report_dates[i]}
    return None

def load_latest_13f_table(cik: str) -> Tuple[pd.DataFrame, str, str]:
    info = latest_13f_info(cik)
    if not info:
        raise RuntimeError("No 13F-HR filing found")
    cik_digits = _cik_digits(cik)
    accession_nodash = info["accession"].replace("-", "")
    idx = _fetch_json(_filing_dir_index_url(cik_digits, accession_nodash))
    items = idx.get("directory", {}).get("item", [])
    xml_name = None
    for it in items:
        name = it.get("name", "").lower()
        if name.endswith(".xml") and ("info" in name or "13f" in name):
            xml_name = it["name"]
            break
    if not xml_name:
        for it in items:
            if it.get("name", "").lower().endswith(".xml"):
                xml_name = it["name"]
                break
    if not xml_name:
        raise RuntimeError("Could not locate 13F info table XML")

    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik_digits}/{accession_nodash}/{xml_name}"
    xml_bytes = _fetch_bytes(xml_url)
    root = etree.fromstring(xml_bytes)

    rows = []
    for it in root.findall(".//{*}infoTable") + root.findall(".//infoTable"):
        def text(path):
            node = it.find(path, namespaces=it.nsmap) if "{" in path else it.find(path)
            return node.text.strip() if node is not None and node.text else None
        name = text(".//{*}nameOfIssuer") or text("nameOfIssuer")
        title = text(".//{*}titleOfClass") or text("titleOfClass")
        cusip = text(".//{*}cusip") or text("cusip")
        value = text(".//{*}value") or text("value")
        shares = text(".//{*}shrsOrPrnAmt/{*}sshPrnamt") or text("shrsOrPrnAmt/sshPrnamt")
        rows.append({
            "name": name,
            "title_of_class": title,
            "cusip": cusip,
            "market_value_usd": float(value) * 1000 if value and value.isdigit() else None,
            "shares": int(shares) if shares and shares.isdigit() else None,
        })
    df = pd.DataFrame(rows)
    df["ticker"] = pd.NA
    df["weight_pct"] = pd.NA
    df["isin"] = pd.NA
    df["sedol"] = pd.NA
    return df, info["reportDate"], xml_url

