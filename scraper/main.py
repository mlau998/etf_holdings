# scraper/main.py
# Fully fixed, module-runnable, JSON/CSV/SQLite outputs, NaN-safe, schema autodetect.

import argparse
import sys
import os
import sqlite3
import yaml
import pandas as pd

from scraper.adapters import etf_from_csv, etf_from_html_table, etf_from_ives
from scraper.edgar import load_latest_13f_table
from scraper.utils import concat_and_order, finalize_types, attach_extras


# ---------- helpers ----------

def _s(v):
    """Return trimmed string; turn None/NaN into ''."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v).strip()

def _nz(v):
    """Return None for NaN, else the value (for SQLite)."""
    return None if (isinstance(v, float) and pd.isna(v)) else v

def _ident(row: pd.Series) -> str:
    """
    Identity key: CUSIP → ISIN → SEDOL → ticker|name.
    Uses raw keys if present; safe for NaNs.
    """
    cusip  = _s(row.get("cusip"))
    isin   = _s(row.get("isin"))
    sedol  = _s(row.get("sedol"))
    ticker = _s(row.get("ticker"))
    name   = _s(row.get("name"))
    return cusip or isin or sedol or f"{ticker}|{name}"

def _choose_schema_path(explicit: str | None) -> str | None:
    """Pick schema file. Priority: explicit → data/schema.sql → data/schema.spl."""
    if explicit and os.path.exists(explicit):
        return explicit
    cand = os.path.join("data", "schema.sql")
    if os.path.exists(cand):
        return cand
    cand2 = os.path.join("data", "schema.spl")
    if os.path.exists(cand2):
        return cand2
    return None


# ---------- core run ----------

def run(
    config_path: str,
    out_path: str = "data/holdings_latest.csv",
    prev_path: str | None = None,
    json_path: str = "data/holdings_latest.json",
    db_path: str = "data/holdings.db",
    schema_path: str | None = None,
) -> None:
    print("[DEBUG] scraper.main: start")
    os.makedirs("data", exist_ok=True)

    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    out_frames: list[pd.DataFrame] = []

    for m in cfg.get("managers", []):
        typ = m.get("type")
        mid = m.get("id")

        if not typ or not mid:
            print(f"[SKIP] invalid manager entry: {m}", file=sys.stderr)
            continue

        if typ == "etf_csv":
            csv_url = m.get("csv_url") or ""
            if not csv_url:
                print(f"[SKIP] {mid}: csv_url not set", file=sys.stderr)
                continue
            try:
                if mid.upper() == "IVES":
                    print("[DEBUG] etf_from_ives")
                    df = etf_from_ives(csv_url, mid)
                else:
                    df = etf_from_csv(csv_url, mid)
                out_frames.append(df)
                print(f"[OK] {mid} rows={len(df)} (CSV)")
            except Exception as e:
                print(f"[ERROR] {mid} etf_from_csv failed: {e}", file=sys.stderr)

        elif typ == "etf_html_table":
            page_url = m.get("page_url") or ""
            if not page_url:
                print(f"[SKIP] {mid}: page_url not set", file=sys.stderr)
                continue
            table_sel = (m.get("table_selector") or "table")
            asof_sel = (m.get("asof_selector") or "")
            try:
                df = etf_from_html_table(page_url, mid, table_selector=table_sel, as_of_selector=asof_sel)
                out_frames.append(df)
                print(f"[OK] {mid} rows={len(df)} (HTML table)")
            except Exception as e:
                print(f"[ERROR] {mid} etf_from_html_table failed: {e}", file=sys.stderr)

        elif typ == "sec_13f":
            cik = m.get("cik")
            if not cik:
                print(f"[SKIP] {mid}: cik not set", file=sys.stderr)
                continue
            try:
                df13f, report_date, _ = load_latest_13f_table(cik)
                core = [
                    "fund_ticker","as_of_date","ticker","name",
                    "cusip","isin","sedol","shares","weight_pct","market_value_usd"
                ]

                out = pd.DataFrame(index=df13f.index)
                out["fund_ticker"] = mid
                out["as_of_date"] = report_date
                for c in core[2:]:
                    out[c] = df13f[c] if c in df13f.columns else pd.NA

                extra_cols = [c for c in df13f.columns if c not in out.columns]
                merged = out.join(df13f[extra_cols]) if extra_cols else out
                merged = attach_extras(merged, keep_cols=core)
                out_frames.append(merged[core + ["extras"]])
                print(f"[OK] {mid} rows={len(merged)} (13F report_date={report_date})")
            except Exception as e:
                print(f"[ERROR] {mid} 13F load failed: {e}", file=sys.stderr)

        else:
            print(f"[WARN] Unknown type {typ} for {mid}", file=sys.stderr)

    # Concatenate and normalize
    final = concat_and_order(out_frames)
    final = finalize_types(final)

    # Write CSV (for audit/compat)
    final.to_csv(out_path, index=False)
    print(f"[DONE] wrote CSV: {len(final)} rows → {out_path}")

    # Write JSON (for Streamlit/API)
    final.to_json(json_path, orient="records")
    print(f"[DONE] wrote JSON: {json_path}")

    # SQLite upsert (if schema exists)
    schema_to_use = _choose_schema_path(schema_path)
    if schema_to_use:
        with sqlite3.connect(db_path) as con:
            con.executescript(open(schema_to_use, "r").read())
            cur = con.cursor()

            for _, r in final.iterrows():
                k = (_s(r["fund_ticker"]), _s(r["as_of_date"]), _ident(r))
                cur.execute(
                    """
                    DELETE FROM holdings
                    WHERE fund_ticker=? AND as_of_date=?
                      AND COALESCE(cusip, isin, sedol, ticker||'|'||name)=?
                    """,
                    k,
                )
                cur.execute(
                    """
                    INSERT INTO holdings (
                        fund_ticker, as_of_date, ticker, name, cusip, isin, sedol,
                        shares, weight_pct, market_value_usd, extras
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _s(r.get("fund_ticker")) or None,
                        _s(r.get("as_of_date")) or None,
                        _s(r.get("ticker")) or None,
                        _s(r.get("name")) or None,
                        _s(r.get("cusip")) or None,
                        _s(r.get("isin")) or None,
                        _s(r.get("sedol")) or None,
                        _nz(r.get("shares")),
                        _nz(r.get("weight_pct")),
                        _nz(r.get("market_value_usd")),
                        _s(r.get("extras")) or None,
                    ),
                )
        print(f"[DONE] upserted into SQLite: {db_path}")
    else:
        print("[WARN] No schema file found (data/schema.sql or data/schema.spl). Skipping SQLite write.", file=sys.stderr)

    # Optional diff if a previous CSV is provided
    if prev_path:
        prev = pd.read_csv(prev_path)
        def key(df):
            k1 = df["cusip"].fillna("")
            k2 = df["isin"].fillna("")
            k3 = df["sedol"].fillna("")
            k4 = (df["ticker"].fillna("") + "|" + df["name"].fillna(""))
            step = k1.where(k1 != "", k2)
            step = step.where(step != "", k3)
            return step.where(step != "", k4)

        prev_keys = set((prev["fund_ticker"] + "::" + key(prev)))
        curr_keys = set((final["fund_ticker"] + "::" + key(final)))
        added = sorted(curr_keys - prev_keys)
        removed = sorted(prev_keys - curr_keys)
        with open(out_path + ".diff.txt", "w") as f:
            f.write("ADDED\n"); [f.write(a + "\n") for a in added]
            f.write("\nREMOVED\n"); [f.write(r + "\n") for r in removed]
        print(f"[DIFF] added={len(added)} removed={len(removed)} → {out_path}.diff.txt")


# ---------- cli ----------

def main():
    ap = argparse.ArgumentParser(description="Scrape ETF holdings and export CSV/JSON/SQLite.")
    ap.add_argument("--config", required=True, help="Path to config.yml")
    ap.add_argument("--out", default="data/holdings_latest.csv", help="Output CSV path")
    ap.add_argument("--prev", default=None, help="Previous CSV for diffing")
    ap.add_argument("--json", default="data/holdings_latest.json", help="Output JSON path")
    ap.add_argument("--db", default="data/holdings.db", help="SQLite DB path")
    ap.add_argument("--schema", default=None, help="Path to schema.sql/spl (optional)")
    args = ap.parse_args()

    run(
        config_path=args.config,
        out_path=args.out,
        prev_path=args.prev,
        json_path=args.json,
        db_path=args.db,
        schema_path=args.schema,
    )


if __name__ == "__main__":
    main()

# Old Code

# import argparse
# import sys
# import yaml
# import pandas as pd
# from scraper.adapters import etf_from_csv, etf_from_html_table, etf_from_ives
# from scraper.edgar import load_latest_13f_table
# from scraper.utils import concat_and_order, finalize_types, attach_extras

# def run(config_path: str, out_path: str, prev_path: str = None):
#     print("[DEBUG] main.py loaded")
#     cfg = yaml.safe_load(open(config_path, "r"))
#     out_frames = []

#     for m in cfg["managers"]:
#         typ = m["type"]

#         if typ == "etf_csv":
#             csv_url = m.get("csv_url") or ""
#             print(f"[DEBUG] csv_url exists: {csv_url}")
#             if not csv_url:
#                 print(f"[SKIP] {m['id']}: csv_url not set", file=sys.stderr); continue
#             try:
#                 if m["id"].upper() == "IVES":
#                     print("[DEBUG] Entering IVES etf_from_ives block")
#                     df = etf_from_ives(csv_url, m["id"])
#                     print(f"[DEBUG] IVES etf_from_ives returned DataFrame with shape: {df.shape}")
#                     out_frames.append(df)
#                     print(f"[OK] {m['id']} rows={len(df)} (IVES CSV)")
#                 else:
#                     df = etf_from_csv(csv_url, m["id"])
#                     out_frames.append(df)
#                     print(f"[OK] {m['id']} rows={len(df)} (CSV)")
#             except Exception as e:
#                 print(f"[ERROR] {m['id']} etf_from_csv failed: {e}", file=sys.stderr)

#         elif typ == "etf_html_table":
#             page_url = m.get("page_url") or ""
#             if not page_url:
#                 print(f"[SKIP] {m['id']}: page_url not set", file=sys.stderr); continue
#             table_sel = (m.get("table_selector") or "table")
#             asof_sel = (m.get("asof_selector") or "")
#             try:
#                 df = etf_from_html_table(page_url, m["id"], table_selector=table_sel, as_of_selector=asof_sel)
#                 out_frames.append(df)
#                 print(f"[OK] {m['id']} rows={len(df)} (HTML table)")
#             except Exception as e:
#                 print(f"[ERROR] {m['id']} etf_from_html_table failed: {e}", file=sys.stderr)

#         elif typ == "sec_13f":
#             cik = m.get("cik")
#             if not cik:
#                 print(f"[SKIP] {m['id']}: cik not set", file=sys.stderr); continue
#             try:
#                 df13f, report_date, _ = load_latest_13f_table(cik)
#                 core = ["fund_ticker","as_of_date", "ticker",
#                 "name","cusip","isin","sedol",
#                 "shares","weight_pct","market_value_usd"]

#                 out = pd.DataFrame(index=df13f.index)                      # << important
#                 out["fund_ticker"] = m["id"]
#                 out["as_of_date"] = report_date
#                 for c in core[2:]:
#                     out[c] = df13f[c] if c in df13f.columns else pd.NA

#                 extra_cols = [c for c in df13f.columns if c not in out.columns]
#                 merged = out.join(df13f[extra_cols]) if extra_cols else out

#                 merged = attach_extras(merged, keep_cols=core)
#                 out_frames.append(merged[core + ["extras"]])

#                 print(f"[OK] {m['id']} rows={len(merged)} (13F report_date={report_date})")
#             except Exception as e:
#                 print(f"[ERROR] {m['id']} 13F load failed: {e}", file=sys.stderr)

#         else:
#             print(f"[WARN] Unknown type {typ} for {m['id']}")

#     final = concat_and_order(out_frames)
#     final = finalize_types(final)
#     final.to_csv(out_path, index=False)
#     print(f"[DONE] {len(final)} rows → {out_path}")

#     os.makedirs("data", exist_ok=True)
#     final.to_json("data/holdings_latest.json", orient="records")

#     # append to SQLite (one row per fund/security per as_of_date)
#     import sqlite3, os
#     os.makedirs("data", exist_ok=True)
#     with sqlite3.connect("data/holdings.db") as con:
#         con.executescript(open("data/schema.sql", "r").read())
#         cur = con.cursor()

#         def _ident(row):
#             return row["cusip"] or row["isin"] or row["sedol"] or f"{(row['ticker'] or '').strip()}|{(row['name'] or '').strip()}"

#         for _, r in final.iterrows():
#             k = (r["fund_ticker"], r["as_of_date"], _ident(r))
#             cur.execute("""
#             DELETE FROM holdings
#             WHERE fund_ticker=? AND as_of_date=? AND COALESCE(cusip, isin, sedol, ticker||'|'||name)=?
#             """, k)
#             cur.execute("""
#             INSERT INTO holdings (fund_ticker, as_of_date, ticker, name, cusip, isin, sedol,
#                                     shares, weight_pct, market_value_usd, extras)
#             VALUES (?,?,?,?,?,?,?,?,?,?,?)
#             """, (r["fund_ticker"], r["as_of_date"], r["ticker"], r["name"], r["cusip"], r["isin"], r["sedol"],
#                 r["shares"], r["weight_pct"], r["market_value_usd"], r["extras"]))


#     if prev_path:
#         prev = pd.read_csv(prev_path)
#         def key(df):
#             k1 = df["cusip"].fillna("")
#             k2 = df["isin"].fillna("")
#             k3 = df["sedol"].fillna("")
#             k4 = (df["ticker"].fillna("") + "|" + df["name"].fillna(""))
#             step = k1.where(k1!="", k2); step = step.where(step!="", k3)
#             return step.where(step!="", k4)
#         prev_keys = set((prev["fund_ticker"] + "::" + key(prev)))
#         curr_keys = set((final["fund_ticker"] + "::" + key(final)))
#         added = sorted(curr_keys - prev_keys); removed = sorted(prev_keys - curr_keys)
#         with open(out_path + ".diff.txt", "w") as f:
#             f.write("ADDED\n"); [f.write(a + "\n") for a in added]
#             f.write("\nREMOVED\n"); [f.write(r + "\n") for r in removed]
#         print(f"[DIFF] added={len(added)} removed={len(removed)} → {out_path}.diff.txt")

# if __name__ == "__main__":
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--config", required=True)
#     ap.add_argument("--out", required=True)
#     ap.add_argument("--prev", required=False, default=None)
#     args = ap.parse_args()
#     run(args.config, args.out, args.prev)




