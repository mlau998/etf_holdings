CREATE TABLE IF NOT EXISTS holdings (
  id INTEGER PRIMARY KEY,
  fund_ticker TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  ticker TEXT,
  name TEXT,
  cusip TEXT,
  isin TEXT,
  sedol TEXT,
  shares REAL,
  weight_pct REAL,
  market_value_usd REAL,
  extras TEXT
);
CREATE INDEX IF NOT EXISTS ix_holdings_fund_date ON holdings(fund_ticker, as_of_date);
CREATE INDEX IF NOT EXISTS ix_holdings_ticker ON holdings(ticker);
CREATE INDEX IF NOT EXISTS ix_holdings_cusip ON holdings(cusip);
CREATE INDEX IF NOT EXISTS ix_holdings_isin ON holdings(isin);
CREATE INDEX IF NOT EXISTS ix_holdings_sedol ON holdings(sedol);