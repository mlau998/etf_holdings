const API_BASE = (window.ENV && window.ENV.API_BASE) || "http://localhost:8000";

// ---- Config: choose which columns to show (in order) ----
const COLUMNS = [
  "fund_ticker",
  "as_of_date",
  "ticker",
  "name",
  "shares",
  "weight_pct",
  "market_value_usd"
];

// ---- Header labels for display ----
const HEADER = {
  fund_ticker: "Fund Ticker",
  as_of_date: "As of",
  ticker: "Ticker",
  name: "Security Name",
  shares: "Shares",
  weight_pct: "Portfolio Weight",
  market_value_usd: "Market Value (USD)"
};

// ---- Optional per-column formatters ----
const fmt = {
  as_of_date: (v) => {
    if (!v) return "";
    // Normalize YYYY-MM-DD or ISO → readable
    const d = new Date(v);
    return isNaN(d) ? String(v) : d.toLocaleDateString();
  },
  shares: (v) => {
    if (v === null || v === undefined || v === "") return "";
    const n = Number(v);
    return isNaN(n) ? String(v) : n.toLocaleString();
  },
  weight_pct: (v) => {
    if (v === null || v === undefined || v === "") return "";
    const n = Number(v);
    // If the values are 0–100 already, keep them; if 0–1, convert to %
    const pct = n <= 1 && n >= 0 ? n * 100 : n;
    return `${pct.toFixed(2)}%`;
  },
  market_value_usd: (v) => {
    if (v === null || v === undefined || v === "") return "";
    const n = Number(v);
    if (isNaN(n)) return String(v);
    return n.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 0 });
  },
  ticker: (v) => (v ? String(v).toUpperCase() : ""),
  fund_ticker: (v) => (v ? String(v).toUpperCase() : ""),
};

function renderTable(rows) {
  const root = document.getElementById("root");
  root.innerHTML = "";

  if (!Array.isArray(rows) || rows.length === 0) {
    root.textContent = "No data.";
    return;
  }

  // Build table
  const table = document.createElement("table");
  table.className = "data-table";

  // Header
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  COLUMNS.forEach((key) => {
    const th = document.createElement("th");
    th.textContent = HEADER[key] ?? key;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  // Body
  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    COLUMNS.forEach((key) => {
      const td = document.createElement("td");
      td.dataset.col = key;

      const raw = row[key];
      const val = fmt[key] ? fmt[key](raw) : (raw ?? "");
      td.textContent = val;

      // Small visual accents
      if (key === "ticker" || key === "fund_ticker") td.classList.add("badge");
      if (key === "market_value_usd" || key === "shares" || key === "weight_pct") td.classList.add("num");

      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  // Mount
  const card = document.createElement("div");
  card.className = "card";
  card.appendChild(table);
  root.appendChild(card);
}

async function boot() {
  const root = document.getElementById("root");
  root.textContent = "Loading…";

  const res = await fetch(`${API_BASE}/holdings`);
  if (!res.ok) { root.textContent = "Failed to load."; return; }
  const rows = await res.json();

  // Optional: keep only keys you declared in COLUMNS (defensive for future fields)
  const slim = rows.map((r) => {
    const o = {};
    COLUMNS.forEach((k) => (o[k] = r[k] ?? null));
    return o;
  });

  renderTable(slim);
}

document.addEventListener("DOMContentLoaded", boot);


