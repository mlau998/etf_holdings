import os, json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

DATA_DIR  = os.getenv("DATA_DIR", "data")
JSON_PATH = os.getenv("JSON_PATH", os.path.join(DATA_DIR, "holdings_latest.json"))

app = FastAPI(title="Holdings API", version="0.1")

FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/holdings")
def holdings():
    if not os.path.exists(JSON_PATH):
        raise HTTPException(status_code=404, detail=f"missing file: {JSON_PATH}")
    # return parsed JSON so consumers get application/json with an array
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise HTTPException(status_code=500, detail="holdings.json must be a JSON array")
    return JSONResponse(data)

# optional: raw file passthrough if you want it
@app.get("/holdings.raw.json")
def holdings_raw():
    if not os.path.exists(JSON_PATH):
        raise HTTPException(status_code=404, detail=f"missing file: {JSON_PATH}")
    return FileResponse(JSON_PATH, media_type="application/json", filename="holdings.json")
