from fastapi import FastAPI, Request
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "lighthouse"}

# Health: รองรับได้ทั้ง /healthz และ /health
@app.get("/healthz", include_in_schema=False)
@app.get("/healthz/", include_in_schema=False)
@app.get("/health", include_in_schema=False)
@app.get("/health/", include_in_schema=False)
@app.head("/healthz", include_in_schema=False)
@app.head("/health", include_in_schema=False)
def health():
    return {"status": "ok"}

# ping สำหรับ self‑test
@app.get("/ping")
def ping(request: Request):
    return {"status": "pong", "path": str(request.url.path)}
