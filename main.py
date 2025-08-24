#my main
from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "lighthouse"}

@app.get("/healthz")
def health():
    return {"status": "ok"}

