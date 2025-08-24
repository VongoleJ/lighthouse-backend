from fastapi import FastAPI, Request
app = FastAPI()

@app.get("/")
def root(): return {"ok": True, "service": "lighthouse"}

@app.get("/healthz")
def health(): return {"status": "ok"}

@app.get("/ping")
def ping(request: Request): return {"status":"pong"}
