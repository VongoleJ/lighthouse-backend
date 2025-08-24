import os, hmac, hashlib, base64, httpx
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "lighthouse"}

@app.get("/ping")
def ping():
    return {"status": "pong"}

# Verify LINE signature
def verify_line_signature(body: bytes, signature: str) -> bool:
    secret = os.environ["LINE_CHANNEL_SECRET"].encode()
    mac = hmac.new(secret, body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode()
    return hmac.compare_digest(expected, signature or "")

LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"

@app.post("/line/webhook")
async def line_webhook(
    request: Request,
    x_line_signature: str = Header(default=""),
    x_api_key: str = Header(default=None)
):
    # Optional API key check
    if os.getenv("API_KEY") and x_api_key != os.getenv("API_KEY"):
        raise HTTPException(status_code=401, detail="Bad api key")

    body = await request.body()

    # Verify LINE signature
    if not verify_line_signature(body, x_line_signature):
        raise HTTPException(status_code=401, detail="Bad signature")

    data = await request.json()
    events = data.get("events", [])

    replies = []
    for ev in events:
        if ev.get("type") != "message":
            continue
        msg = ev["message"]
        if msg.get("type") != "text":
            continue

        text = (msg.get("text") or "").strip().lower()
        reply_token = ev["replyToken"]

        if text == "ping":
            replies.append({
                "replyToken": reply_token,
                "messages": [{"type": "text", "text": "pong ✅"}]
            })
        elif text.startswith("temp"):
            replies.append({
                "replyToken": reply_token,
                "messages": [{"type": "text", "text": "Bangkok ~30°C (demo)"}]
            })
        else:
            replies.append({
                "replyToken": reply_token,
                "messages": [{"type": "text", "text": f"คุณพิมพ์: {text}"}]
            })

    if not replies:
        return {"ok": True}

    headers = {
        "Authorization": f"Bearer {os.environ['LINE_CHANNEL_ACCESS_TOKEN']}",
        "Content-Type": "application/json"
    }
    async with httpx.AsyncClient(timeout=10) as client:
        for payload in replies:
            r = await client.post(LINE_REPLY_URL, json=payload, headers=headers)
            if r.status_code >= 300:
                print("LINE reply error:", r.status_code, r.text)

    return {"ok": True}
