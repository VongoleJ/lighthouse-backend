import os, hmac, hashlib, base64, json, io, time, threading
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
import duckdb
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

# -------- Env / Config --------
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8080")

TOU_PEAK_START = int(os.getenv("TOU_PEAK_START", "18"))
TOU_PEAK_END   = int(os.getenv("TOU_PEAK_END", "22"))
TOU_PEAK_THB   = float(os.getenv("TOU_PEAK_THB", "5.0"))
TOU_OFF_THB    = float(os.getenv("TOU_OFF_THB", "3.5"))
FLAT_THB       = float(os.getenv("FLAT_THB", "4.2"))

LLM_API_KEY = os.getenv("GROQ_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.1-8b")
LLM_TEMP = float(os.getenv("LLM_TEMP", "0.2"))
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "768"))

app = FastAPI(title="Lighthouse MVP (LINE + Cloud Run)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# -------- In-memory media store (PNG) --------
_MEDIA: Dict[str, Tuple[bytes, float]] = {}
_MEDIA_TTL = 60 * 30
def put_media_png(png_bytes: bytes) -> str:
    token = str(int(time.time()*1000)) + "_" + str(abs(hash(png_bytes)) % (10**8))
    _MEDIA[token] = (png_bytes, time.time())
    return token
def gc_media():
    now = time.time()
    dead = [k for k,(b,ts) in _MEDIA.items() if now - ts > _MEDIA_TTL]
    for k in dead:
        _MEDIA.pop(k, None)

@app.get("/media/{token}")
def media(token: str):
    gc_media()
    item = _MEDIA.get(token)
    if not item:
        return PlainTextResponse("Not Found", status_code=404)
    data, ts = item
    return StreamingResponse(io.BytesIO(data), media_type="image/png")

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": time.time()}

# -------- LINE helpers --------
def verify_line_signature(raw_body: bytes, x_line_signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, x_line_signature or "")

def line_reply(reply_token: str, messages):
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Content-Type":"application/json","Authorization":f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    body = {"replyToken": reply_token, "messages": messages}
    r = requests.post(url, headers=headers, json=body, timeout=10)
    return r.status_code, r.text

def line_push(user_id: str, messages):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type":"application/json","Authorization":f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    body = {"to": user_id, "messages": messages}
    r = requests.post(url, headers=headers, json=body, timeout=15)
    return r.status_code, r.text

# -------- Data loading & views --------
def load_demo_df() -> pd.DataFrame:
    p = os.path.join(os.path.dirname(__file__), "sample_data.csv")
    df = pd.read_csv(p, parse_dates=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df

def add_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute
    df["weekday"] = df["timestamp"].dt.weekday
    def tou(h):
        return "peak" if TOU_PEAK_START <= h < TOU_PEAK_END else "off"
    df["tou_period"] = df["hour"].apply(tou)
    df["price_thb_per_kwh"] = df["tou_period"].map({"peak":TOU_PEAK_THB,"off":TOU_OFF_THB})
    df["kwh_15m"] = df["kwh"].astype(float)
    df["kwh_hour_est"] = df["kwh_15m"] * 4.0
    df["cost_thb"] = df["kwh_15m"] * df["price_thb_per_kwh"]
    return df

def build_duck_views(df: pd.DataFrame):
    con = duckdb.connect(database=":memory:")
    con.register("readings", df)
    con.execute("""
        CREATE VIEW v_daily AS
        SELECT date,
               SUM(kwh_15m) AS kwh_day,
               SUM(cost_thb) AS cost_day,
               SUM(CASE WHEN tou_period='peak' THEN kwh_15m ELSE 0 END) AS kwh_peak,
               SUM(CASE WHEN tou_period='off'  THEN kwh_15m ELSE 0 END) AS kwh_off
        FROM readings GROUP BY 1 ORDER BY 1;
    """)
    con.execute("""
        CREATE VIEW v_peak_off AS
        SELECT date, tou_period, SUM(kwh_15m) AS kwh, SUM(cost_thb) AS cost
        FROM readings GROUP BY 1,2 ORDER BY 1,2;
    """)
    con.execute("""
        CREATE VIEW v_weekday_slot AS
        SELECT weekday, hour, minute,
               AVG(kwh_15m) AS kwh_avg,
               MAX(kwh_15m) AS kwh_max
        FROM readings GROUP BY 1,2,3 ORDER BY 1,2,3;
    """)
    return con

# -------- Detection --------
def compute_typical(df: pd.DataFrame, alpha: float = 0.2):
    g = df.groupby(["weekday","hour","minute"])["kwh_15m"]
    med = g.median().rename("med").reset_index()
    mad = (g.apply(lambda x: (x - x.median()).abs().median()).rename("mad")).reset_index()
    tb = med.merge(mad, on=["weekday","hour","minute"], how="left")
    tb["mad"].fillna(0.0, inplace=True)
    tb["typical"] = tb["med"]
    return tb

def classify_patterns(df: pd.DataFrame, typical: pd.DataFrame, k: float = 2.5):
    out = []
    key = ["weekday","hour","minute"]
    m = df.merge(typical[key+["typical","mad"]], on=key, how="left")
    m["lo"] = (m["typical"] - k*m["mad"]).clip(lower=0.0)
    m["hi"] = (m["typical"] + k*m["mad"]).clip(lower=0.0)
    m["out"] = (m["kwh_15m"] < m["lo"]) | (m["kwh_15m"] > m["hi"])
    peak = m[m["tou_period"]=="peak"].groupby("date")["kwh_15m"].sum()
    total = m.groupby("date")["kwh_15m"].sum()
    share = (peak/total).fillna(0)
    for d,v in share.items():
        if v > 0.45:
            out.append({"date": str(d), "label":"peak_share_high", "detail": f"peak share ≈ {int(v*100)}%"})
    night = m[(m["hour"]>=1)&(m["hour"]<4)].groupby("date")["kwh_15m"].mean()
    night_typ = m[(m["hour"]>=1)&(m["hour"]<4)].groupby("date")[["typical","mad"]].mean()
    for d,val in night.items():
        t = night_typ.loc[d,"typical"]; mad = max(0.01, night_typ.loc[d,"mad"])
        if val > t + 2.0*mad:
            out.append({"date": str(d), "label":"overnight_load_up", "detail": f"avg night {val:.2f} > {t:.2f}"})
    m["above"] = m["kwh_15m"] > (m["hi"] + 0.05)
    for d, day in m.groupby("date"):
        day = day.sort_values("timestamp")
        streak = 0; flagged = False
        for v in day["above"].values:
            streak = streak + 1 if v else 0
            if streak >= 3 and not flagged:
                out.append({"date": str(d), "label":"run_on_appliance", "detail":"≥3 consecutive slots high"})
                flagged = True
    daily = m.groupby("date")["kwh_15m"].sum().reset_index()
    daily["diff"] = daily["kwh_15m"].diff().abs()
    thr = daily["kwh_15m"].median()*0.2
    for _, row in daily.iterrows():
        if row["diff"] and row["diff"] > thr:
            out.append({"date": str(row['date']), "label":"change_point", "detail": f"Δday ≈ {row['diff']:.2f} kWh"})
    return m, out

# -------- SQL Assist --------
def parse_thai_date(s: str) -> Optional[datetime]:
    import re
    months_th = {"ม.ค.":1,"ก.พ.":2,"มี.ค.":3,"เม.ย.":4,"พ.ค.":5,"มิ.ย.":6,"ก.ค.":7,"ส.ค.":8,"ก.ย.":9,"ต.ค.":10,"พ.ย.":11,"ธ.ค.":12}
    m = re.search(r"(\\d{1,2})\\s*(ม\\.ค\\.|ก\\.พ\\.|มี\\.ค\\.|เม\\.ย\\.|พ\\.ค\\.|มิ\\.ย\\.|ก\\.ค\\.|ส\\.ค\\.|ก\\.ย\\.|ต\\.ค\\.|พ\\.ย\\.|ธ\\.ค\\.)", s, re.IGNORECASE)
    if m:
        d = int(m.group(1)); mo = months_th[m.group(2)]
        y = datetime.now().year
        return datetime(y, mo, d)
    m2 = re.search(r"(\\d{1,2})\\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", s, re.IGNORECASE)
    if m2:
        d = int(m2.group(1)); mo = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(m2.group(2).lower())+1
        y = datetime.now().year
        return datetime(y, mo, d)
    return None

def sql_assist(con: duckdb.DuckDBPyConnection, question: str):
    q = question.strip().lower()
    if "วันนี้" in q or "today" in q:
        sql = "SELECT date, SUM(kwh_15m) AS kwh, SUM(cost_thb) AS thb FROM readings WHERE date = CURRENT_DATE GROUP BY 1;"
        try: return sql, con.execute(sql).fetchdf()
        except: pass
    dt = parse_thai_date(question)
    if dt:
        sql = f"SELECT date, SUM(kwh_15m) AS kwh, SUM(cost_thb) AS thb FROM readings WHERE date = DATE '{dt.date()}' GROUP BY 1;"
        return sql, con.execute(sql).fetchdf()
    if ("พีค" in q and "%" in q) or ("peak" in q and "%" in q):
        if dt:
            sql = f"""
            WITH dd AS (SELECT SUM(kwh_15m) AS total_kwh FROM readings WHERE date = DATE '{dt.date()}'),
                 pp AS (SELECT SUM(kwh_15m) AS peak_kwh FROM readings WHERE date = DATE '{dt.date()}' AND tou_period='peak')
            SELECT 100.0 * pp.peak_kwh / dd.total_kwh AS peak_pct FROM dd, pp;
            """
        else:
            sql = """
            WITH dd AS (SELECT date, SUM(kwh_15m) AS total_kwh FROM readings GROUP BY 1),
                 pp AS (SELECT date, SUM(kwh_15m) AS peak_kwh FROM readings WHERE tou_period='peak' GROUP BY 1)
            SELECT dd.date, 100.0*pp.peak_kwh/dd.total_kwh AS peak_pct
            FROM dd JOIN pp ON dd.date=pp.date ORDER BY dd.date DESC LIMIT 1;
            """
        return sql, con.execute(sql).fetchdf()
    if "สัปดาห์" in q or "week" in q:
        sql = """
        SELECT date, SUM(kwh_15m) AS kwh, SUM(cost_thb) AS thb,
               SUM(CASE WHEN tou_period='peak' THEN kwh_15m ELSE 0 END) AS kwh_peak
        FROM readings
        WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
        GROUP BY 1 ORDER BY 1;
        """
        return sql, con.execute(sql).fetchdf()
    sql = "SELECT date, SUM(kwh_15m) AS kwh, SUM(cost_thb) AS thb FROM readings GROUP BY 1 ORDER BY date DESC LIMIT 1;"
    return sql, con.execute(sql).fetchdf()

# -------- LLM (Groq) + fallback --------
def llm_narrative(context: Dict[str, Any], question: str, numbers: str) -> str:
    sys = "You are Lighthouse, an energy assistant. Reply in Thai by default; if the user's question is clearly English, reply in English."
    user = f"Question: {question}\nNumbers:\n{numbers}\nContext:\n{json.dumps(context, ensure_ascii=False)}\nWrite a concise answer with clear recommendation."
    if not LLM_API_KEY:
        return f"สรุป (โหมดไม่ใช้ LLM):\nคำถาม: {question}\nตัวเลข:\n{numbers}\nคำตอบย่อ: ย้ายโหลดช่วงพีคไปออฟพีค และตรวจช่วงที่หลุดช่วงปกติ หากต้องการกราฟ พิมพ์ 'กราฟ'"
    try:
        url = "https://api.groq.com/openai/v1/chat/completions"
        headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type":"application/json"}
        payload = {
            "model": LLM_MODEL, "temperature": LLM_TEMP, "max_tokens": LLM_MAX_TOKENS,
            "messages":[{"role":"system","content":sys},{"role":"user","content":user}]
        }
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        if r.ok:
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()
        else:
            return f"สรุป (fallback):\n{numbers}"
    except Exception:
        return f"สรุป (fallback):\n{numbers}"

# -------- Chart (PNG) --------
def render_chart_png(df: pd.DataFrame, m: pd.DataFrame) -> bytes:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    fig, ax = plt.subplots(figsize=(10,3))
    ax.plot(m["timestamp"], m["kwh_15m"], label="actual")
    ax.plot(m["timestamp"], m["typical"], label="typical", linewidth=1)
    ax.fill_between(m["timestamp"], m["lo"], m["hi"], alpha=0.2, label="deadband")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.set_ylabel("kWh / 15m")
    ax.set_title("Usage vs Deadband")
    ax.legend()
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=140)
    import matplotlib.pyplot as plt2
    plt2.close(fig)
    return buf.getvalue()

# -------- Webhook --------
@app.post("/callback")
async def callback(request: Request):
    raw = await request.body()
    sig = request.headers.get("x-line-signature", "")
    if not verify_line_signature(raw, sig):
        raise HTTPException(401, "invalid signature")
    payload = await request.json()
    events = payload.get("events", [])
    for ev in events:
        if ev.get("type") == "message" and ev.get("message", {}).get("type") == "text":
            user_id = ev["source"].get("userId","")
            reply_token = ev.get("replyToken","")
            text = ev["message"]["text"][:500]
            lang_en = any(ch.isascii() and ch.isalpha() for ch in text)
            ack = "Got it, processing…" if lang_en else "รับทราบ กำลังประมวลผล…"
            line_reply(reply_token, [{"type":"text","text":ack}])
            threading.Thread(target=process_and_push, args=(user_id, text)).start()
    return JSONResponse({"ok": True})

def add_time_columns(df: pd.DataFrame) -> pd.DataFrame  # type: ignore
# (dummy to satisfy static analyzers)
    return df

def process_and_push(user_id: str, question: str):
    try:
        df = load_demo_df()
        # build derived columns AFTER load (typo fix for above static stub)
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df["weekday"] = df["timestamp"].dt.weekday
        df["tou_period"] = df["hour"].apply(lambda h: "peak" if TOU_PEAK_START <= h < TOU_PEAK_END else "off")
        df["price_thb_per_kwh"] = df["tou_period"].map({"peak":TOU_PEAK_THB,"off":TOU_OFF_THB})
        df["kwh_15m"] = df["kwh"].astype(float)
        df["kwh_hour_est"] = df["kwh_15m"] * 4.0
        df["cost_thb"] = df["kwh_15m"] * df["price_thb_per_kwh"]

        tb = compute_typical(df)
        m, patterns = classify_patterns(df, tb, k=2.5)
        con = build_duck_views(df)
        sql, table = sql_assist(con, question)
        numbers = table.to_string(index=False)
        ctx = {"tou":{"peak":[TOU_PEAK_START,TOU_PEAK_END],
                      "thb":{"peak":TOU_PEAK_THB,"off":TOU_OFF_THB,"flat":FLAT_THB}},
               "patterns":patterns[:5]}
        answer = llm_narrative(ctx, question, numbers)
        want_chart = ("กราฟ" in question) or ("chart" in question.lower())
        messages = [{"type":"text","text":answer[:4900]}]
        if want_chart:
            png = render_chart_png(df, m.tail(96*7))
            token = put_media_png(png)
            img_url = f"{PUBLIC_BASE_URL}/media/{token}"
            messages.append({"type":"image","originalContentUrl":img_url,"previewImageUrl":img_url})
        line_push(user_id, messages)
    except Exception as e:
        line_push(user_id, [{"type":"text","text":f"ขออภัย เกิดข้อผิดพลาด: {e}"}])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT","8080")))
