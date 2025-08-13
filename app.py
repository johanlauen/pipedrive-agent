import os, datetime as dt
from typing import Dict, Any
import httpx
from fastapi import FastAPI, Request, Header

PIPEDRIVE_BASE = os.getenv("PIPEDRIVE_BASE", "https://api.pipedrive.com/v1")
API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")

app = FastAPI()

def pd_get(path: str, params: Dict[str, Any] = None):
    params = params or {}
    params["api_token"] = API_TOKEN
    r = httpx.get(f"{PIPEDRIVE_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def pd_post(path: str, data: Dict[str, Any]):
    params = {"api_token": API_TOKEN}
    r = httpx.post(f"{PIPEDRIVE_BASE}{path}", params=params, json=data, timeout=30)
    r.raise_for_status()
    return r.json()

def add_note(deal_id: int, content: str):
    return pd_post("/notes", {"deal_id": deal_id, "content": content})

def add_activity(deal_id: int, subject: str, due_in_days: int = 3, type_: str = "call"):
    due = (dt.datetime.utcnow() + dt.timedelta(days=due_in_days)).strftime("%Y-%m-%d")
    return pd_post("/activities", {
        "subject": subject, "type": type_, "deal_id": deal_id, "due_date": due
    })

def send_followup_email(to_email: str, subject: str, body: str):
    # Foreløpig “dummy” – kobles til SendGrid/Mailgun senere.
    print(f"[EMAIL] to={to_email} subj={subject}\n{body}\n")
    return True

def get_person_email(person_id: int) -> str | None:
    if not person_id:
        return None
    data = pd_get(f"/persons/{person_id}")
    person = data.get("data") or {}
    return person.get("email")[0]["value"] if person.get("email") else None

def deal_last_activity_age_days(deal: dict) -> int:
    lad = deal.get("last_activity_date")
    if not lad:
        return 999
    d = dt.datetime.strptime(lad, "%Y-%m-%d")
    return (dt.date.today() - d.date()).days

@app.get("/health")
def health():
    return {"ok": True, "time_utc": dt.datetime.utcnow().isoformat() + "Z"}

# Webhook fra Pipedrive
@app.post("/webhook")
async def webhook(request: Request, x_pipedrive_signature: str | None = Header(default=None)):
    payload = await request.json()

    # Pipedrive sender ulike varianter (v1/v2). Vi henter trygt fra begge.
    meta = payload.get("meta") or {}
    event = payload.get("event")  # kan være "updated.deal" i noen varianter
    action = meta.get("action") or payload.get("event_action") or ""
    obj = meta.get("object") or payload.get("event_object") or ""

    current = (
        payload.get("current")
        or payload.get("data", {}).get("current")
        or {}
    )
    previous = (
        payload.get("previous")
        or payload.get("data", {}).get("previous")
        or {}
    )

    deal_id = current.get("id") or meta.get("id")
    stage_cur = current.get("stage_id")
    stage_prev = previous.get("stage_id")

    # Logg for feilsøk
    print(f"[WEBHOOK] event={event} action={action} object={obj} deal_id={deal_id} stage {stage_prev}->{stage_cur}")

    # Legg note KUN når vi faktisk har en deal_id
    if deal_id:
        # Vil du bare notere ved reell stage-endring? Behold denne if-en:
        if stage_prev != stage_cur:
            add_note(deal_id, f"Webhook: stage {stage_prev} → {stage_cur} @ {dt.datetime.utcnow().isoformat()}Z")
        else:
            # Om du heller vil logge alle endringer, bytt til å alltid legge note.
            add_note(deal_id, f"Webhook mottatt (action={action or 'n/a'}, object={obj or 'n/a'}) @ {dt.datetime.utcnow().isoformat()}Z")

    # Alltid 200, så Pipedrive slipper å spamme retries mens vi tester
    return {"ok": True}


# Daglig sweep (kalles av cron)
@app.post("/daily-sweep")
def daily_sweep():
    stages = pd_get("/stages").get("data", [])
    stage_name_to_id = {s["name"]: s["id"] for s in stages}
    stage_kunde_kontaktet = stage_name_to_id.get("Kunde kontaktet")
    stage_tilbud_sendt = stage_name_to_id.get("Tilbud sendt")

    deals = []
    start = 0
    while True:
        chunk = pd_get("/deals", {"status": "open", "start": start, "limit": 500}).get("data") or []
        deals += chunk
        if len(chunk) < 500:
            break
        start += 500

    processed = {"kk_followups": 0, "ts_followups": 0}
    for d in deals:
        sid = d.get("stage_id")
        person_id = d.get("person_id", {}).get("value")
        email = get_person_email(person_id)

        # “Kunde kontaktet” > 3 dager
        if sid == stage_kunde_kontaktet and deal_last_activity_age_days(d) >= 3 and email:
            sent = send_followup_email(
                email,
                "Skal vi booke gratis befaring? – Softvask Norge",
                "Hei!\n\nVille bare følge opp om du fortsatt ønsker pris på tak/fasadevask. "
                "Vi kan ta en gratis befaring når det passer.\n\n– Johan, Softvask Norge"
            )
            if sent:
                add_note(d["id"], "Auto-oppfølging sendt (Kunde kontaktet).")
                add_activity(d["id"], "Ring kunden hvis ingen svar", due_in_days=3)
                processed["kk_followups"] += 1

        # “Tilbud sendt” > 7 dager
        if sid == stage_tilbud_sendt and deal_last_activity_age_days(d) >= 7 and email:
            sent = send_followup_email(
                email,
                "Spørsmål til tilbudet vårt? – Softvask Norge",
                "Hei!\n\nVille bare sjekke om du har sett på tilbudet. "
                "Gi meg beskjed om du har spørsmål eller ønsker endringer.\n\n– Johan"
            )
            if sent:
                add_note(d["id"], "Auto-oppfølging sendt (Tilbud sendt).")
                add_activity(d["id"], "Ring kunden hvis ingen svar", due_in_days=4)
                processed["ts_followups"] += 1

    return {"status": "ok", "processed": processed}
