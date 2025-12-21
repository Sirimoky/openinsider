import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
import xml.etree.ElementTree as ET

import smtplib
from email.message import EmailMessage

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"
HISTORY_PATH = "history.jsonl"


# -----------------------------
# Utils JSON / JSONL
# -----------------------------
def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def ensure_history_file() -> None:
    if not os.path.exists(HISTORY_PATH):
        open(HISTORY_PATH, "a", encoding="utf-8").close()


# -----------------------------
# HTTP fetch con retry
# -----------------------------
def fetch_text(url: str, headers: Dict[str, str], retries: int = 3, timeout: int = 45) -> str:
    last = None
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last = e
            print(f"[fetch] Tentativo {i}/{retries} fallito: {e}")
            time.sleep(2 * i)
    raise RuntimeError(f"Impossibile scaricare {url}: {last}")


# -----------------------------
# Email (Gmail SMTP)
# -----------------------------
def send_email(cfg: dict, subject: str, body: str) -> None:
    email_cfg = cfg.get("email", {})
    if not email_cfg.get("enabled", False):
        return

    smtp_pass = os.environ.get("SMTP_PASS")
    if not smtp_pass:
        raise RuntimeError("SMTP_PASS non presente (GitHub Secret mancante).")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_cfg["smtp_user"]
    msg["To"] = email_cfg["to"]
    msg.set_content(body)

    with smtplib.SMTP(email_cfg["smtp_host"], int(email_cfg["smtp_port"])) as server:
        server.starttls()
        server.login(email_cfg["smtp_user"], smtp_pass)
        server.send_message(msg)


# -----------------------------
# SEC: Bootstrap storico via FULL-INDEX master.idx
# (molte meno richieste rispetto ai daily-index)
# -----------------------------
def quarter_for_date(d: datetime) -> int:
    return (d.month - 1) // 3 + 1


def full_index_master_url(year: int, qtr: int) -> str:
    # https://www.sec.gov/Archives/edgar/full-index/2025/QTR4/master.idx
    return f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"


def parse_master_idx(text: str) -> List[Dict[str, str]]:
    """
    master.idx ha header e poi righe:
    CIK|Company Name|Form Type|Date Filed|Filename
    """
    rows: List[Dict[str, str]] = []
    lines = text.splitlines()

    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("CIK|Company Name|Form Type|Date Filed|Filename"):
            start = i + 1
            break
    if start is None:
        return rows

    for line in lines[start:]:
        if "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, name, form, date_filed, filename = [p.strip() for p in parts]
        rows.append(
            {
                "cik": cik,
                "company_name": name,
                "form_type": form,
                "date_filed": date_filed,
                "filename": filename,
            }
        )
    return rows


def bootstrap_history_once(cfg: dict, headers: Dict[str, str], state: dict) -> None:
    """
    Bootstrap 90/120 giorni SOLO la prima volta.
    Se fallisce, non blocca il live.
    """
    sec_cfg = cfg["sec"]
    days = int(sec_cfg.get("history_days", 120))
    max_rows = int(sec_cfg.get("max_history_rows", 50000))

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()

    # memoria anti-duplicati per lo storico
    seen = set(state.get("history_seen_filenames", []))

    now = datetime.now(timezone.utc)
    this_q = quarter_for_date(now)
    this_y = now.year

    prev_q = this_q - 1
    prev_y = this_y
    if prev_q == 0:
        prev_q = 4
        prev_y -= 1

    quarters = [(this_y, this_q), (prev_y, prev_q)]

    added = 0
    new_rows: List[Dict[str, Any]] = []

    for (y, q) in quarters:
        url = full_index_master_url(y, q)
        print(f"[history] Download full-index: {url}")

        # master.idx può essere grande: timeout più alto
        idx_text = fetch_text(url, headers=headers, retries=3, timeout=90)
        rows = parse_master_idx(idx_text)

        for r in rows:
            if r.get("form_type") != "4":
                continue

            # filtro per data filed
            date_str = r.get("date_filed", "")
            try:
                dfiled = datetime.strptime(date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if dfiled < cutoff:
                continue

            fn = r["filename"]
            if fn in seen:
                continue

            seen.add(fn)
            new_rows.append(
                {
                    "source": "bootstrap",
                    "date_filed": r["date_filed"],
                    "company_name": r["company_name"],
                    "cik": r["cik"],
                    "form_type": r["form_type"],
                    "filename": fn,
                    "filing_url": f"https://www.sec.gov/Archives/{fn}",
                }
            )
            added += 1

            if len(seen) >= max_rows:
                break

        # piccola pausa “gentile”
        time.sleep(0.5)

    append_jsonl(HISTORY_PATH, new_rows)
    # limita crescita
    state["history_seen_filenames"] = list(seen)[-max_rows:]
    print(f"[history] Nuove righe storico aggiunte: {added}")


# -----------------------------
# LIVE: feed Atom + parse Form4 XML
# -----------------------------
def parse_atom_entries(atom_xml: str) -> List[Dict[str, str]]:
    root = ET.fromstring(atom_xml)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    out = []

    for entry in root.findall("atom:entry", ns):
        entry_id = (entry.findtext("atom:id", default="", namespaces=ns) or "").strip()
        title = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
        updated = (entry.findtext("atom:updated", default="", namespaces=ns) or "").strip()

        link = ""
        link_el = entry.find("atom:link", ns)
        if link_el is not None and "href" in link_el.attrib:
            link = link_el.attrib["href"].strip()

        if entry_id:
            out.append({"id": entry_id, "title": title, "updated": updated, "link": link})
    return out


def find_filing_xml_url(filing_index_html: str) -> Optional[str]:
    # Cerca un link a un file .xml (form4)
    m = re.search(r'href="([^"]+\.xml)"', filing_index_html, flags=re.IGNORECASE)
    if not m:
        return None
    href = m.group(1)
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.sec.gov" + href
    return "https://www.sec.gov/" + href.lstrip("/")


def parse_form4_xml(form4_xml: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(form4_xml)

    def t(path):
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else ""

    ticker = t(".//issuerTradingSymbol")
    insider = t(".//reportingOwnerName")
    title = t(".//officerTitle")

    rows = []

    for tx in root.findall(".//nonDerivativeTransaction"):
        trade_date = t(".//transactionDate/value")
        code = t(".//transactionCoding/transactionCode")

        qty = float(t(".//transactionShares/value") or 0)
        price = float(t(".//transactionPricePerShare/value") or 0)
        value = qty * price

        rows.append({
            "trade_date": trade_date,
            "ticker": ticker,
            "insider_name": insider,
            "title": title,
            "trade_type": code,
            "price": price,
            "qty": qty,
            "value": value
        })

    return rows



def alert_condition(cfg: dict, filing_data: Dict[str, Any]) -> bool:
    alert_cfg = cfg.get("alert", {})
    if not alert_cfg.get("enabled", True):
        return False

    ticker = (filing_data.get("ticker") or "").upper().strip()
    whitelist = [t.upper().strip() for t in alert_cfg.get("tickers_whitelist", []) if t.strip()]
    if whitelist and ticker not in whitelist:
        return False

    needed_code = (alert_cfg.get("transaction_code", "P") or "P").upper().strip()
    has_code = any((tx.get("code") or "").upper().strip() == needed_code for tx in filing_data.get("transactions", []))
    if not has_code:
        return False

    min_value = float(alert_cfg.get("min_total_value_usd", 0))
    return float(filing_data.get("total_value_usd", 0.0)) >= min_value


def run_live(cfg: dict, headers: Dict[str, str], state: dict) -> None:
    sec_cfg = cfg["sec"]
    atom_url = sec_cfg["live_atom_url"]
    max_proc = int(sec_cfg.get("max_live_process_per_run", 20))

    seen_ids = set(state.get("seen_live_ids", []))

    atom = fetch_text(atom_url, headers=headers, retries=3, timeout=45)
    entries = parse_atom_entries(atom)

    # quali sono nuovi
    new_entries = [e for e in entries if e["id"] not in seen_ids]
    print(f"[live] Entry lette: {len(entries)} | nuove: {len(new_entries)}")

    # aggiorna stato: dedup mantenendo ordine
    ids_now = [e["id"] for e in entries]
    dedup_ids = list(dict.fromkeys(ids_now))
    state["seen_live_ids"] = dedup_ids[:500]

    processed = 0
    emailed = 0

    for e in new_entries:
        if processed >= max_proc:
            break
        processed += 1

        filing_index_url = e.get("link", "")
        if not filing_index_url:
            continue

        try:
            # scarica filing index html
            filing_index_html = fetch_text(filing_index_url, headers=headers, retries=2, timeout=45)
            xml_url = find_filing_xml_url(filing_index_html)
            if not xml_url:
                print(f"[live] XML non trovato per: {filing_index_url}")
                continue

            # scarica xml
            form4_xml = fetch_text(xml_url, headers=headers, retries=2, timeout=45)
            filing_data = parse_form4_xml(form4_xml)

            record = {
                "source": "live",
                "atom_id": e["id"],
                "updated": e["updated"],
                "title": e["title"],
                "filing_index_url": filing_index_url,
                "form4_xml_url": xml_url,
                "ticker": filing_data.get("ticker", ""),
                "total_value_usd": filing_data.get("total_value_usd", 0.0),
                "transactions": filing_data.get("transactions", []),
            }
            append_jsonl(HISTORY_PATH, [record])

            if alert_condition(cfg, filing_data):
                emailed += 1
                subject = f"[ALERT] Form 4 {record['ticker']} value≈{int(record['total_value_usd'])}$"
                body = (
                    "Nuovo Form 4 che soddisfa la condizione.\n\n"
                    f"Ticker: {record['ticker']}\n"
                    f"Titolo: {record['title']}\n"
                    f"Updated: {record['updated']}\n"
                    f"Valore stimato (shares*price): {record['total_value_usd']:.2f} USD\n"
                    f"Filing index: {record['filing_index_url']}\n"
                    f"XML: {record['form4_xml_url']}\n\n"
                    "Transazioni:\n"
                    + "\n".join(
                        [
                            f"- code={tx['code']} shares={tx['shares']} price={tx['price']} value={tx['value']}"
                            for tx in record["transactions"]
                        ]
                    )
                )
                send_email(cfg, subject, body)

            # piccola pausa tra filing per non stressare SEC
            time.sleep(0.5)

        except Exception as ex:
            print(f"[live] Errore processing entry: {ex}")

    print(f"[live] Processate: {processed} | Email inviate: {emailed}")


# -----------------------------
# MAIN
# -----------------------------
def main():
    cfg = load_json(CONFIG_PATH, {})
    if "sec" not in cfg or "live_atom_url" not in cfg["sec"]:
        raise RuntimeError("config.json non valido: manca sec.live_atom_url")

    # Header più “browser-like” e con User-Agent esplicito
    headers = {
        "User-Agent": cfg["sec"].get("user_agent", "InsiderMonitor/1.0 (contact=example@example.com)"),
        "Accept": "application/xml,text/xml,application/atom+xml,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    state = load_json(
        STATE_PATH,
        {
            "seen_live_ids": [],
            "history_seen_filenames": [],
        },
    )
    state.setdefault("bootstrap_done", False)

    ensure_history_file()
        # Debug: invio email di test (una volta sola, poi disattivalo nel config)
    if cfg.get("debug", {}).get("send_test_email", False):
        try:
            send_email(cfg, "[TEST] SEC monitor", "Email di test inviata correttamente da GitHub Actions.")
            print("[email] Test email inviata.")
        except Exception as e:
            print(f"[email] ERRORE invio test email: {e}")
            raise

    # Bootstrap SOLO una volta (come vuoi tu).
    if not state.get("bootstrap_done", False):
        print("[history] Bootstrap iniziale in corso...")
        try:
            bootstrap_history_once(cfg, headers, state)
            state["bootstrap_done"] = True
            state["bootstrap_completed_at"] = datetime.now(timezone.utc).isoformat()
            print("[history] Bootstrap completato.")
        except Exception as e:
            # Non blocchiamo il live: e dato che accetti anche “bootstrap incompleto”,
            # lo marchiamo come done e passiamo a live-only.
            state["bootstrap_done"] = True
            state["bootstrap_completed_at"] = datetime.now(timezone.utc).isoformat()
            state["bootstrap_error"] = str(e)
            print(f"[history] Bootstrap fallito, passo a live-only: {e}")

    run_live(cfg, headers, state)

    save_json(STATE_PATH, state)
    print("OK: state.json aggiornato.")


if __name__ == "__main__":
    main()
