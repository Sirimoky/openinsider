import json
import os
import re
import time
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import tz
import xml.etree.ElementTree as ET
import smtplib
from email.message import EmailMessage

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"
HISTORY_PATH = "history.jsonl"


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


def fetch_text(url: str, headers: Dict[str, str], retries: int = 3, timeout: int = 30) -> str:
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


# -------------------------
# STORICO via master.idx giornaliero
# -------------------------

def quarter_for_date(d: datetime) -> int:
    return (d.month - 1) // 3 + 1


def daily_master_idx_url(d: datetime) -> str:
    q = quarter_for_date(d)
    y = d.year
    datestr = d.strftime("%Y%m%d")
    return f"https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/master.{datestr}.idx"


def parse_master_idx(text: str) -> List[Dict[str, str]]:
    rows = []
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
        rows.append({
            "cik": cik,
            "company_name": name,
            "form_type": form,
            "date_filed": date_filed,
            "filename": filename
        })
    return rows


def bootstrap_history(cfg: dict, headers: Dict[str, str], state: dict) -> None:
    sec_cfg = cfg["sec"]
    days = int(sec_cfg.get("history_days", 90))
    max_rows = int(sec_cfg.get("max_history_rows", 50000))

    seen = set(state.get("history_seen_filenames", []))
    today = datetime.now(timezone.utc).date()

    new_rows = []
    added = 0

    for delta in range(1, days + 1):
        d = today - timedelta(days=delta)
        dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        url = daily_master_idx_url(dt)

        try:
            idx_text = fetch_text(url, headers=headers, retries=2, timeout=30)
            time.sleep(0.25)
        except Exception as e:
            print(f"[history] Skip {d} (non disponibile): {e}")
            time.sleep(0.25)
            continue

        rows = parse_master_idx(idx_text)
        for r in rows:
            if r.get("form_type") != "4":
                continue
            fn = r["filename"]
            if fn in seen:
                continue
            seen.add(fn)
            new_rows.append({
                "source": "master.idx",
                "filename": fn,
                "date_filed": r["date_filed"],
                "company_name": r["company_name"],
                "cik": r["cik"],
                "form_type": r["form_type"],
                "filing_url": f"https://www.sec.gov/Archives/{fn}"
            })
            added += 1
            if len(seen) >= max_rows:
                break

        if len(seen) >= max_rows:
            break

    # salva su history.jsonl
    append_jsonl(HISTORY_PATH, new_rows)

    # limita seen per non crescere all’infinito
    state["history_seen_filenames"] = list(seen)[-max_rows:]
    print(f"[history] Nuove righe storico aggiunte: {added}")


# -------------------------
# LIVE Atom + parse Form4 XML
# -------------------------

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
    m = re.search(r'href="([^"]+\.xml)"', filing_index_html, flags=re.IGNORECASE)
    if not m:
        return None
    href = m.group(1)
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.sec.gov" + href
    return "https://www.sec.gov/" + href.lstrip("/")


def parse_form4_xml(form4_xml: str) -> Dict[str, Any]:
    root = ET.fromstring(form4_xml)

    def find_text(path: str) -> str:
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else ""

    ticker = find_text(".//issuerTradingSymbol")

    txs = []
    total_value = 0.0

    for tx in root.findall(".//nonDerivativeTransaction"):
        code = ""
        code_el = tx.find(".//transactionCoding/transactionCode")
        if code_el is not None and code_el.text:
            code = code_el.text.strip()

        shares = 0.0
        price = 0.0

        shares_el = tx.find(".//transactionAmounts/transactionShares/value")
        price_el = tx.find(".//transactionAmounts/transactionPricePerShare/value")

        if shares_el is not None and shares_el.text:
            try:
                shares = float(shares_el.text.strip())
            except ValueError:
                shares = 0.0
        if price_el is not None and price_el.text:
            try:
                price = float(price_el.text.strip())
            except ValueError:
                price = 0.0

        value = shares * price
        txs.append({"code": code, "shares": shares, "price": price, "value": value})
        total_value += value

    return {"ticker": ticker, "transactions": txs, "total_value_usd": total_value}


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

    atom = fetch_text(atom_url, headers=headers, retries=3, timeout=30)
    entries = parse_atom_entries(atom)

    new_entries = [e for e in entries if e["id"] not in seen_ids]
    print(f"[live] Entry lette: {len(entries)} | nuove: {len(new_entries)}")

    # aggiorna visto (teniamo 500)
    state["seen_live_ids"] = [e["id"] for e in entries][:500]

    alerted = 0
    processed = 0

    for e in new_entries:
        if processed >= max_proc:
            break
        processed += 1

        filing_index_url = e.get("link", "")
        if not filing_index_url:
            continue

        try:
            filing_index_html = fetch_text(filing_index_url, headers=headers, retries=2, timeout=30)
            xml_url = find_filing_xml_url(filing_index_html)
            if not xml_url:
                print(f"[live] XML non trovato per: {filing_index_url}")
                continue

            form4_xml = fetch_text(xml_url, headers=headers, retries=2, timeout=30)
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
                "transactions": filing_data.get("transactions", [])
            }
            append_jsonl(HISTORY_PATH, [record])

            if alert_condition(cfg, filing_data):
                alerted += 1
                subject = f"[ALERT] Form 4 {record['ticker']} value≈{int(record['total_value_usd'])}$"
                body = (
                    "Nuovo Form 4 che soddisfa la condizione.\n\n"
                    f"Ticker: {record['ticker']}\n"
                    f"Titolo: {record['title']}\n"
                    f"Updated: {record['updated']}\n"
                    f"Valore stimato (shares*price): {record['total_value_usd']:.2f} USD\n"
                    f"Filing index: {record['filing_index_url']}\n"
                    f"XML: {record['form4_xml_url']}\n\n"
                    "Transazioni:\n" +
                    "\n".join([f"- code={tx['code']} shares={tx['shares']} price={tx['price']} value={tx['value']}"
                               for tx in record["transactions"]])
                )
                send_email(cfg, subject, body)

        except Exception as ex:
            print(f"[live] Errore processing entry: {ex}")

    print(f"[live] Processate: {processed} | Email inviate: {alerted}")


def main():
    cfg = load_json(CONFIG_PATH, {})
    if "sec" not in cfg or "live_atom_url" not in cfg["sec"]:
        raise RuntimeError("config.json non valido: manca sec.live_atom_url")

    headers = {
    "User-Agent": cfg["sec"].get("user_agent", "InsiderMonitor/1.0 (contact=example@example.com)"),
    "Accept": "application/xml,text/xml,application/atom+xml,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}
    if not os.path.exists(HISTORY_PATH):
        open(HISTORY_PATH, "a", encoding="utf-8").close()

    state = load_json(STATE_PATH, {"seen_live_ids": [], "history_seen_filenames": []})
    
    bootstrap_history(cfg, headers, state)
    run_live(cfg, headers, state)

    save_json(STATE_PATH, state)
    print("OK: state.json aggiornato.")


if __name__ == "__main__":
    main()
