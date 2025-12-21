import json
import os
import time
import smtplib
from email.message import EmailMessage
import xml.etree.ElementTree as ET

import requests

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_text(url, headers, retries=3, timeout=30):
    last = None
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last = e
            print(f"Tentativo {i}/{retries} fallito: {e}")
            time.sleep(2 * i)
    raise RuntimeError(f"Impossibile scaricare {url}: {last}")


def parse_atom_entries(atom_xml):
    root = ET.fromstring(atom_xml)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    entries = []
    for entry in root.findall("atom:entry", ns):
        entry_id = (entry.findtext("atom:id", default="", namespaces=ns) or "").strip()
        title = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
        updated = (entry.findtext("atom:updated", default="", namespaces=ns) or "").strip()

        link = ""
        link_el = entry.find("atom:link", ns)
        if link_el is not None and "href" in link_el.attrib:
            link = link_el.attrib["href"].strip()

        if entry_id:
            entries.append({"id": entry_id, "title": title, "updated": updated, "link": link})
    return entries


def send_email(cfg, subject, body):
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


def main():
    cfg = load_json(CONFIG_PATH, {})
    sec_cfg = cfg.get("sec", {})
    if "live_atom_url" not in sec_cfg:
        raise RuntimeError("config.json: manca sec.live_atom_url")

    headers = {
        "User-Agent": sec_cfg.get("user_agent", "InsiderMonitor/1.0 (contact=example@example.com)"),
        "Accept": "application/atom+xml,application/xml,text/xml,*/*",
    }

    state = load_json(STATE_PATH, {"seen_live_ids": []})
    seen = set(state.get("seen_live_ids", []))

    atom_url = sec_cfg["live_atom_url"]
    print(f"Fetch ATOM: {atom_url}")
    atom = fetch_text(atom_url, headers=headers)

    entries = parse_atom_entries(atom)
    print(f"Entry lette: {len(entries)}")

    new_entries = [e for e in entries if e["id"] not in seen]
    print(f"Nuove: {len(new_entries)}")

    # aggiorna stato (teniamo i 300 più recenti)
    ids_now = [e["id"] for e in entries]
    state["seen_live_ids"] = ids_now[:300]
    save_json(STATE_PATH, state)

    # TEST EMAIL (disattivato): lo useremo dopo con la condizione
    # if new_entries:
    #     send_email(cfg, "Test alert", f"Nuove entry: {len(new_entries)}")

    print("OK: state.json aggiornato.")


if __name__ == "__main__":
    main()
