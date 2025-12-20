import json
import os
import time
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

import requests

CONFIG_PATH = "config.json"
STATE_PATH = "state.json"


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fetch_text(url: str, user_agent: str) -> str:
    last_err = None
    for attempt in range(1, 4):  # 3 tentativi
        try:
            r = requests.get(
                url,
                headers={
                    "User-Agent": user_agent,
                    "Accept": "application/atom+xml,application/xml,text/xml,*/*",
                },
                timeout=30,
            )
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_err = e
            print(f"Tentativo {attempt}/3 fallito: {e}")
            time.sleep(3 * attempt)
    raise RuntimeError(f"Impossibile raggiungere {url} dopo 3 tentativi: {last_err}")


def parse_sec_atom(atom_xml: str) -> List[Dict[str, str]]:
    """
    Parse minimale del feed Atom SEC "getcurrent" (Form 4).
    Ritorna una lista di entry con id, title, updated, link.
    """
    root = ET.fromstring(atom_xml)

    # namespace Atom
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
            entries.append(
                {"id": entry_id, "title": title, "updated": updated, "link": link}
            )

    return entries


def main():
    cfg = load_json(CONFIG_PATH, {})
    url = cfg["url"]
    ua = cfg.get("user_agent", "InsiderMonitor/1.0 (contact=example@example.com)")
    max_items = int(cfg.get("max_items_to_track", 300))

    # Stato: lista degli ultimi ID visti
    state = load_json(STATE_PATH, {"seen_ids": []})
    seen_ids = set(state.get("seen_ids", []))

    print(f"Fetch ATOM: {url}")
    try:
        atom = fetch_text(url, ua)
    except Exception as e:
        print(f"ERRORE rete o sito non raggiungibile: {e}")
        # usciamo puliti (workflow verde) e riproverà al prossimo giro
        return

    entries = parse_sec_atom(atom)
    if not entries:
        print("Nessuna entry trovata nel feed (strano).")
        return

    # Di solito il feed è già ordinato (nuovi prima). Manteniamo le prime N.
    entries = entries[:max_items]

    ids_now = [e["id"] for e in entries]
    new_entries = [e for e in entries if e["id"] not in seen_ids]

    print(f"Entry lette: {len(entries)} | nuove: {len(new_entries)}")

    # Debug: stampa le prime 3 nuove
    for e in new_entries[:3]:
        print(f"NUOVO: {e['updated']} | {e['title']} | {e['link']}")

    # Salva stato aggiornato (solo gli ultimi N)
    save_json(STATE_PATH, {"seen_ids": ids_now})

    # Qui, più avanti, applicheremo la condizione sulle new_entries
    # e invieremo email se serve.


if __name__ == "__main__":
    main()
