import json
import os
import re
import hashlib
from typing import List, Dict, Any, Optional

import requests
import pandas as pd

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
    r = requests.get(
        url,
        headers={"User-Agent": user_agent, "Accept": "text/html,*/*"},
        timeout=30,
    )
    r.raise_for_status()
    return r.text


def find_csv_url(html: str) -> Optional[str]:
    """
    Cerca un link CSV nel contenuto HTML.
    OpenInsider spesso espone un link tipo '.../screener?....&f=1&...&o=csv' o 'Download CSV'.
    Qui cerchiamo in modo generico un href che contenga 'csv' o 'Download CSV'.
    """
    # cattura href="...csv..."
    m = re.search(r'href="([^"]*csv[^"]*)"', html, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # cattura href='...csv...'
    m = re.search(r"href='([^']*csv[^']*)'", html, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    return None


def absolutize(base_url: str, maybe_relative: str) -> str:
    if maybe_relative.startswith("http://") or maybe_relative.startswith("https://"):
        return maybe_relative
    # OpenInsider usa spesso path assoluti tipo /screener?...
    if maybe_relative.startswith("/"):
        return "http://openinsider.com" + maybe_relative
    # altrimenti relativo semplice
    return base_url.rstrip("/") + "/" + maybe_relative.lstrip("/")


def download_csv(csv_url: str, user_agent: str) -> pd.DataFrame:
    r = requests.get(
        csv_url,
        headers={"User-Agent": user_agent, "Accept": "text/csv,*/*"},
        timeout=30,
    )
    r.raise_for_status()

    # pandas legge da bytes via buffer
    from io import BytesIO
    return pd.read_csv(BytesIO(r.content))


def make_row_key(row: pd.Series) -> str:
    """
    Chiave “abbastanza unica” basata sul contenuto della riga.
    Per evitare dipendenza da un singolo campo, facciamo hash dell’intera riga normalizzata.
    """
    parts = []
    for v in row.values.tolist():
        s = "" if pd.isna(v) else str(v).strip()
        parts.append(s)
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def main():
    cfg = load_json(CONFIG_PATH, {})
    url = cfg["url"]
    ua = cfg.get("user_agent", "Mozilla/5.0")
    max_rows = int(cfg.get("max_rows_to_track", 300))

    state = load_json(STATE_PATH, {"seen_keys": []})
    seen_keys = set(state.get("seen_keys", []))

    print(f"Fetch HTML: {url}")
    html = fetch_text(url, ua)

    csv_link = find_csv_url(html)
    if not csv_link:
        # fallback: se non troviamo CSV dalla homepage, stampiamo un messaggio chiaro.
        print("Non ho trovato un link CSV nell'HTML della pagina configurata.")
        print("Nel prossimo step useremo direttamente l'URL della vista 'screener' o 'latest' con CSV.")
        return

    csv_url = absolutize(url, csv_link)
    print(f"Trovato CSV: {csv_url}")

    df = download_csv(csv_url, ua)
    if df.empty:
        print("CSV scaricato ma tabella vuota.")
        return

    # limitiamo righe per non far crescere lo stato all’infinito
    df = df.head(max_rows).copy()

    # genera chiavi
    keys: List[str] = [make_row_key(df.iloc[i]) for i in range(len(df))]

    new_keys = [k for k in keys if k not in seen_keys]
    print(f"Righe lette: {len(df)} | nuove: {len(new_keys)}")

    # Aggiorna stato: teniamo le ultime N chiavi viste (order: dalla più recente)
    # Qui assumiamo che il CSV sia ordinato “nuovi prima”. Se non lo è, lo sistemiamo dopo.
    updated_keys = keys[:max_rows]
    save_json(STATE_PATH, {"seen_keys": updated_keys})

    # Per ora: solo log. Quando aggiungeremo la condizione, useremo le righe nuove.
    if new_keys:
        print("Ci sono nuove righe (non ancora notifica email in questa fase).")


if __name__ == "__main__":
    main()
