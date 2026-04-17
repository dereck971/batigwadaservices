#!/usr/bin/env python3
"""
Scraper Pages Jaunes — enrichit data/artisans.json avec les numéros manquants.

Usage:
    pip install requests beautifulsoup4 lxml
    python3 scripts/scrape_pagesjaunes.py           # run full
    python3 scripts/scrape_pagesjaunes.py --limit 10  # test batch
    python3 scripts/scrape_pagesjaunes.py --resume  # reprise après crash

Logique:
    1. Charge data/artisans.json
    2. Pour chaque entrée SANS téléphone, cherche "nom commune" sur PagesJaunes
    3. Matche si nom similaire (ratio > 0.85) ET commune identique
    4. Ajoute telephone + met à jour source
    5. Sauvegarde incrémentale toutes les 50 entrées

Respecte les règles de scraping:
    - 3-5 sec entre requêtes (randomisé)
    - User-Agent navigateur
    - Backoff exponentiel sur 403/429
    - Résumable via fichier .progress
"""
import json
import time
import random
import re
import argparse
import sys
from pathlib import Path
from difflib import SequenceMatcher
from urllib.parse import quote_plus

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Installe d'abord: pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# --- CONFIG ---
DATA_FILE = Path("data/artisans.json")
PROGRESS_FILE = Path("data/.scrape_progress.json")
BASE_URL = "https://www.pagesjaunes.fr/annuaire/chercherlespros"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
MIN_DELAY = 3.0
MAX_DELAY = 5.5
SIMILARITY_THRESHOLD = 0.85


def normalize(s: str) -> str:
    """Lowercase, remove accents, collapse whitespace."""
    import unicodedata
    s = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return " ".join(s.split())


def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def normalize_phone(raw: str) -> str:
    """Format French phone: '0X XX XX XX XX'."""
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("33"):
        digits = "0" + digits[2:]
    if len(digits) == 10 and digits[0] == "0":
        return " ".join([digits[0:2], digits[2:4], digits[4:6], digits[6:8], digits[8:10]])
    return raw.strip()


def search_pagesjaunes(session, name: str, commune: str, retries: int = 3):
    """Search and return (phone, matched_name) or (None, None)."""
    params = {"quoiqui": name, "ou": f"{commune}, Guadeloupe"}
    for attempt in range(retries):
        try:
            r = session.get(BASE_URL, params=params, timeout=15)
            if r.status_code == 200:
                return parse_results(r.text, name, commune)
            if r.status_code in (403, 429):
                wait = 60 * (2 ** attempt)
                print(f"  [rate-limit {r.status_code}] sleep {wait}s...", flush=True)
                time.sleep(wait)
                continue
            return None, None
        except requests.RequestException as e:
            print(f"  [err] {e}", flush=True)
            time.sleep(10)
    return None, None


def parse_results(html: str, target_name: str, target_commune: str):
    """Extract phone of best matching result."""
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("li.bi, article.bi")  # bi = "bloc info"
    best = (0.0, None, None)
    for card in cards:
        name_el = card.select_one("a.denomination-links, .denomination, h3")
        if not name_el:
            continue
        result_name = name_el.get_text(strip=True)
        # Commune check
        loc_el = card.select_one(".adresse, address")
        result_loc = loc_el.get_text(" ", strip=True) if loc_el else ""
        if normalize(target_commune) not in normalize(result_loc):
            continue
        ratio = similar(target_name, result_name)
        if ratio > best[0]:
            phone_el = card.select_one(
                ".bi-bloc-info-telephone .coord-numero, "
                ".tel, "
                "a[href^='tel:'], "
                "span[itemprop='telephone']"
            )
            phone = None
            if phone_el:
                phone = phone_el.get_text(strip=True) or phone_el.get("href", "").replace("tel:", "")
            if phone:
                best = (ratio, phone, result_name)
    if best[0] >= SIMILARITY_THRESHOLD and best[1]:
        return normalize_phone(best[1]), best[2]
    return None, None


def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"done_indices": [], "added": 0, "failed": 0, "notfound": 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="limit N entries (0=all)")
    parser.add_argument("--resume", action="store_true", help="resume from progress")
    parser.add_argument("--dry-run", action="store_true", help="no save")
    args = parser.parse_args()

    data = json.loads(DATA_FILE.read_text())
    progress = load_progress() if args.resume else {"done_indices": [], "added": 0, "failed": 0, "notfound": 0}
    done = set(progress["done_indices"])

    # Candidates: no phone, has name + commune
    candidates = [
        (i, a) for i, a in enumerate(data)
        if not (a.get("telephone", "") or "").strip()
        and (a.get("nom_entreprise", "") or "").strip()
        and (a.get("commune", "") or "").strip()
        and i not in done
    ]
    if args.limit:
        candidates = candidates[: args.limit]

    print(f"=== Scraping Pages Jaunes ===")
    print(f"Candidates à traiter: {len(candidates)}")
    print(f"Délai entre requêtes: {MIN_DELAY}-{MAX_DELAY}s")
    print(f"Progression: {progress['added']} ajoutés, {progress['notfound']} non trouvés, {progress['failed']} échecs\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        for idx, (i, a) in enumerate(candidates, 1):
            name = a["nom_entreprise"]
            commune = a["commune"]
            phone, matched = search_pagesjaunes(session, name, commune)

            if phone:
                data[i]["telephone"] = phone
                if not (data[i].get("source") or "").strip():
                    data[i]["source"] = "PagesJaunes (scraping)"
                else:
                    data[i]["source"] = data[i]["source"] + " + PagesJaunes"
                progress["added"] += 1
                print(f"  [{idx}/{len(candidates)}] ✓ {name[:40]:40} → {phone}  (match: {matched[:40]})")
            else:
                progress["notfound"] += 1
                print(f"  [{idx}/{len(candidates)}] ✗ {name[:40]:40} | {commune}")

            progress["done_indices"].append(i)

            # Incremental save every 50
            if idx % 50 == 0 and not args.dry_run:
                DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
                save_progress(progress)
                print(f"  [save] {progress['added']} ajoutés / {idx} traités")

            # Polite delay
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    except KeyboardInterrupt:
        print("\n[interrupted]")

    finally:
        if not args.dry_run:
            DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
            save_progress(progress)

    print(f"\n=== Terminé ===")
    print(f"Ajoutés: {progress['added']}")
    print(f"Non trouvés: {progress['notfound']}")
    print(f"Échecs: {progress['failed']}")


if __name__ == "__main__":
    main()
