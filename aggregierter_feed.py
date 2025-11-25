#!/usr/bin/env python3
"""
Aggregierter RSS-Feed-Generator — erweiterte Version

Features:
- Lädt eine CSV mit Vereinsnamen + optionalen News-/RSS-URLs.
- Versucht, RSS-Feeds automatisch zu erkennen.
- Scrapt News-Seiten heuristisch falls kein Feed vorhanden.
- Dedup via SQLite (seen.db).
- Generiert vereine_feed.xml.
"""

import csv
import os
import re
import sqlite3
import time
from datetime import datetime
from email.utils import format_datetime

import feedparser
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# --- Konfiguration ----------------------------------------------------------
CSV_FILE = "clubs.csv"
OUTPUT_RSS = "vereine_feed.xml"
SEEN_DB = "seen.db"
USER_AGENT = "VereineRSSAggregator/1.0 (+https://example.com)"
REQUEST_TIMEOUT = 12
MAX_ITEMS_PER_SOURCE = 8

# --- DB für bereits gesehene GUIDs -----------------------------------------
class SeenStore:
    def __init__(self, path=SEEN_DB):
        self.conn = sqlite3.connect(path)
        self._init()

    def _init(self):
        c = self.conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            id INTEGER PRIMARY KEY,
            guid TEXT UNIQUE,
            first_seen TIMESTAMP
        )""")
        self.conn.commit()

    def is_seen(self, guid):
        c = self.conn.cursor()
        c.execute("SELECT 1 FROM seen WHERE guid = ?", (guid,))
        return c.fetchone() is not None

    def mark_seen(self, guid):
        c = self.conn.cursor()
        try:
            c.execute("INSERT INTO seen (guid, first_seen) VALUES (?, ?)", (guid, datetime.utcnow()))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

# --- HTTP GET ---------------------------------------------------------------
def http_get(url):
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r

# --- RSS Entdeckung --------------------------------------------------------
def discover_rss_from_url(url):
    """Sucht auf der Seite nach RSS/Atom <link> oder prüft gängige feed-Pfade."""
    try:
        r = http_get(url)
    except Exception as e:
        print(f"Fehler beim Laden von {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    link_tags = soup.find_all("link", type=re.compile(r"(application|text)/rss\+xml|application/atom\+xml"))
    for lt in link_tags:
        href = lt.get("href")
        if href:
            return requests.compat.urljoin(url, href)

    # gängige Kandidaten prüfen
    candidates = [
        url.rstrip("/") + "/feed",
        url.rstrip("/") + "/rss",
        url.rstrip("/") + "/rss.xml",
        url.rstrip("/") + "/news/rss",
        url.rstrip("/") + "/news/feed",
        url.rstrip("/") + "/aktuelles/feed",
    ]
    for c in candidates:
        try:
            rr = requests.get(c, headers={"User-Agent": USER_AGENT}, timeout=6)
            if rr.status_code == 200 and ("xml" in rr.headers.get("Content-Type", "") or re.search(r"<rss|<feed", rr.text)):
                return c
        except Exception:
            continue

    return None

# --- Feed lesen -------------------------------------------------------------
def fetch_feed_items(rss_url):
    try:
        f = feedparser.parse(rss_url)
    except Exception as e:
        print(f"feedparser Fehler für {rss_url}: {e}")
        return []

    items = []
    for e in f.entries[:MAX_ITEMS_PER_SOURCE]:
        title = e.get("title") or "(kein Titel)"
        link = e.get("link") or e.get("id")
        if not link:
            continue
        desc = e.get("summary", "")
        pub = e.get("published_parsed")
        if pub:
            pubdate = format_datetime(datetime.fromtimestamp(time.mktime(pub)))
        else:
            pubdate = e.get("published") or e.get("updated") or format_datetime(datetime.utcnow())

        items.append({"title": title, "link": link, "description": desc, "pubDate": pubdate})
    return items

# --- Heuristisches Scrapen --------------------------------------------------
def scrape_for_articles(page_url):
    """Extrahiert artikel-ähnliche Links von einer News/Startseite anhand heuristischer Regeln."""
    try:
        r = http_get(page_url)
    except Exception as e:
        print(f"Fehler beim Laden von {page_url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a.get("href")
        text = a.get_text(strip=True)
        if not text or len(text) < 5:
            continue
        if href.startswith("#") or href.startswith("mailto:"):
            continue
        full = requests.compat.urljoin(page_url, href)
        if re.search(r"/news/|/article/|/artikel/|/aktuell|/presse|/blog|/berichte|/nachrichten|/news\-|\d{4}/\d{2}", full, re.I):
            candidates.append((full, text))
        else:
            parts = requests.utils.urlparse(full).path.split('/')
            if len([p for p in parts if p]) >= 3:
                candidates.append((full, text))

    seen = set()
    unique = []
    for link, text in candidates:
        if link in seen:
            continue
        seen.add(link)
        unique.append((link, text))
        if len(unique) >= MAX_ITEMS_PER_SOURCE:
            break

    items = []
    for link, text in unique:
        items.append({
            "title": text,
            "link": link,
            "description": f"Artikel von {page_url}",
            "pubDate": format_datetime(datetime.utcnow()),
        })
    return items

# --- CSV Loader -------------------------------------------------------------
def load_clubs(csv_path=CSV_FILE):
    clubs = []
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV-Datei {csv_path} nicht gefunden. Bitte erstelle sie mit den Vereinsnamen.")
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('club') or row.get('name')
            url = row.get('news_url') or row.get('url') or None
            clubs.append({'name': name.strip() if name else None, 'url': url.strip() if url else None})
    return clubs

# --- Aggregation ------------------------------------------------------------
def aggregate(clubs, seen_store):
    all_items = []
    for c in clubs:
        name = c['name']
        url = c['url']
        print(f"Processing: {name} ({url or 'no url provided'})")

        source_items = []
        if url:
            rss = discover_rss_from_url(url)
            if rss:
                print(f"  Gefundenes RSS für {name}: {rss}")
                source_items = fetch_feed_items(rss)
            else:
                guess_paths = ["/news", "/aktuelles", "/presse", "/news/feed", "/de/aktuelles"]
                found = False
                for gp in guess_paths:
                    guess = url.rstrip('/') + gp
                    try:
                        r = requests.get(guess, headers={"User-Agent": USER_AGENT}, timeout=6)
                        if r.status_code == 200 and len(r.text) > 200:
                            print(f"  Versuche zu scrapen: {guess}")
                            source_items = scrape_for_articles(guess)
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    print(f"  Keine spezielle News-Seite gefunden, scrappe die gegebene URL: {url}")
                    source_items = scrape_for_articles(url)
        else:
            print(f"  Keine URL angegeben für {name}; überspringe. (Du kannst sie in clubs.csv ergänzen)")

        for it in source_items:
            if ' - ' not in it['title'] and not it['title'].startswith(name+':'):
                it['title'] = f"{name}: {it['title']}"
            it['description'] = f"Quelle: {name}. {it.get('description','')}"

        new_items = []
        for it in source_items:
            guid = it['link']
            if seen_store.is_seen(guid):
                continue
            seen_store.mark_seen(guid)
            new_items.append(it)

        all_items.extend(new_items)

    def parse_date(s):
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00'))
        except Exception:
            try:
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(s)
            except Exception:
                return datetime.utcnow()

    all_items.sort(key=lambda x: parse_date(x.get('pubDate', '')), reverse=True)
    return all_items

# --- RSS Builder ------------------------------------------------------------
def build_rss(items):
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "Aggregierte Vereins-News"
    ET.SubElement(channel, "link").text = "https://example.com/vereine-feed.xml"
    ET.SubElement(channel, "description").text = "Tagesaktuelle Nachrichten mehrerer deutscher Sportvereine"
    ET.SubElement(channel, "language").text = "de-DE"
    ET.SubElement(channel, "lastBuildDate").text = format_datetime(datetime.utcnow())

    for it in items:
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = it.get('title')
        ET.SubElement(item, "link").text = it.get('link')
        ET.SubElement(item, "description").text = it.get('description')
        ET.SubElement(item, "pubDate").text = it.get('pubDate')
        ET.SubElement(item, "guid").text = it.get('link')

    return ET.tostring(rss, encoding="utf-8", xml_declaration=True)

# --- Main ------------------------------------------------------------------
def main():
    seen = SeenStore()
    clubs = load_clubs()
    items = aggregate(clubs, seen)
    xml = build_rss(items)
    with open(OUTPUT_RSS, 'wb') as f:
        f.write(xml)
    print(f"Erfolg: {OUTPUT_RSS} erzeugt. ({len(items)} neue Items)")

if __name__ == '__main__':
    main()
