#!/usr/bin/env python3
"""PC part price tracker: RAM, SSD and GPU prices with history.

    python3 tracker.py serve     # dashboard at http://127.0.0.1:8765 + re-check every 2 hours
    python3 tracker.py update    # fetch once and exit

Uses only the Python standard library. Data comes from two public sources that
allow automated access:

  * Pangoly price-trend series (daily average / lowest / highest retail price per
    RAM, SSD and GPU class, back to 2022), read from Internet Archive snapshots.
  * HardwareDeals.co open GPU dataset on GitHub (daily eBay listing snapshots,
    from September 2025), reduced here to a daily median per GPU model.
"""
import datetime as dt
import gzip
import io
import json
import os
import re
import statistics
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
STORE_PATH = os.path.join(DATA, "store.json")
OUT_PATH = os.path.join(DATA, "prices.js")
STATUS_PATH = os.path.join(DATA, "status.json")
UA = "pc-price-tracker/1.0 (personal price chart)"
DEFAULT_INTERVAL_HOURS = 2.0
PORT = 8765

CDX = ("http://web.archive.org/cdx/search/cdx?url=pangoly.com/en/price-trends/data/"
       "&matchType=prefix&output=json&fl=original,timestamp&filter=statuscode:200")

GROUPS = {
    "ram": {"category": "ram", "label": "Retail",
            "metrics": {"avg": "Average", "min": "Lowest", "max": "Highest"},
            "note": "Daily average, lowest and highest retail price across kits of each size "
                    "(Pangoly price trends, via Internet Archive snapshots). "
                    "Sizes marked (eBay) are not covered by that source: they show the median asking price "
                    "of new kits on eBay instead, from September 2025."},
    "ram-new": {"category": "ram", "label": "New (eBay listings)",
                "metrics": {"avg": "Median", "min": "25th percentile", "max": "75th percentile"},
                "note": "Median asking price of new desktop kits listed on eBay in USD, 7-day rolling "
                        "(HardwareDeals.co open dataset). Asking prices, not completed sales."},
    "ssd": {"category": "ssd", "label": "Retail",
            "metrics": {"avg": "Average", "min": "Lowest", "max": "Highest"},
            "note": "Daily average, lowest and highest retail price across drives of each class "
                    "(Pangoly price trends, via Internet Archive snapshots)."},
    "gpu-retail": {"category": "gpu", "label": "Retail",
                   "metrics": {"avg": "Average", "min": "Lowest", "max": "Highest"},
                   "note": "Daily average, lowest and highest retail price across cards of each model "
                           "(Pangoly price trends, via Internet Archive snapshots)."},
    "gpu-new": {"category": "gpu", "label": "New (eBay listings)",
                "metrics": {"avg": "Median", "min": "25th percentile", "max": "75th percentile"},
                "note": "Median asking price of new cards listed on eBay in USD, 7-day rolling "
                        "(HardwareDeals.co open dataset). Asking prices, not completed sales."},
    "gpu-used": {"category": "gpu", "label": "Used (eBay listings)",
                 "metrics": {"avg": "Median", "min": "25th percentile", "max": "75th percentile"},
                 "note": "Median asking price of used and open-box cards listed on eBay in USD, 7-day rolling "
                         "(HardwareDeals.co open dataset). Asking prices, not completed sales."},
}
PANGOLY_GROUP = {"ram": "ram", "ssd": "ssd", "vga": "gpu-retail"}
GPU_MODEL_RE = re.compile(
    r"^(RTX [2345]0[5-9]0( TI)?( SUPER)?|RX [5-9]\d{3}( XTX| XT| GRE)?|ARC [AB]\d{3})$")
# GPUs shown on the chart, fastest first. Every RTX card with data is kept;
# Radeon cards are kept only if they are roughly RTX 4080 class or better.
GPU_RANK = ["RTX 5090", "RTX 4090", "RTX 5080", "RTX 4080 SUPER", "RTX 4080", "RX 7900 XTX",
            "RTX 5070 TI", "RX 9070 XT", "RTX 4070 TI SUPER", "RTX 3090 TI", "RTX 5070", "RTX 4070 TI",
            "RTX 3090", "RTX 4070 SUPER", "RTX 3080 TI", "RTX 4070", "RTX 3080", "RTX 5060 TI",
            "RTX 4060 TI", "RTX 3070 TI", "RTX 3070", "RTX 5060", "RTX 2080 TI", "RTX 4060",
            "RTX 3060 TI", "RTX 2080 SUPER", "RTX 2080", "RTX 5050", "RTX 3060", "RTX 2070 SUPER",
            "RTX 2070", "RTX 2060 SUPER", "RTX 2060", "RTX 3050"]
# RAM kits shown on the chart, in display order: DDR5 largest first, then DDR4.
RAM_RANK = ["128GB DDR5", "64GB DDR5", "32GB DDR5", "16GB DDR5", "8GB DDR5",
            "64GB DDR4", "32GB DDR4", "16GB DDR4", "8GB DDR4"]
MIN_LISTINGS = 4      # listings needed before a day's median counts
MIN_GPU_DAYS = 60     # days of data needed before a GPU model is charted

_lock = threading.Lock()
_status = {"running": False, "lastCheck": None, "nextCheck": None}


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch(url, timeout=45, retries=3):
    """GET a URL with retries; transparently gunzips."""
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            if b"Internet Archive: Temporarily Offline" in body[:400]:
                raise IOError("Internet Archive temporarily offline")
            return body
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            last = e
        except Exception as e:  # network errors, timeouts
            last = e
        time.sleep(4 * (attempt + 1))
    raise IOError("%s" % last)


def load_store():
    try:
        with open(STORE_PATH) as f:
            store = json.load(f)
    except (OSError, ValueError):
        store = {"series": {}, "pangoly": {}}
    try:
        with open(STATUS_PATH) as f:
            store["sources"] = json.load(f)
    except (OSError, ValueError):
        store["sources"] = store.get("sources", {})
    return store


def save_store(store):
    """Prices and check status are kept apart so store.json only changes when prices do."""
    save_json(STORE_PATH, {k: v for k, v in store.items() if k != "sources"})
    save_json(STATUS_PATH, store["sources"])


def save_json(path, obj, prefix=""):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(prefix)
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


def day_number(date):
    return (date - dt.date(1970, 1, 1)).days


# ---------------------------------------------------------------- Pangoly

def pretty(slug):
    words = []
    for w in slug.split("-"):
        if w in ("x4", "x2"):
            continue
        if w == "pcie":
            words.append("NVMe PCIe")
        elif w.startswith("gen"):
            words.append(w.capitalize())
        else:
            words.append(w.upper())
    return " ".join(words)


def update_pangoly(store):
    rows = json.loads(fetch(CDX))[1:]
    latest = {}
    for original, ts in rows:
        m = re.search(r"/price-trends/data/([a-z]+)/([a-z0-9+\-]+)$", original)
        if m and m.group(1) in PANGOLY_GROUP and ts > latest.get(m.groups(), ("", ""))[0]:
            latest[m.groups()] = (ts, original)
    changed, failed = 0, 0
    for (kind, slug), (ts, original) in sorted(latest.items()):
        sid = "pangoly:%s/%s" % (kind, slug)
        if store["pangoly"].get(sid, "") >= ts:
            continue
        time.sleep(2)  # be gentle with the archive
        try:
            raw = json.loads(fetch("http://web.archive.org/web/%sid_/%s" % (ts, original)))
        except Exception as e:
            failed += 1
            log("  could not read %s: %s" % (sid, e))
            continue
        s = store["series"].setdefault(sid, {"group": PANGOLY_GROUP[kind], "name": pretty(slug), "points": {}})
        for metric in ("avg", "min", "max"):
            pts = s["points"].setdefault(metric, {})
            for ms, price in raw.get(metric, []):
                if price and price > 0:
                    pts[str(int(ms // 86400000))] = round(float(price), 2)
        store["pangoly"][sid] = ts
        changed += 1
        log("  %s updated from snapshot %s" % (sid, ts))
    msg = "%d series tracked, %d refreshed this check" % (len(latest), changed)
    if failed:
        msg += ", %d could not be read (will retry)" % failed
    return msg


# ---------------------------------------------------------------- eBay listing datasets

def quantile(sorted_vals, q):
    pos = (len(sorted_vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


USED = ("used", "open box", "certified refurbished", "seller refurbished",
        "excellent - refurbished", "very good - refurbished", "good - refurbished")


def classify_gpu(it):
    model = re.sub(r"\s+", " ", str(it.get("chipset") or "")).strip().upper()
    if not GPU_MODEL_RE.match(model):
        return None
    cond = str(it.get("condition") or "").lower()
    group = "gpu-new" if cond == "new" else "gpu-used" if cond in USED else None
    if not group:
        return None
    name = model.title().replace("Rtx", "RTX").replace("Rx", "RX").replace("Xtx", "XTX") \
                .replace("Xt", "XT").replace("Gre", "GRE")
    return group, name


def classify_ram(it):
    speed = str(it.get("speed") or "").upper()
    gen = "DDR5" if speed.startswith("DDR5") else "DDR4" if speed.startswith("DDR4") else None
    if not gen or it.get("form_factor") != "Desktop" or str(it.get("condition") or "").lower() != "new":
        return None
    try:
        cap = int(float(it.get("capacity_gb")))
    except (TypeError, ValueError):
        return None
    if cap not in (16, 32, 48, 64, 96, 128):
        return None
    return "ram-new", "%dGB %s" % (cap, gen)


DATASETS = {
    "gpu": {"repo": "gpu-deals", "days": "gpu_days", "classify": classify_gpu},
    "ram": {"repo": "ram-deals", "days": "ram_days", "classify": classify_ram},
}


def ingest_day(store, classify, day_str, listings):
    buckets = {}
    for it in listings:
        try:
            if it.get("currency") != "USD":
                continue
            price = float(it.get("price"))
            hit = classify(it)
        except (TypeError, ValueError, AttributeError):
            continue
        if price > 0 and hit:
            buckets.setdefault(hit, []).append(price)
    y, m, d = map(int, day_str.split("-"))
    key = str(day_number(dt.date(y, m, d)))
    for (group, name), prices in buckets.items():
        if len(prices) < MIN_LISTINGS:
            continue
        prices.sort()
        sid = "ebay:%s/%s" % (group, name.upper())
        s = store["series"].setdefault(sid, {"group": group, "name": name, "points": {}})
        s["points"].setdefault("avg", {})[key] = round(statistics.median(prices), 2)
        s["points"].setdefault("min", {})[key] = round(quantile(prices, 0.25), 2)
        s["points"].setdefault("max", {})[key] = round(quantile(prices, 0.75), 2)


def update_dataset(kind):
    cfg = DATASETS[kind]
    base = "HardwareDealsCo/%s" % cfg["repo"]

    def run(store):
        tree = json.loads(fetch("https://api.github.com/repos/%s/git/trees/HEAD?recursive=1" % base))["tree"]
        files = {}
        for node in sorted(tree, key=lambda n: n["path"]):
            m = re.search(r"historical-[a-z\-]+/(?:[A-Z]+-)?(\d{4}-\d{2}-\d{2})\.json$", node["path"])
            if m:
                files[m.group(1)] = node["path"]
        done = set(store.get(cfg["days"], []))
        missing = sorted(d for d in files if d not in done)
        if len(missing) > 25:
            log("  downloading full %s archive for %d days" % (kind.upper(), len(missing)))
            blob = fetch("https://codeload.github.com/%s/tar.gz/refs/heads/main" % base, timeout=600)
            want = {files[d]: d for d in missing}
            with tarfile.open(fileobj=io.BytesIO(blob), mode="r:*") as tar:
                for member in tar:
                    path = member.name.split("/", 1)[-1]
                    if path in want:
                        try:
                            ingest_day(store, cfg["classify"], want[path], json.load(tar.extractfile(member)))
                            done.add(want[path])
                        except ValueError:
                            pass
        else:
            for d in missing:
                ingest_day(store, cfg["classify"], d,
                           json.loads(fetch("https://raw.githubusercontent.com/%s/main/%s" % (base, files[d]))))
                done.add(d)
                log("  %s listings for %s added" % (kind.upper(), d))
        store[cfg["days"]] = sorted(done)
        return "%d daily snapshots, %d new this check" % (len(done), len(missing))
    return run


def smooth(days, vals, window=7):
    """Trailing rolling median: eBay asking prices jump around from day to day."""
    out, start = [], 0
    for i, d in enumerate(days):
        while days[start] < d - window + 1:
            start += 1
        chunk = [v for v in vals[start:i + 1] if v is not None]
        out.append(round(statistics.median(chunk), 2) if chunk else None)
    return out


# ---------------------------------------------------------------- build + run

SOURCES = [
    ("pangoly", "Pangoly price trends (Internet Archive)", "https://pangoly.com/en/price-trends",
     ("ram", "ssd", "gpu-retail"), update_pangoly),
    ("gpu", "HardwareDeals.co open GPU dataset (GitHub)", "https://github.com/HardwareDealsCo/gpu-deals",
     ("gpu-new", "gpu-used"), update_dataset("gpu")),
    ("ram", "HardwareDeals.co open RAM dataset (GitHub)", "https://github.com/HardwareDealsCo/ram-deals",
     ("ram-new",), update_dataset("ram")),
]


def build_output(store, interval_hours):
    series = []
    def rank(item):
        name = item[1]["name"].upper()
        order = RAM_RANK if item[1]["group"].startswith("ram") else GPU_RANK
        return (item[1]["group"], order.index(name) if name in order else 999, item[0])

    for sid, s in sorted(store["series"].items(), key=rank):
        avg = s["points"].get("avg", {})
        if s["group"].startswith("gpu-") and s["name"].upper() not in GPU_RANK:
            continue
        if s["group"].startswith("ram") and s["name"].upper() not in RAM_RANK:
            continue
        ebay = sid.startswith("ebay:")
        if ebay and len(avg) < MIN_GPU_DAYS:
            continue
        if not avg:
            continue
        days = sorted(int(k) for k in avg)
        cols = {m: [s["points"].get(m, {}).get(str(d)) for d in days] for m in ("avg", "min", "max")}
        if ebay:
            cols = {m: smooth(days, v) for m, v in cols.items()}
        series.append(dict({"id": sid, "group": s["group"], "name": s["name"], "days": days}, **cols))
    # The retail source has no series for some kit sizes; fill those from eBay listings, clearly labelled.
    have = {x["name"] for x in series if x["group"] == "ram"}
    for x in [x for x in series if x["group"] == "ram-new" and x["name"] not in have]:
        series.append(dict(x, id=x["id"] + "@retail", group="ram", name=x["name"] + " (eBay)"))
    series.sort(key=lambda x: (x["group"], RAM_RANK.index(x["name"].replace(" (eBay)", ""))
                               if x["group"].startswith("ram") else 0))
    sources = []
    for sid, name, url, groups, _ in SOURCES:
        info = dict(store["sources"].get(sid, {}))
        last = [x["days"][-1] for x in series if x["group"] in groups]
        info.update({"id": sid, "name": name, "url": url, "latestDay": max(last) if last else None})
        sources.append(info)
    out = {"generatedAt": now_iso(), "intervalHours": interval_hours,
           "groups": GROUPS, "sources": sources, "series": series}
    save_json(OUT_PATH, out, prefix="window.PRICE_DATA=")
    return len(series)


def run_update(interval_hours=DEFAULT_INTERVAL_HOURS):
    with _lock:
        if _status["running"]:
            return False
        _status["running"] = True
    try:
        os.makedirs(DATA, exist_ok=True)
        store = load_store()
        log("Checking prices...")
        for sid, name, _url, _groups, fn in SOURCES:
            info = {"checkedAt": now_iso()}
            try:
                info["message"] = fn(store)
                info["ok"] = True
                info["lastSuccess"] = info["checkedAt"]
            except Exception as e:
                prev = store["sources"].get(sid, {})
                info["ok"] = False
                info["message"] = "Could not reach source: %s" % e
                info["lastSuccess"] = prev.get("lastSuccess")
            log("  %s: %s" % (name, info["message"]))
            store["sources"][sid] = info
            save_store(store)
        n = build_output(store, interval_hours)
        log("Done: %d price series written." % n)
        _status["lastCheck"] = now_iso()
        return True
    finally:
        _status["running"] = False


def scheduler(interval_hours):
    while True:
        try:
            run_update(interval_hours)
        except Exception as e:
            log("Update failed: %s" % e)
        nxt = time.time() + interval_hours * 3600
        _status["nextCheck"] = dt.datetime.fromtimestamp(nxt, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        time.sleep(max(60, nxt - time.time()))


class Handler(BaseHTTPRequestHandler):
    FILES = {"/": ("index.html", "text/html; charset=utf-8"),
             "/index.html": ("index.html", "text/html; charset=utf-8"),
             "/data/prices.js": (os.path.join("data", "prices.js"), "application/javascript; charset=utf-8")}

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/status":
            return self._send(200, "application/json", json.dumps(_status).encode())
        if path in self.FILES:
            name, ctype = self.FILES[path]
            try:
                with open(os.path.join(ROOT, name), "rb") as f:
                    return self._send(200, ctype, f.read())
            except OSError:
                if name.endswith("prices.js"):
                    return self._send(200, ctype, b"window.PRICE_DATA=null")
        self._send(404, "text/plain", b"Not found")

    def do_POST(self):
        if self.path == "/api/update":
            interval = self.server.interval_hours
            threading.Thread(target=run_update, args=(interval,), daemon=True).start()
            return self._send(202, "application/json", b'{"started":true}')
        self._send(404, "text/plain", b"Not found")

    def log_message(self, *args):
        pass


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "serve"
    interval = DEFAULT_INTERVAL_HOURS
    port = int(os.environ.get("PORT", PORT))
    for a in argv[2:]:
        if a.startswith("--interval-hours="):
            interval = float(a.split("=", 1)[1])
        elif a.startswith("--port="):
            port = int(a.split("=", 1)[1])
    if cmd == "update":
        run_update(interval)
    elif cmd == "serve":
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        server.interval_hours = interval
        threading.Thread(target=scheduler, args=(interval,), daemon=True).start()
        log("Dashboard: http://127.0.0.1:%d  (re-checking every %g hours, Ctrl+C to stop)" % (port, interval))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv)
