# PC Part Price Tracker

Tracks RAM, SSD and GPU prices and shows them on an interactive chart.

**Live page:** https://pond333.github.io/pc-part-price-tracker/ (re-checked every 2 hours by GitHub Actions)

## Run it

```bash
python3 tracker.py serve
```

Then open http://127.0.0.1:8765. While it runs, the bot re-checks its sources every 2 hours
(change with `--interval-hours=6`) and the page picks up new data on its own.
`python3 tracker.py update` does a single check and exits. No installs needed: Python 3 only.

## Where the prices come from

| Part | Source | History |
|---|---|---|
| RAM, SSD, RTX 5080 retail | Pangoly price trends, read from Internet Archive snapshots | daily, from 2021-2023 depending on the item |
| GPU new / used | HardwareDeals.co open dataset on GitHub (eBay listings), reduced to a daily median per model | daily, from Sept 2025 |

Retailers such as Newegg, Amazon, Best Buy and PCPartPicker block automated access, so the bot does not
scrape them. New points appear when a source publishes newer prices; the 2-hour check finds them soon after.

Data is kept in `data/store.json`; `data/prices.js` is what the page reads.
