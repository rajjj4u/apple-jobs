# Apple Jobs Trend Finder — US Tech Demand 🍎

Daily GitHub Actions job that scrapes Apple's US job listings and posts a Telegram
digest with the **top 3 in-demand tech hiring areas**, including actual role
titles and salary bands.

## What it does

1. Scrapes up to 60 pages of [jobs.apple.com/en-us/search?location=united-states-USA](https://jobs.apple.com/en-us/search?location=united-states-USA)
2. Categorizes ~12 tech areas (ML/AI, Apple Silicon, Software Engineering, etc.)
3. Computes the top 3 categories by unique role count
4. Picks a salary band per category based on the most common seniority
5. Sends a Telegram digest with titles + salary + delta vs yesterday
6. Commits `history.json` back to the repo for trend tracking

## Schedule

- **Daily at 9:00 AM UTC** (matches the flight-deals schedule)
- Manual trigger via `workflow_dispatch`

## Setup

### 1. Create the GitHub repo

```bash
gh repo create rajjj4u/apple-jobs --public --source=. --push
```

### 2. Add Telegram secrets

Go to **Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Value |
|--------|-------|
| `TELEGRAM_BOT_TOKEN` | Your Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID`   | Your chat ID (e.g. from `@userinfobot`) |

### 3. Enable Actions

The workflow at `.github/workflows/apple-jobs.yml` will run automatically
on the cron schedule. Check the **Actions** tab to verify.

## Telegram output example

```
🍎 Apple US Tech Jobs Digest — Aug 28, 2026
264 unique tech roles open across 600+ total postings (↑ 12 vs yesterday)

#1 ML / AI / Generative AI
  • 64 unique roles  •  87 postings
  • Most common seniority: Senior
  • 💰 Salary band: $170K – $280K
  • Top roles:
      – Software Engineer in Natural Language Processing (NLP) and Mach…
      – Sr. Machine Learning Research Engineer, Siri Speech
      – Machine Learning Engineer - On-Device Control and Optimization
      – Sr. Machine Learning Engineer, Foundation Models Inference - …
      – AI/ML Engineer (GenAI), Wireless Technologies & Ecosystems

#2 Apple Silicon / Hardware Engineering
  • 58 unique roles  •  81 postings
  • 💰 Salary band: $170K – $280K
  • Top roles: …

#3 Software Engineering (iOS/macOS/Services/Core OS)
  • 61 unique roles  •  92 postings
  • 💰 Salary band: $170K – $280K
  • Top roles: …

Source: jobs.apple.com (US-only, scraped live)
Next digest: tomorrow at 9:00 AM UTC
```

## Files

| File | Description |
|------|-------------|
| `find_apple_jobs.py` | Main scraper + categorizer + Telegram sender |
| `requirements.txt` | Python deps (aiohttp) |
| `.github/workflows/apple-jobs.yml` | GitHub Actions workflow (daily 9 AM UTC) |
| `history.json` | Auto-managed daily snapshot history (last 30 days) |
| `latest_snapshot.json` | Most recent run snapshot |

## Architecture

- **Scraper**: Direct HTTP GET against Apple's SSR-rendered HTML (no auth, no API key)
- **Concurrency**: `aiohttp` with semaphore-bounded parallel page fetches
- **Categorization**: Regex pattern matching against a 12-category taxonomy
- **Salary bands**: Static lookup table by seniority (Junior/Mid/Senior/Staff/Principal/Director)
- **Deduplication**: Role number (`role_id-location_code`) used to count unique roles
- **State**: `history.json` for trend deltas; never gates the run (always fresh)

## Salary bands

These are public market estimates from levels.fyi + Apple pay-transparency postings.
Apple's total comp is base + RSU refreshers + bonus + ESPP.

| Seniority | Base salary band |
|-----------|------------------|
| Director  | $250K – $500K+ |
| Principal | $200K – $400K+ |
| Staff     | $200K – $380K |
| Senior    | $170K – $280K |
| Lead      | $180K – $310K |
| Manager   | $180K – $320K |
| Mid       | $140K – $220K |
| New Grad  | $120K – $180K |
| Intern    | $50 – $90/hr |

## Local testing

```bash
pip install -r requirements.txt
TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy python find_apple_jobs.py
```

The script will print the message to stdout regardless of whether Telegram creds are set.

## Related

- Sibling repo: [rajjj4u/flight-deals](https://github.com/rajjj4u/flight-deals) — daily flight deals
- Apple Jobs source: https://jobs.apple.com/en-us/search?location=united-states-USA
