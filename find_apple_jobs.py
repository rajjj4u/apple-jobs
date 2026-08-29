#!/usr/bin/env python3
"""
Apple Jobs Trend Finder - US Tech Hiring Demand
===============================================
Daily script that scrapes Apple's US job listings, identifies the top 3
in-demand tech hiring areas, and sends a Telegram update with:
  - Top 3 hiring categories
  - Actual role titles in each category
  - Salary ranges (band by level: IC2 → ICT5/ICT6)
  - Total tech role count + change vs. previous run
  - Posting date freshness

Strategy:
  1. Scrape all job listing pages from Apple's public job search
     (no auth, no API key needed - just HTTP GET on the SSR HTML)
  2. Extract role titles + location + posting date + role number from each listing
  3. Categorize each role into one of ~12 tech categories
  4. Deduplicate by role number + count open postings per category
  5. Look up salary band by title seniority markers (Jr/Sr/Staff/Principal/etc.)
  6. Send a Telegram message with the digest
  7. Append the daily run to history.json so we can compute deltas

Optimizations:
  - Parallel HTTP fetches with asyncio + aiohttp
  - Single parser pass per page (regex over the SSR HTML)
  - No state-of-the-world (works fresh each day; uses history.json for trend)
"""

import os
import sys
import json
import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

try:
    import aiohttp
except ImportError:
    print("Missing dependency: aiohttp. Run: pip install -r requirements.txt")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("apple_jobs")

# ============ CONFIGURATION ============

BASE_URL = "https://jobs.apple.com/en-us/search"
LOCATION_QUERY = "united-states-USA"
MAX_PAGES = 60          # Apple's US list shows 600+ results ≈ 25 pages; allow headroom
PAGE_WORKERS = 8        # concurrent page fetches
MAX_TITLE_LEN = 70      # truncate long titles in Telegram output

# Telegram credentials from env (set by GitHub Actions)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Job category taxonomy
# Each entry: (canonical name, keywords that map to it)
# Order matters - first match wins, so put more specific categories first
CATEGORIES: List[Tuple[str, List[str]]] = [
    ("ML / AI / Generative AI", [
        r"\bmachine learning\b", r"\bml engineer", r"\bml\b research", r"\bml\b data",
        r"\bml\b model", r"\bml\b platform", r"\bml\b system", r"\bml\b ops",
        r"\bmlops\b", r"\baiml\b", r"\bai/ml\b", r"\bai engineer", r"\bartificial intelligence\b",
        r"\bdeep learning\b", r"\bneural\b", r"\bllm\b", r"\bfoundation model",
        r"\bgenerative ai\b", r"\bgenai\b", r"\btransformer\b", r"\bnlp\b",
        r"\bnatural language\b", r"\bcomputer vision\b", r"\bdata scientist\b",
        r"\bapplied ml\b", r"\brobot ml\b", r"\bgpu ml\b", r"\bon-device\b",
        r"\bsiri\b", r"\bads predictions\b", r"\bads signals\b",
        r"\bads matching\b", r"\bad campaign\b", r"\bresponsible ai\b",
    ]),
    ("Apple Silicon / Hardware Engineering", [
        r"\bcpu\b", r"\bgpu\b", r"\brtl\b", r"\basic\b", r"\bcircuit design",
        r"\bstandard cell\b", r"\bsilicon\b", r"\bchip\b", r"\bmicroarchitect",
        r"\bgate level\b", r"\bdesign verification\b", r"\bemulation verification",
        r"\banalog/mixed-signal\b", r"\bmixed signal\b", r"\bsoc\b",
        r"\bpre-silicon\b", r"\bsilicon debug\b", r"\bsilicon photonics\b",
        r"\bdisplay silicon\b", r"\bdisplay panel\b", r"\bdisplay module\b",
        r"\bdisplay electrical\b", r"\btft\b", r"\bbattery management\b",
        r"\bbattery algorithm\b", r"\bhardware system", r"\bmanufacturing design",
        r"\btooling engineer\b", r"\bprototyping systems\b", r"\brf system\b",
        r"\bwireless system\b", r"\bsignal and power integrity\b",
        r"\bpower integrity\b", r"\bthermal engineer\b", r"\bacoustic engineer",
        r"\bmicrophone module\b", r"\bpanel design\b", r"\bproduct design engineer",
        r"\bmechanical systems\b", r"\bmodeling and simulation\b",
        r"\bcontrols critical\b", r"\bdata center mechanical\b", r"\blab systems\b",
        r"\bsystems debug\b", r"\btest & instrumentation\b", r"\bfpga\b",
        r"\bcamera mechanical\b", r"\bcamera simulation\b", r"\bcamera imaging\b",
        r"\bhardware engineering program\b", r"\boptical sensing\b",
    ]),
    ("Software Engineering (iOS/macOS/Services/Core OS)", [
        r"\bsoftware engineer\b", r"\bsoftware development engineer\b",
        r"\bbackend\b", r"\bfront end\b", r"\bfull stack\b", r"\bfull-stack\b",
        r"\bios engineer\b", r"\bswift engineer\b", r"\bdarwin\b", r"\bcoreos\b",
        r"\bkernel\b", r"\bembedded software\b", r"\bfirmware\b",
        r"\bapplication & system\b", r"\bcompiler\b", r"\bsafari\b",
        r"\bscreen sharing\b", r"\bsatellite operations\b",
        r"\bfoundationdb\b", r"\bgenerative ui\b", r"\bagentic os\b",
    ]),
    ("Product / Program Management (Technical)", [
        r"\bproduct manager\b", r"\bprogram manager\b",
        r"\bengineering program manager\b", r"\bengineering project manager\b",
        r"\bnpi operations\b", r"\bnew product operations\b",
        r"\btechnical project manager\b", r"\bproject manager\b",
    ]),
    ("Computer Vision / AR / VR / Design", [
        r"\b3d computer vision\b", r"\breal-time computer vision\b",
        r"\bsenior computer vision\b", r"\bux designer\b",
        r"\bdesigner, interactive\b", r"\bart director\b", r"\bmotion design\b",
        r"\bacd editorial\b", r"\blead designer, design systems\b",
        r"\bvisual communication designer\b", r"\bcreative director\b",
    ]),
    ("Data Engineering / Analytics", [
        r"\bdata engineer\b", r"\bdata analyst\b", r"\bdata solutions\b",
        r"\bdata lakehouse\b", r"\bdata operations\b", r"\bdata qa\b",
        r"\bbusiness operations analyst\b", r"\bmarket analyst\b",
        r"\bconsumer insights\b", r"\bcompetitive intelligence\b",
        r"\bpricing analyst\b", r"\bfinancial analyst\b",
    ]),
    ("QA / Test / SDET", [
        r"\bquality engineer\b", r"\bqa engineer\b", r"\btest engineer\b",
        r"\btest and validation\b", r"\bscreening & integration\b",
        r"\bsdet\b", r"\bsoftware development engineer in test\b",
        r"\bquality systems\b",
    ]),
    ("Security / Cryptography", [
        r"\bsecurity\b", r"\bred team\b", r"\bcryptography\b",
        r"\bsecurity adoption\b", r"\bcloud security\b",
        r"\bsecurity software\b", r"\bprincipal security\b",
    ]),
    ("Infrastructure / SRE / Platform", [
        r"\bsite reliability\b", r"\bsre\b", r"\binfrastructure engineer\b",
        r"\bnetwork reliability\b", r"\brelease engineer\b",
        r"\bobservability\b", r"\bprovisioning\b", r"\bplatform engineer\b",
    ]),
    ("Research / Applied Science", [
        r"\bresearch scientist\b", r"\bresearch engineer\b",
        r"\bresearch operations\b", r"\bapplied sensing\b",
        r"\bhuman factors\b",
    ]),
]

# Seniority detection
SENIORITY_PATTERNS = [
    (r"\bprincipal\b", "Principal"),
    (r"\bstaff\b", "Staff"),
    (r"\bsenior\b|\bsr\.?\b", "Senior"),
    (r"\blead\b", "Lead"),
    (r"\bmanager\b|\bmgr\b|\bhead of\b", "Manager"),
    (r"\bdirector\b", "Director"),
    (r"\bintern\b|\binternship\b", "Intern"),
    (r"\bearly career\b|\bnew grad\b", "New Grad"),
]

# Salary bands by seniority (USD base, full-time US, public estimates 2025-26)
# Source: levels.fyi + Glassdoor + Apple pay transparency postings
SALARY_BANDS = {
    "Principal": "$200K – $400K+",
    "Staff":     "$200K – $380K",
    "Director":  "$250K – $500K+",
    "Senior":    "$170K – $280K",
    "Lead":      "$180K – $310K",
    "Manager":   "$180K – $320K",
    "Mid":       "$140K – $220K",
    "New Grad":  "$120K – $180K",
    "Intern":    "$50 – $90/hr",
    "Unknown":   "$140K – $260K (est.)",
}

# Seniority precedence for picking a band
SENIORITY_RANK = [
    "Director", "Principal", "Staff",
    "Senior", "Lead", "Manager",
    "Mid", "New Grad", "Intern", "Unknown",
]

# ============ DATA MODELS ============

@dataclass
class JobListing:
    title: str
    role_number: str
    location: str
    posting_date: str
    team: str
    category: str = "Other / Non-Tech"
    seniority: str = "Mid"

    @property
    def url(self) -> str:
        # role_number encodes team as a 4-digit suffix; e.g. 200679352-0836
        # The team code is captured from the URL fragment when fetched
        return f"https://jobs.apple.com/en-us/details/{self.role_number}/{self._slug(self.title)}?team={self.team}"

    @staticmethod
    def _slug(title: str) -> str:
        s = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
        return s[:80]

    def short_title(self, max_len: int = MAX_TITLE_LEN) -> str:
        if len(self.title) <= max_len:
            return self.title
        return self.title[: max_len - 1].rstrip() + "…"

@dataclass
class CategoryRollup:
    name: str
    role_count: int        # unique open roles
    posting_count: int     # total postings incl. same role at multiple locations
    top_titles: List[str] = field(default_factory=list)
    seniority_breakdown: Dict[str, int] = field(default_factory=dict)
    sample_role: Optional[JobListing] = None

# ============ SCRAPER ============

# Role number patterns:
#   "PIPE-114438158"      → retail seasonal roles (URL: /en-us/details/114438158/...)
#   "200680838"           → just the 9-digit role id (URL: /en-us/details/200680838-0836/...)
#   "200680838-0836"      → role id + location code (most common)
ROLE_NUM_RE = r"(?:PIPE-)?\d{9}(?:-\d{4})?"

TITLE_RE = re.compile(
    r'<h3><a class="link-inline t-intro word-wrap-break-word more"\s+'
    r'aria-label="(?P<title>[^"]+?) (?P<role_num>' + ROLE_NUM_RE + r')"\s+'
    r'href="(?P<href>/en-us/details/(?:PIPE-)?\d{9}(?:-\d{4})?/[^"]+\?team=(?P<team>[A-Z]+))"\s+'
    r'data-discover="true">(?P<display>[^<]+)</a></h3>'
)
# location span can use either "search-store-name-container-N" or "search-store-name-N"
LOC_RE = re.compile(
    r'<span id="search-store-name(?:-container)?-\d+">(?P<loc>[^<]+)</span>'
)
DATE_RE = re.compile(
    r'<span class="job-posted-date" id="search-job-posted-date-\d+">(?P<date>[^<]+)</span>'
)
TEAM_RE = re.compile(
    r'<span id="search-[^-]+-\d+" class="team-name mt-0">(?P<team>[^<]+)</span>'
)
# href like "/en-us/details/200680838-0836/staff-media-applications-engineer-...?team=SFTWR"
TEAM_CODE_RE = re.compile(r'\?team=([A-Z]+)')

async def fetch_page(session: aiohttp.ClientSession, page: int) -> str:
    url = f"{BASE_URL}?location={LOCATION_QUERY}&page={page}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Page {page} returned HTTP {resp.status}")
        return await resp.text()

def parse_page(html: str) -> List[JobListing]:
    """Parse a single page of Apple's SSR HTML into JobListing objects.

    Apple's search page uses an accordion layout where each job has:
      <h3><a aria-label="Title 200xxx-yyyy" href="...team=ZZZZ">Title</a></h3>
      <span class="team-name mt-0">Team Name</span>
      <span class="job-posted-date" ...>Date</span>
      <span id="search-store-name-container-N">City</span>

    We use the role number as the unique identifier per role/location combo.
    """
    listings: List[JobListing] = []

    # Find all titles first (the anchors); then walk to the nearest location and date
    title_iter = list(TITLE_RE.finditer(html))
    for idx, m in enumerate(title_iter):
        title = m.group("display").strip()
        title = (
            title.replace("&amp;", "&")
                 .replace("&#x27;", "'")
                 .replace("&#39;", "'")
                 .replace("&quot;", '"')
                 .replace("&lt;", "<")
                 .replace("&gt;", ">")
        )
        # The 9-digit role number is the actual unique role id.
        # The location-suffixed form (e.g. 200680838-0836) tells us the specific location.
        aria_role = m.group("role_num")
        href = m.group("href")
        # Extract the 9-digit role id and the -XXXX location code from the href
        href_match = re.search(r"/en-us/details/(?:PIPE-)?(\d{9})(-(\d{4}))?/", href)
        if href_match:
            base_role_id = href_match.group(1)
            loc_code = href_match.group(3) or ""
            full_role = base_role_id if not loc_code else f"{base_role_id}-{loc_code}"
        else:
            base_role_id = aria_role
            full_role = aria_role
            loc_code = ""
        team_code = m.group("team")

        # Look in a window after this title for team full name + location + date
        start = m.end()
        end = title_iter[idx + 1].start() if idx + 1 < len(title_iter) else start + 4000
        window = html[start:end]

        team_match = TEAM_RE.search(window)
        loc_match = LOC_RE.search(window)
        date_match = DATE_RE.search(window)

        location = loc_match.group("loc").strip() if loc_match else "Various"
        date = date_match.group("date").strip() if date_match else "?"

        listings.append(JobListing(
            title=title,
            role_number=full_role,
            location=location,
            posting_date=date,
            team=team_code,
        ))

    return listings

async def scrape_all_pages() -> List[JobListing]:
    """Fetch pages in parallel and aggregate listings."""
    log.info("Scraping up to %d pages of apple.com/us/search ...", MAX_PAGES)
    sem = asyncio.Semaphore(PAGE_WORKERS)

    async def bounded_fetch(session, page):
        async with sem:
            try:
                return page, await fetch_page(session, page)
            except Exception as e:
                log.warning("Page %d failed: %s", page, e)
                return page, ""

    async with aiohttp.ClientSession(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
                          "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
    ) as session:
        # Fire all page requests at once
        tasks = [bounded_fetch(session, p) for p in range(1, MAX_PAGES + 1)]
        results = await asyncio.gather(*tasks)

    all_jobs: List[JobListing] = []
    for page, html in results:
        if not html:
            continue
        page_jobs = parse_page(html)
        log.info("  page %2d: %d jobs", page, len(page_jobs))
        all_jobs.extend(page_jobs)

    log.info("Total raw listings scraped: %d", len(all_jobs))
    return all_jobs

# ============ CATEGORIZATION ============

def categorize(job: JobListing) -> str:
    title_l = job.title.lower()
    for name, patterns in CATEGORIES:
        for pat in patterns:
            if re.search(pat, title_l):
                return name
    return "Other / Non-Tech"

def detect_seniority(job: JobListing) -> str:
    title = job.title
    for pat, label in SENIORITY_PATTERNS:
        if re.search(pat, title, re.IGNORECASE):
            return label
    return "Mid"

def is_tech(job: JobListing) -> bool:
    return categorize(job) != "Other / Non-Tech"

# ============ ANALYSIS ============

def analyze(jobs: List[JobListing]) -> List[CategoryRollup]:
    tech_jobs = [j for j in jobs if is_tech(j)]

    # Group by (category, role_number_base) - the same role can be open in multiple cities
    by_cat: Dict[str, List[JobListing]] = {}
    for j in tech_jobs:
        j.category = categorize(j)
        j.seniority = detect_seniority(j)
        by_cat.setdefault(j.category, []).append(j)

    rollups: List[CategoryRollup] = []
    for cat, cat_jobs in by_cat.items():
        # unique roles (role_number = role id + location code, so use the base 9-digit role id)
        unique_role_ids = {j.role_number.split("-")[0] for j in cat_jobs}
        titles = Counter(j.title for j in cat_jobs)
        seniority_breakdown = Counter(j.seniority for j in cat_jobs)

        rollups.append(CategoryRollup(
            name=cat,
            role_count=len(unique_role_ids),
            posting_count=len(cat_jobs),
            top_titles=[t for t, _ in titles.most_common(8)],
            seniority_breakdown=dict(seniority_breakdown),
            sample_role=cat_jobs[0],
        ))

    rollups.sort(key=lambda r: (-r.role_count, -r.posting_count))
    return rollups

def pick_salary_band(seniority_breakdown: Dict[str, int]) -> Tuple[str, str]:
    """Pick a salary band string + the seniority level it represents.

    Strategy: the most common seniority in this category drives the band.
    """
    if not seniority_breakdown:
        return SALARY_BANDS["Mid"], "Mid"
    # Order by SENIORITY_RANK to ensure 'Senior' beats 'Mid' when tied
    sorted_by_count = sorted(
        seniority_breakdown.items(),
        key=lambda x: (-x[1], SENIORITY_RANK.index(x[0]) if x[0] in SENIORITY_RANK else 99),
    )
    level = sorted_by_count[0][0]
    return SALARY_BANDS.get(level, SALARY_BANDS["Mid"]), level

# ============ HISTORY ============

HISTORY_FILE = Path("history.json")
MAX_HISTORY_DAYS = 30

def load_history() -> List[dict]:
    if not HISTORY_FILE.exists():
        return []
    try:
        return json.loads(HISTORY_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return []

def save_history(history: List[dict]) -> None:
    # Keep last MAX_HISTORY_DAYS entries only
    trimmed = history[-MAX_HISTORY_DAYS:]
    HISTORY_FILE.write_text(json.dumps(trimmed, indent=2))

def compute_delta(current_total: int, history: List[dict]) -> Tuple[Optional[int], str]:
    """Return (delta from yesterday, arrow string)."""
    if not history:
        return None, ""
    yesterday = history[-1].get("tech_role_count")
    if yesterday is None:
        return None, ""
    delta = current_total - yesterday
    if delta > 0:
        return delta, f" (↑ {delta} vs yesterday)"
    if delta < 0:
        return delta, f" (↓ {abs(delta)} vs yesterday)"
    return 0, " (— flat vs yesterday)"

# ============ TELEGRAM ============

def send_telegram(message: str) -> bool:
    """Send an HTML-formatted Telegram message via the Bot API."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; cannot send.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    req = __import__("urllib.request", fromlist=["Request"]).Request(
        url, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    # 4096 char Telegram limit per message; truncate gracefully if needed
    if len(message) > 4000:
        log.warning("Message length %d > 4000; truncating.", len(message))
        message = message[:3950] + "\n…"
        data = urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = __import__("urllib.request", fromlist=["Request"]).Request(
            url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

    try:
        resp = __import__("urllib.request", fromlist=["urlopen"]).urlopen(req, timeout=15)
        status = resp.getcode()
        if status == 200:
            log.info("Telegram message sent (%d chars).", len(message))
            return True
        log.error("Telegram returned HTTP %s", status)
        return False
    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False

# ============ FORMAT ============

def format_telegram(jobs: List[JobListing], rollups: List[CategoryRollup], history: List[dict]) -> str:
    today = datetime.now(timezone.utc).strftime("%b %d, %Y")
    tech_total = sum(1 for j in jobs if is_tech(j))
    total_listings = len(jobs)
    delta, delta_str = compute_delta(tech_total, history)

    # Header
    lines: List[str] = []
    lines.append(f"🍎 <b>Apple US Tech Jobs Digest</b> — {today}")
    lines.append(f"<i>{tech_total} unique tech roles open across {total_listings} total postings</i>{delta_str}")
    lines.append("")

    top3 = rollups[:3]
    for i, rollup in enumerate(top3, 1):
        band, level = pick_salary_band(rollup.seniority_breakdown)
        lines.append(f"<b>#{i} {rollup.name}</b>")
        lines.append(f"  • <b>{rollup.role_count}</b> unique roles  •  <b>{rollup.posting_count}</b> postings")
        lines.append(f"  • Most common seniority: <b>{level}</b>")
        lines.append(f"  • 💰 Salary band: <b>{band}</b>")

        # Top roles (top 5, with truncation)
        lines.append(f"  • <b>Top roles:</b>")
        for t in rollup.top_titles[:5]:
            # HTML-escape for Telegram, but & in the title should render as &
            t_disp = (
                t.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
            )
            if len(t_disp) > MAX_TITLE_LEN:
                t_disp = t_disp[: MAX_TITLE_LEN - 1] + "…"
            lines.append(f"      – {t_disp}")

        # Seniority breakdown summary
        if rollup.seniority_breakdown:
            breakdown = ", ".join(
                f"{k}: {v}" for k, v in sorted(
                    rollup.seniority_breakdown.items(),
                    key=lambda x: -x[1],
                )[:4]
            )
            lines.append(f"  • <i>Seniority mix: {breakdown}</i>")

        lines.append("")

    # Footer
    lines.append("<i>Source: jobs.apple.com (US-only, scraped live)</i>")
    lines.append("<i>Next digest: tomorrow at 9:00 AM UTC</i>")
    return "\n".join(lines)

# ============ MAIN ============

async def main_async() -> int:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram creds missing; will compute digest but skip delivery.")

    jobs = await scrape_all_pages()
    if not jobs:
        log.error("No jobs scraped; aborting.")
        return 1

    rollups = analyze(jobs)
    history = load_history()

    # Append current run
    history.append({
        "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tech_role_count": sum(1 for j in jobs if is_tech(j)),
        "top_categories": [r.name for r in rollups[:3]],
        "top_category_counts": [r.role_count for r in rollups[:3]],
    })
    save_history(history)

    message = format_telegram(jobs, rollups, history)
    print("=" * 60)
    print(message)
    print("=" * 60)

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_telegram(message)

    # Persist a snapshot for the repo
    snapshot = {
        "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_listings": len(jobs),
        "tech_role_count": sum(1 for j in jobs if is_tech(j)),
        "top_categories": [
            {
                "name": r.name,
                "role_count": r.role_count,
                "posting_count": r.posting_count,
                "top_titles": r.top_titles,
                "seniority_breakdown": r.seniority_breakdown,
            }
            for r in rollups[:5]
        ],
    }
    Path("latest_snapshot.json").write_text(json.dumps(snapshot, indent=2))
    return 0

def main() -> int:
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main())
