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
# ORDERING PRINCIPLE: bottom-to-top specificity.
# The MOST specific/niche categories (rare specialized skills) come FIRST so
# they grab roles like "Software Engineer, On-Device ML" before generic SWE.
# Generic catch-all categories (SWE, PM) come LAST.
CATEGORIES: List[Tuple[str, List[str]]] = [
    # ---- Tier 1: Hyper-specialized / niche skills (matched first) ----
    ("ML / AI / Generative AI", [
        # Hyper-specific ML signals (must win against generic "Software Engineer")
        r"\bon-device ml\b", r"\bon[- ]device\b.*\bml\b",
        r"\bmachine learning\b", r"\bdeep learning\b",
        r"\bneural\b", r"\bllm\b", r"\bfoundation model",
        r"\bgenerative ai\b", r"\bgenai\b", r"\bgenerative ui\b",
        r"\btransformer\b", r"\bnlp\b", r"\bnatural language\b",
        r"\bcomputer vision\b", r"\bvision\b", r"\bperception\b",
        r"\bdata scientist\b", r"\bdata science\b",
        r"\bapplied ml\b", r"\bapplied machine learning\b", r"\bapplied ai\b",
        r"\brobot ml\b", r"\brobotics\b",
        r"\bgpu ml\b", r"\bcoreml\b", r"\bmlx\b", r"\bmetal\b",
        r"\breinforcement learning\b",
        # ML role prefixes
        r"\bml\b\s*(engineer|researcher|scientist|engineer|manager|analyst)",
        r"\bai/ml\b", r"\baiml\b", r"\bartificial intelligence\b",
        r"\bai engineer", r"\bai platform", r"\bai data platform",
        r"\bresponsible ai\b",
        # Apple-specific ML teams/products
        r"\bsiri\b", r"\bapple intelligence\b",
        r"\bads (predictions|matching|signals|campaign)", r"\bad predictions\b",
        # Apple-specific ML data platforms (AiDP team)
        r"\bai\s*&\s*data platforms\b", r"\baidp\b",
        r"\bai\s+data\s+platforms\b",
        # ML frameworks / tools
        r"\btensorflow\b", r"\bpytorch\b", r"\bjax\b", r"\bllama\b",
        r"\bmodel integration\b", r"\bmodel inference\b", r"\bmodel optimization\b",
        # Looser fallbacks — apply AFTER specific ones (deliberately no bare \bml\b —
        # too many false positives like "HTML", "yml", "Gmail")
        r"\bmachine learning\b",
    ]),
    ("Apple Silicon / Hardware Engineering", [
        # Highly specific silicon signals
        r"\bcpu\b", r"\bgpu\b", r"\brtl\b", r"\basic\b", r"\bcircuit design",
        r"\bstandard cell\b", r"\bsilicon\b", r"\bchip\b", r"\bmicroarchitect",
        r"\bgate level\b", r"\bdesign verification\b", r"\bemulation verification",
        r"\banalog/mixed-signal\b", r"\bmixed signal\b", r"\bsoc\b",
        r"\bpre-silicon\b", r"\bsilicon debug\b", r"\bsilicon photonics\b",
        r"\bdisplay silicon\b", r"\bdisplay panel\b", r"\bdisplay module\b",
        r"\bdisplay electrical\b", r"\btft\b",
        r"\bbattery management\b", r"\bbattery algorithm\b",
        r"\bmanufacturing design\b", r"\btooling engineer\b",
        r"\bprototyping systems\b", r"\brf system\b",
        r"\bwireless system\b", r"\bwireless (tools|bluetooth|module|qa)",
        r"\bsignal and power integrity\b", r"\bpower integrity\b",
        r"\bthermal engineer\b", r"\bthermal design\b",
        r"\bacoustic engineer\b", r"\bmicrophone module\b",
        r"\bpanel design\b", r"\bproduct design engineer\b",
        r"\bmechanical systems\b", r"\bmodeling and simulation\b",
        r"\bcontrols critical\b", r"\bdata center mechanical\b",
        r"\blab systems\b", r"\bsystems debug\b",
        r"\btest & instrumentation\b", r"\bfpga\b",
        r"\bcamera mechanical\b", r"\bcamera simulation\b", r"\bcamera imaging\b",
        r"\bhardware engineering program\b", r"\boptical sensing\b",
        r"\bhardware system", r"\bhardware engineer\b",
        # Fallback: any "engineering program manager" tied to hardware
        r"\bhardware (program|engineering program)\b",
    ]),
    ("Computer Vision / AR / VR / Design", [
        r"\b3d computer vision\b", r"\breal-time computer vision\b",
        r"\bsenior computer vision\b",
        r"\bux designer\b", r"\bui designer\b", r"\binteraction designer\b",
        r"\bdesigner, interactive\b", r"\bart director\b", r"\bmotion design\b",
        r"\bacd editorial\b", r"\blead designer, design systems\b",
        r"\bvisual communication designer\b", r"\bcreative director\b",
    ]),
    ("Security / Cryptography", [
        r"\bred team\b", r"\bcryptography\b", r"\bsecurity adoption\b",
        r"\bcloud security\b", r"\bsecurity software\b",
        r"\bprincipal security\b", r"\bsecurity\b", r"\bsecurity engineer",
    ]),
    ("Infrastructure / SRE / Platform", [
        r"\bsite reliability\b", r"\bsre\b", r"\binfrastructure engineer\b",
        r"\bnetwork reliability\b", r"\brelease engineer\b",
        r"\bobservability\b", r"\bprovisioning\b", r"\bplatform engineer\b",
        r"\bdevops\b", r"\bkubernetes\b",
    ]),
    ("QA / Test / SDET", [
        r"\bquality engineer\b", r"\bqa engineer\b", r"\btest engineer\b",
        r"\btest and validation\b", r"\bscreening & integration\b",
        r"\bsdet\b", r"\bsoftware development engineer in test\b",
        r"\bquality systems\b", r"\bscreening\b",
    ]),
    ("Research / Applied Science", [
        r"\bresearch scientist\b", r"\bresearch engineer\b",
        r"\bresearch operations\b", r"\bapplied sensing\b",
        r"\bhuman factors\b",
    ]),
    ("Data Engineering / Analytics", [
        r"\bdata engineer\b", r"\bdata analyst\b", r"\bdata solutions\b",
        r"\bdata lakehouse\b", r"\bdata operations\b", r"\bdata qa\b",
        r"\bbusiness operations analyst\b", r"\bmarket analyst\b",
        r"\bconsumer insights\b", r"\bcompetitive intelligence\b",
        r"\bpricing analyst\b", r"\bfinancial analyst\b",
    ]),
    # ---- Tier 2: Mid-level functional categories ----
    ("Product / Program Management (Technical)", [
        r"\bproduct manager\b", r"\bprogram manager\b",
        r"\bengineering program manager\b", r"\bengineering project manager\b",
        r"\bnpi operations\b", r"\bnew product operations\b",
        r"\btechnical project manager\b", r"\bproject manager\b",
    ]),
    # ---- Tier 3: Generic catch-alls (matched last) ----
    ("Software Engineering (iOS/macOS/Services/Core OS)", [
        r"\bsoftware engineer\b", r"\bsoftware development engineer\b",
        r"\bbackend\b", r"\bfront end\b", r"\bfull stack\b", r"\bfull-stack\b",
        r"\bios engineer\b", r"\bswift engineer\b", r"\bdarwin\b", r"\bcoreos\b",
        r"\bkernel\b", r"\bembedded software\b", r"\bfirmware\b",
        r"\bapplication & system\b", r"\bcompiler\b", r"\bsafari\b",
        r"\bscreen sharing\b", r"\bsatellite operations\b",
        r"\bfoundationdb\b", r"\bagentic os\b",
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

# Numeric score for ranking "highest paid" roles. Higher = more senior = more $$.
# New Grad / Intern are excluded from the highest-paid digest by being <0.
SENIORITY_SCORE: Dict[str, int] = {
    "Director":  7,
    "Principal": 6,
    "Staff":     5,
    "Senior":    4,
    "Lead":      3,
    "Manager":   2,
    "Mid":       1,
    "New Grad":  0,
    "Intern":   -1,
    "Unknown":   1,  # treat as mid for ranking purposes
}

# Salary bands by seniority level (USD base) — used in the highest-paid section
SENIORITY_SALARY_BAND: Dict[str, str] = {
    "Director":  "$250K – $500K+",
    "Principal": "$200K – $400K+",
    "Staff":     "$200K – $380K",
    "Senior":    "$170K – $280K",
    "Lead":      "$180K – $310K",
    "Manager":   "$180K – $320K",
    "Mid":       "$140K – $220K",
    "New Grad":  "$120K – $180K",
    "Intern":    "$50 – $90/hr",
    "Unknown":   "$140K – $260K (est.)",
}

# ============ SKILLS TAXONOMY ============
# Curated list of tech skills. We count occurrences of each in the full
# job description (not the title), ranked by frequency within each category.
# Order matters for display only; counts determine ranking.
SKILLS_TAXONOMY: List[Tuple[str, str]] = [
    # Languages
    ("Python", r"\bpython\b"),
    ("Swift", r"\bswift\b"),
    ("C++", r"\bc\+\+\b"),
    ("C", r"\b(?<!visual )\bc language\b|\bansi c\b|(?<!\.)\bC\b(?=[\s,;.])"),
    ("Objective-C", r"\bobjective[- ]c\b"),
    ("Java", r"\bjava\b(?!script)"),
    ("JavaScript/TypeScript", r"\b(javascript|typescript|ts)\b"),
    ("Rust", r"\brust\b"),
    ("Go", r"\b(go(?:lang)?)\b"),
    ("SQL", r"\bsql\b"),
    ("Shell/Bash", r"\b(bash|shell|sh|zsh)\b"),
    ("Verilog/SystemVerilog", r"\b(verilog|systemverilog)\b"),
    ("VHDL", r"\bvhdl\b"),
    ("Matlab", r"\bmatlab\b"),
    # ML / AI frameworks
    ("PyTorch", r"\bpytorch\b"),
    ("TensorFlow", r"\btensorflow\b"),
    ("JAX", r"\bjax\b"),
    ("Core ML / MLX", r"\b(coreml|mlx)\b"),
    ("LLMs / Foundation Models", r"\b(llm|llms|foundation model)\b"),
    ("CUDA", r"\bcuda\b"),
    ("Metal", r"\bmetal\b"),
    # Hardware / silicon
    ("RTL Design", r"\brtl\b"),
    ("ASIC Design", r"\basic\b"),
    ("FPGA", r"\bfpga\b"),
    ("CPU Architecture", r"\bcpu\b"),
    ("GPU Architecture", r"\bgpu\b"),
    ("SoC", r"\bsoc\b"),
    ("Signal Integrity", r"\bsignal integrity\b"),
    ("Power Integrity", r"\bpower integrity\b"),
    ("RF / Wireless", r"\b(rf|wireless)\b"),
    # Mobile / platforms
    ("iOS / iPadOS", r"\b(iOS|iPadOS|ios)\b"),
    ("macOS", r"\bmacOS\b"),
    ("visionOS", r"\bvisionOS\b"),
    ("watchOS", r"\bwatchOS\b"),
    ("Android", r"\bandroid\b"),
    # Web / cloud
    ("React", r"\breact\b"),
    ("Kubernetes", r"\b(kubernetes|k8s)\b"),
    ("Docker / Containers", r"\b(docker|containers?|containerd)\b"),
    ("AWS", r"\baws\b"),
    ("GCP", r"\bgcp\b"),
    ("Azure", r"\bazure\b"),
    # Data / DB
    ("PostgreSQL", r"\bpostgres(?:ql)?\b"),
    ("MongoDB", r"\bmongodb\b"),
    ("Redis", r"\bredis\b"),
    ("Kafka", r"\bkafka\b"),
    ("Spark", r"\bspark\b"),
    ("Snowflake", r"\bsnowflake\b"),
    # Apple-specific platforms
    ("Xcode", r"\bxcode\b"),
    ("SwiftUI", r"\bswiftui\b"),
    ("UIKit", r"\buikit\b"),
    ("ARKit", r"\barkit\b"),
    ("RealityKit", r"\brealitykit\b"),
    ("CoreData", r"\bcoredata\b"),
    ("Combine", r"\bcombine\b"),
    ("FoundationDB", r"\bfoundationdb\b"),
    ("MapKit", r"\bmapkit\b"),
    ("CoreLocation", r"\bcorelocation\b"),
    ("WebKit", r"\bwebkit\b"),
    ("iCloud", r"\bicloud\b"),
    ("CloudKit", r"\bcloudkit\b"),
    ("Core ML", r"\bcoreml\b"),
    ("Metal", r"\bmetal\b"),
    # Apple-specific ML platforms / teams
    ("AI & Data Platforms (AiDP)", r"\bai\s*&\s*data platforms\b|\baidp\b|\bai\s+data\s+platforms\b"),
    ("ML Infrastructure", r"\bml infrastructur"),
    ("Foundation Models / LLM Inference", r"\bfoundation model|\bllm\b.*\binference\b|\bmodel inference\b"),
    ("Search & Knowledge Platforms", r"\bsearch\s*&\s*knowledge\b"),
    # Methods / practices
    ("Computer Vision", r"\bcomputer vision\b"),
    ("NLP", r"\b(nlp|natural language)\b"),
    ("Speech / Audio", r"\b(speech|audio)\b"),
    ("3D Graphics / Rendering", r"\b(3d graphics|rendering|metal)\b"),
    ("Computer Graphics", r"\bcomputer graphics\b"),
    ("Cryptography", r"\bcryptograph"),
    ("Distributed Systems", r"\b(distributed systems?|distributed computing)\b"),
    ("Microservices", r"\bmicroservices?\b"),
    ("MLOps", r"\bmlops\b"),
    ("SystemVerilog/UVM", r"\b(systemverilog|uvm)\b"),
    ("Observability", r"\bobservability\b"),
    # Soft
    ("Agile / Scrum", r"\b(agile|scrum)\b"),
]

# Pre-compile skill regexes (case-insensitive)
SKILLS_COMPILED = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in SKILLS_TAXONOMY]

# How many detail pages to fetch concurrently for skill extraction.
SKILL_FETCH_WORKERS = 16
# Cap on number of jobs to fetch details for (top roles per top categories only)
# Setting to None fetches all. Set to e.g. 100 to limit.
MAX_DETAIL_FETCHES = None

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
    description: str = ""           # raw job description text (HTML stripped)
    skills: List[str] = field(default_factory=list)  # extracted skill names

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
    top_titles: List[Tuple[str, int]] = field(default_factory=list)  # (title, count)
    seniority_breakdown: Dict[str, int] = field(default_factory=dict)
    top_skills: List[Tuple[str, int]] = field(default_factory=list)  # (skill, count)
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

# ============ JOB DESCRIPTION FETCHING ============

async def fetch_job_description(session: aiohttp.ClientSession, job: JobListing) -> JobListing:
    """Fetch the detail page for a single job and extract its description text.

    Apple's job detail page embeds the actual job description inside an
    escaped JSON blob (key "jobSummary"). We extract that via regex, unescape
    it, then count skills by matching against SKILLS_COMPILED regexes.

    Why not parse the rendered HTML? Apple's site wraps every page with a huge
    nav/footer/chrome that mentions "JavaScript" boilerplate. Pulling text from
    the rendered DOM catches that chrome and gives bogus skill counts.
    """
    url = f"https://jobs.apple.com/en-us/details/{job.role_number}/{job._slug(job.title)}?team={job.team}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                log.debug("Detail fetch failed for %s: HTTP %s", job.role_number, resp.status)
                return job
            html = await resp.text()
    except Exception as e:
        log.debug("Detail fetch error for %s: %s", job.role_number, e)
        return job

    # Extract the escaped JSON jobSummary field. The HTML contains JSON where
    # the inner string has been backslash-escaped for HTML embedding, so:
    #   \"jobSummary\":\"<escaped description>\"
    # where <escaped description> itself has \" for quotes and \\ for backslashes.
    match = re.search(
        r'\\"jobSummary\\"\s*:\s*\\"((?:[^"\\]|\\.)*?)\\"(?:\s*[,}])',
        html,
    )
    if not match:
        log.debug("No jobSummary found for %s", job.role_number)
        return job

    raw = match.group(1)
    # Unescape the doubly-escaped string:
    # 1) \" -> "        (HTML-escaped quote → real quote)
    # 2) \\ -> \        (escaped backslash → real backslash)
    # 3) \n, \t, \uXXXX → real chars
    text = raw.replace('\\"', '"').replace('\\\\', '\\')
    try:
        # Use JSON decoder for any remaining \uXXXX / \n etc.
        text = __import__("json").loads(f'"{text}"')
    except Exception:
        # Fallback: do manual replacements if JSON decode fails
        text = text.replace('\\n', '\n').replace('\\t', '\t').replace('\\u0026', '&')

    text = re.sub(r"\s+", " ", text).strip()
    job.description = text[:8000]  # cap to avoid memory bloat

    # Extract skills from description
    skills_found: List[str] = []
    for name, regex in SKILLS_COMPILED:
        if regex.search(job.description):
            skills_found.append(name)

    # Fallback: also match skills against the title. Apple's jobSummaries
    # can be vague (e.g. "iOS Software Engineer" descriptions rarely mention
    # "iOS" or "Swift" explicitly). The title is a reliable signal.
    for name, regex in SKILLS_COMPILED:
        if name in skills_found:
            continue
        if regex.search(job.title):
            skills_found.append(name)

    job.skills = skills_found
    return job

async def fetch_all_descriptions(jobs: List[JobListing]) -> None:
    """Fetch descriptions for a list of jobs in parallel, populating skills.

    Mutates each JobListing in-place with description + skills fields.
    """
    if MAX_DETAIL_FETCHES is not None:
        jobs_to_fetch = jobs[:MAX_DETAIL_FETCHES]
        log.info("Fetching descriptions for %d jobs (cap: %d)...",
                 len(jobs_to_fetch), MAX_DETAIL_FETCHES)
    else:
        jobs_to_fetch = jobs
        log.info("Fetching descriptions for %d jobs...", len(jobs_to_fetch))

    sem = asyncio.Semaphore(SKILL_FETCH_WORKERS)

    async def bounded(session, j):
        async with sem:
            return await fetch_job_description(session, j)

    async with aiohttp.ClientSession(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
                          "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
    ) as session:
        await asyncio.gather(*[bounded(session, j) for j in jobs_to_fetch])

    fetched = sum(1 for j in jobs_to_fetch if j.description)
    log.info("Fetched %d/%d descriptions successfully.", fetched, len(jobs_to_fetch))

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

    # Group by category
    by_cat: Dict[str, List[JobListing]] = {}
    for j in tech_jobs:
        j.category = categorize(j)
        j.seniority = detect_seniority(j)
        by_cat.setdefault(j.category, []).append(j)

    rollups: List[CategoryRollup] = []
    for cat, cat_jobs in by_cat.items():
        unique_role_ids = {j.role_number.split("-")[0] for j in cat_jobs}
        titles = Counter(j.title for j in cat_jobs)
        seniority_breakdown = Counter(j.seniority for j in cat_jobs)

        # Aggregate skills across all jobs in this category
        skill_counter: Counter = Counter()
        for j in cat_jobs:
            for s in j.skills:
                skill_counter[s] += 1
        # Show top skills (only those that appear in at least 2 jobs to filter noise)
        top_skills = [
            (name, count) for name, count in skill_counter.most_common()
            if count >= 2
        ][:10]

        rollups.append(CategoryRollup(
            name=cat,
            role_count=len(unique_role_ids),
            posting_count=len(cat_jobs),
            top_titles=[(t, c) for t, c in titles.most_common(8)],
            seniority_breakdown=dict(seniority_breakdown),
            top_skills=top_skills,
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

# ============ HIGHEST-PAID ROLES ============

# How many top highest-paid roles to feature in the digest
TOP_PAID_ROLES_IN_DIGEST = 5
# Minimum seniority score to be eligible (excludes Mid / New Grad / Intern)
TOP_PAID_MIN_SCORE = 4  # Senior and above

@dataclass
class PaidRole:
    title: str
    category: str
    seniority: str           # the highest seniority level seen for this role
    salary_band: str
    posting_count: int       # how many postings across locations/teams
    top_skills: List[Tuple[str, int]] = field(default_factory=list)
    sample_locations: List[str] = field(default_factory=list)
    seniority_score: int = 0

def top_paid_roles(jobs: List[JobListing]) -> List[PaidRole]:
    """Identify the highest-paid open roles by their highest seniority level.

    Strategy:
    - Bucket jobs by (normalized title, category). Two postings with the same
      title in different locations count as one role with N postings.
    - For each role, the highest seniority wins (e.g. if there's a Senior and a
      Mid posting for the same title, the role is ranked as Senior).
    - Rank by seniority score (desc), then by posting count (desc).
    - Filter out Mid / New Grad / Intern — we want the high end of the band.
    - Filter out non-tech roles (Art Director, Creative Director, etc.) — this
      digest is for tech hiring.
    """
    # Roles in non-tech categories won't have extractable tech skills and aren't
    # useful in this digest. Build a set of "tech category" names dynamically.
    # We also explicitly exclude design roles (Art Director, Creative Director)
    # even though they're in a tech-adjacent category — they don't have
    # extractable tech skills and aren't useful here.
    DESIGN_CATEGORY = "Computer Vision / AR / VR / Design"
    tech_categories = {name for name, _ in CATEGORIES if name != DESIGN_CATEGORY}

    # Normalize title for bucketing. We collapse common senior prefixes
    # so "Senior SWE, Foo" and "Sr. SWE, Foo" bucket together.
    def norm(t: str) -> str:
        s = re.sub(r"\s+", " ", t).strip().lower()
        # Collapse senior prefixes
        s = re.sub(r"^(senior|sr\.?|staff|principal|lead|head of|director|manager|mid|junior|intern)\s+", "", s)
        return s

    buckets: Dict[Tuple[str, str], List[JobListing]] = {}
    for j in jobs:
        if j.category not in tech_categories:
            continue
        key = (norm(j.title), j.category)
        buckets.setdefault(key, []).append(j)

    paid: List[PaidRole] = []
    for (title_l, category), role_jobs in buckets.items():
        # Pick the highest seniority seen across postings
        seniority_counts = Counter(j.seniority for j in role_jobs)
        # Order by score (highest first)
        ordered = sorted(
            seniority_counts.items(),
            key=lambda kv: (-SENIORITY_SCORE.get(kv[0], 1), -kv[1]),
        )
        top_seniority = ordered[0][0]
        score = SENIORITY_SCORE.get(top_seniority, 1)
        if score < TOP_PAID_MIN_SCORE:
            continue

        # Aggregate skills for this role across its postings
        skill_counter: Counter = Counter()
        for j in role_jobs:
            for s in j.skills:
                skill_counter[s] += 1

        # Sample top 3 locations
        locations = Counter(j.location for j in role_jobs if j.location and j.location != "Various")
        sample_locations = [loc for loc, _ in locations.most_common(3)]

        # Display title (use the first job's title case)
        display_title = role_jobs[0].title

        paid.append(PaidRole(
            title=display_title,
            category=category,
            seniority=top_seniority,
            salary_band=SENIORITY_SALARY_BAND.get(top_seniority, SENIORITY_SALARY_BAND["Unknown"]),
            posting_count=len(role_jobs),
            top_skills=[(s, c) for s, c in skill_counter.most_common(5)],
            sample_locations=sample_locations,
            seniority_score=score,
        ))

    # Sort: seniority score desc, then posting count desc, then title asc
    paid.sort(key=lambda r: (-r.seniority_score, -r.posting_count, r.title))
    return paid[:TOP_PAID_ROLES_IN_DIGEST]

# ============ FORMAT ============

# How many top categories + titles to show in the Telegram digest
TOP_CATEGORIES_IN_DIGEST = 2
TOP_TITLES_IN_DIGEST = 3
TOP_SKILLS_IN_DIGEST = 6

def format_telegram(
    jobs: List[JobListing],
    rollups: List[CategoryRollup],
    paid_roles: List[PaidRole],
    history: List[dict],
) -> str:
    today = datetime.now(timezone.utc).strftime("%b %d, %Y")
    tech_total = sum(1 for j in jobs if is_tech(j))
    total_listings = len(jobs)
    delta, delta_str = compute_delta(tech_total, history)

    # Header
    lines: List[str] = []
    lines.append(f"🍎 <b>Apple US Tech Jobs Digest</b> — {today}")
    lines.append(f"<i>{tech_total} unique tech roles open across {total_listings} total postings</i>{delta_str}")
    lines.append("")

    top_categories = rollups[:TOP_CATEGORIES_IN_DIGEST]
    for i, rollup in enumerate(top_categories, 1):
        band, level = pick_salary_band(rollup.seniority_breakdown)
        lines.append(f"<b>#{i} {rollup.name}</b>")
        lines.append(f"  • <b>{rollup.role_count}</b> unique roles  •  <b>{rollup.posting_count}</b> postings")
        lines.append(f"  • Most common seniority: <b>{level}</b>")
        lines.append(f"  • 💰 Salary band: <b>{band}</b>")

        # Top roles (top N, with posting count in parens)
        lines.append(f"  • <b>Top roles:</b>")
        for t, count in rollup.top_titles[:TOP_TITLES_IN_DIGEST]:
            t_disp = (
                t.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
            )
            if len(t_disp) > MAX_TITLE_LEN:
                t_disp = t_disp[: MAX_TITLE_LEN - 1] + "…"
            suffix = f" <i>(×{count})</i>" if count > 1 else ""
            lines.append(f"      – {t_disp}{suffix}")

        # Top skills
        if rollup.top_skills:
            skill_strs = [f"{name} <i>({count})</i>" for name, count in rollup.top_skills[:TOP_SKILLS_IN_DIGEST]]
            lines.append(f"  • <b>Top skills:</b> {' · '.join(skill_strs)}")

        lines.append("")

    # Highest-Paid Roles section (dynamic — driven by current open postings)
    if paid_roles:
        lines.append("<b>💎 Highest-Paid Open Roles</b>")
        lines.append(f"<i>Senior / Staff / Principal / Director tier, ranked by seniority × demand</i>")
        lines.append("")
        for i, role in enumerate(paid_roles, 1):
            # Title escape + truncation
            t_disp = (
                role.title.replace("&", "&amp;")
                           .replace("<", "&lt;")
                           .replace(">", "&gt;")
            )
            if len(t_disp) > MAX_TITLE_LEN:
                t_disp = t_disp[: MAX_TITLE_LEN - 1] + "…"
            # Seniority badge
            level_badge = f"<b>{role.seniority}</b>"
            # Locations
            loc_str = ""
            if role.sample_locations:
                loc_str = " · " + ", ".join(role.sample_locations)
            lines.append(f"<b>{i}. {t_disp}</b> <i>({level_badge})</i>")
            lines.append(f"   💰 {role.salary_band}  ·  {role.posting_count} posting{'s' if role.posting_count != 1 else ''}{loc_str}")
            if role.top_skills:
                skill_strs = [f"{n} <i>({c})</i>" for n, c in role.top_skills]
                lines.append(f"   🛠 <b>Skills:</b> {' · '.join(skill_strs)}")
            else:
                lines.append(f"   🛠 <i>Skills: not yet extracted</i>")
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

    # Fetch job descriptions for skill extraction. We fetch only the unique
    # tech-role base role IDs to keep request count reasonable.
    tech_jobs = [j for j in jobs if is_tech(j)]
    unique_jobs_by_id = {}
    for j in tech_jobs:
        base_id = j.role_number.split("-")[0]
        # Keep the first occurrence (any will do for skill extraction)
        unique_jobs_by_id.setdefault(base_id, j)
    jobs_for_skills = list(unique_jobs_by_id.values())
    log.info("Fetching descriptions for %d unique tech roles...", len(jobs_for_skills))
    await fetch_all_descriptions(jobs_for_skills)

    # Now propagate skills from the dedup'd jobs back to all postings of the same role
    skills_lookup = {j.role_number.split("-")[0]: j.skills for j in jobs_for_skills}
    for j in jobs:
        j.skills = skills_lookup.get(j.role_number.split("-")[0], [])

    rollups = analyze(jobs)
    paid = top_paid_roles(jobs)
    history = load_history()

    # Append current run
    history.append({
        "date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tech_role_count": sum(1 for j in jobs if is_tech(j)),
        "top_categories": [r.name for r in rollups[:3]],
        "top_category_counts": [r.role_count for r in rollups[:3]],
        "top_paid_roles": [
            {"title": r.title, "category": r.category, "seniority": r.seniority,
             "salary_band": r.salary_band, "posting_count": r.posting_count}
            for r in paid
        ],
    })
    save_history(history)

    message = format_telegram(jobs, rollups, paid, history)
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
                "top_titles": [{"title": t, "count": c} for t, c in r.top_titles],
                "top_skills": [{"skill": s, "count": c} for s, c in r.top_skills],
                "seniority_breakdown": r.seniority_breakdown,
            }
            for r in rollups[:5]
        ],
        "highest_paid_roles": [
            {
                "title": r.title,
                "category": r.category,
                "seniority": r.seniority,
                "salary_band": r.salary_band,
                "posting_count": r.posting_count,
                "top_skills": [{"skill": s, "count": c} for s, c in r.top_skills],
                "sample_locations": r.sample_locations,
            }
            for r in paid
        ],
    }
    Path("latest_snapshot.json").write_text(json.dumps(snapshot, indent=2))
    return 0

def main() -> int:
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main())
