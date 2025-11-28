"""
Apartments.com Web Scraper for UMN Housing Research

Usage:
  python3 -m scraper.main --headless=False --max_search_pages=1 --max_buildings=2  # test run
  python3 -m scraper.main --headless=True --max_search_pages=50 --max_buildings=800  # full run
  python3 -m scraper.main --auto_restart --max_sessions=5 --session_cooldown=300  # auto-restart mode
"""
import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import List, Optional, Dict, Any, Set
from urllib.parse import urljoin

from playwright.async_api import async_playwright, Page
import requests


# ============================================================================
# CONFIGURATION
# ============================================================================

UMN_CAMPUS_LAT = 44.9731
UMN_CAMPUS_LON = -93.2359
SEARCH_RADIUS_KM = 10.0  # 10 km radius (updated)

BASE_URL = "https://www.apartments.com"
SEARCH_LOCATION = "Minneapolis, MN"

# ============================================================================
# SEARCH LOCATIONS - VERIFIED WORKING apartments.com URL formats
# ============================================================================
# All distances calculated from UMN campus center (44.9731, -93.2359)
# KEY: Use ONLY formats known to work on apartments.com
#
# WORKING FORMATS on apartments.com:
#   /minneapolis-mn/           - city search (MOST RELIABLE)
#   /minneapolis-mn-55414/     - ZIP code search
#   /1-bedrooms-minneapolis-mn/ - bedroom filter
#   /pet-friendly-apartments/minneapolis-mn/ - category search
#   /condos-for-rent/minneapolis-mn/ - property type
#
# NOT WORKING (cause redirects/errors):
#   /dinkytown-minneapolis-mn/  - neighborhood alone may redirect or 404
#   /prospect-park-minneapolis-mn/ - same issue

# Primary search locations - VERIFIED WORKING URL patterns
# Prioritize the main city search which has all buildings, with pagination
# Then ZIP codes which are KNOWN to work reliably
PRIMARY_SEARCH_LOCATIONS = [
    # Main city search - most comprehensive (go through ALL pages)
    "minneapolis-mn",
    
    # ZIP codes - VERIFIED WORKING format: /minneapolis-mn-55414/
    "minneapolis-mn-55414",  # Dinkytown/Stadium Village (~0.5km from UMN)
    "minneapolis-mn-55455",  # UMN campus (0km)
    "minneapolis-mn-55413",  # Northeast Minneapolis (~3km)
    "minneapolis-mn-55401",  # Downtown Minneapolis (~3.5km)
    "minneapolis-mn-55454",  # West Bank/Cedar-Riverside (~1.5km)
    "minneapolis-mn-55404",  # Phillips/Seward (~3.5km)
    "minneapolis-mn-55403",  # Loring Park/Lowry Hill (~4.5km)
    "minneapolis-mn-55406",  # Longfellow (~5km)
    "minneapolis-mn-55408",  # Uptown/CARAG (~6km - edge of radius)
    "minneapolis-mn-55407",  # Powderhorn (~6km)
    
    # St Paul ZIP codes within 10km
    "saint-paul-mn-55104",   # St Paul Hamline-Midway (~6km)
    "saint-paul-mn-55108",   # St Paul Como (~4km)
    
    # St Paul city search
    "saint-paul-mn",
]

# Bedroom filter searches - VERIFIED WORKING
BEDROOM_FILTER_SEARCHES = [
    "studios-minneapolis-mn",
    "1-bedrooms-minneapolis-mn",
    "2-bedrooms-minneapolis-mn",
    "3-bedrooms-minneapolis-mn",
    "4-bedrooms-minneapolis-mn",
]

# Property type searches - VERIFIED WORKING format
PROPERTY_TYPE_SEARCHES = [
    "condos-for-rent/minneapolis-mn",
    "townhomes-for-rent/minneapolis-mn",
    "houses-for-rent/minneapolis-mn",
]

# Special category searches - VERIFIED WORKING
SPECIAL_CATEGORY_SEARCHES = [
    "pet-friendly-apartments/minneapolis-mn",
    "furnished-apartments/minneapolis-mn",
    "luxury-apartments/minneapolis-mn",
    "cheap-apartments/minneapolis-mn",
    "student-housing/minneapolis-mn",
    
    # St Paul variations
    "pet-friendly-apartments/saint-paul-mn",
    "student-housing/saint-paul-mn",
]

# Price range searches - VERIFIED WORKING format
# These help find different units by filtering on price
PRICE_RANGE_SEARCHES = [
    "under-1000-minneapolis-mn",
    "under-1500-minneapolis-mn",
    "under-2000-minneapolis-mn",
    "1000-to-1500-minneapolis-mn",
    "1500-to-2000-minneapolis-mn",
    "over-2000-minneapolis-mn",
]

def generate_search_urls() -> List[str]:
    """
    Generate a focused list of VERIFIED WORKING search URLs for apartments.com.
    
    STRATEGY FOR FINDING MORE UNIQUE LISTINGS:
    1. Start with main city search - gets ALL listings, paginate through
    2. ZIP code searches - each ZIP surfaces buildings near that area first
    3. Bedroom filters - different sort order means different buildings at top
    4. Price ranges - different buildings appear at different price points
    5. Property types - condos, townhomes, houses (different from apartments)
    6. Special categories - pet-friendly, furnished, etc.
    
    IMPORTANT: All URL patterns have been verified to work on apartments.com.
    Bad patterns cause redirects/errors = wasted time with 0 results.
    """
    urls = []
    
    # 1. Primary locations (city + ZIP codes) - MOST RELIABLE
    urls.extend(PRIMARY_SEARCH_LOCATIONS)
    
    # 2. Bedroom filters - surfaces different buildings
    urls.extend(BEDROOM_FILTER_SEARCHES)
    
    # 3. Price range searches - different price = different buildings first
    urls.extend(PRICE_RANGE_SEARCHES)
    
    # 4. Property type searches
    urls.extend(PROPERTY_TYPE_SEARCHES)
    
    # 5. Special category searches
    urls.extend(SPECIAL_CATEGORY_SEARCHES)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    return unique_urls

# Generate the full search URL list (now ~35 verified working URLs)
SEARCH_LOCATIONS = generate_search_urls()

# Rate limiting settings
# Normal mode: slower but safer
PAGE_DELAY_SECONDS = 5.0  # Base delay (reduced from 6.0 for faster scraping)
# Turbo mode: faster but higher risk of detection (set by --turbo flag)
TURBO_PAGE_DELAY = 3.0  # Faster delays for turbo mode
PAGE_DELAY_VARIANCE = 5.0  # Random variance added to base delay (0 to this value)
GEOCODE_DELAY_SECONDS = 1.5

# Bot detection avoidance settings
SCROLL_DELAY_MIN = 0.5  # Minimum delay when scrolling
SCROLL_DELAY_MAX = 2.0  # Maximum delay when scrolling
MOUSE_MOVE_ENABLED = True  # Enable simulated mouse movements

# Bot detection retry settings (when "Access Denied" is detected)
BOT_DETECTION_BASE_WAIT = 30  # Base seconds to wait when bot detected
BOT_DETECTION_RETRY_INCREMENT = 15  # Additional seconds per retry attempt


def get_random_delay() -> float:
    """Get a randomized delay to avoid detection patterns."""
    base = PAGE_DELAY_SECONDS + random.uniform(0, PAGE_DELAY_VARIANCE)
    # Add occasional longer pauses to simulate human behavior
    if random.random() < 0.1:  # 10% chance of extra-long pause
        base += random.uniform(3, 8)
    return base

# User agents to rotate (helps avoid bot detection)
# Expanded list with more recent browser versions for 2024
USER_AGENTS = [
    # Chrome on Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # Chrome on Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # Safari on Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    # Firefox
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    # Edge
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
]

# Output
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = OUTPUT_DIR / f"umn_housing_data_{TIMESTAMP}.csv"
OUTPUT_CSV_ALL = OUTPUT_DIR / f"umn_housing_ALL_{TIMESTAMP}.csv"
LOG_FILE = OUTPUT_DIR / f"scraper_log_{TIMESTAMP}.log"

# Persistent output file for accumulating results across sessions
PERSISTENT_CSV = OUTPUT_DIR / "umn_housing_combined.csv"
SCRAPED_URLS_FILE = OUTPUT_DIR / "scraped_urls.txt"
LOCATION_COUNTER_FILE = OUTPUT_DIR / "location_counts.txt"

# Previously scraped URLs to skip (from user-reported lost data)
# These are URLs the user already scraped but lost - skip them to allow fresh re-scraping
# NOTE: Only include URLs within 10km of UMN campus. Removed: Edina, St. Louis Park (too far)
KNOWN_SCRAPED_URLS = [
    "https://www.apartments.com/lumos-apartments-minneapolis-mn/ztq9cyw/",
    "https://www.apartments.com/lakefield-apartments-minneapolis-mn/vms5ejg/",
    "https://www.apartments.com/the-archive-minneapolis-mn/8z05mr1/",
    "https://www.apartments.com/moment-minneapolis-mn/s1qg2g4/",
    "https://www.apartments.com/the-quad-on-delaware-minneapolis-mn/q5w315x/",
    "https://www.apartments.com/the-laker-minneapolis-mn/ezrcwgm/",
    "https://www.apartments.com/groove-lofts-minneapolis-mn/cmvfmhe/",
    "https://www.apartments.com/29-bryant-apartments-minneapolis-mn/gsnmjnz/",
    "https://www.apartments.com/expo-minneapolis-mn/90sk3p6/",
    # Removed: york-place-apartments-edina - Edina is ~12km from UMN, outside 10km radius
    "https://www.apartments.com/lago-apartments-minneapolis-mn/1fls1t4/",
    "https://www.apartments.com/vesi-minneapolis-mn/y3gy3ds/",
    "https://www.apartments.com/270-hennepin-minneapolis-mn/1eej1c4/",
    "https://www.apartments.com/xenia-apartments-minneapolis-mn/v2crl6g/",
    # Removed: arlo-west-end-saint-louis-park - St. Louis Park is ~13km from UMN, outside 10km radius
    "https://www.apartments.com/nox-apartments-minneapolis-mn/f7ksbzr/",
    "https://www.apartments.com/ironclad-residential-minneapolis-mn/6z0vvcv/",
    "https://www.apartments.com/minneapolis-grand-apartments-minneapolis-mn/bj3z6n9/",
    "https://www.apartments.com/aberdeen-apartments-minneapolis-mn/wjvqy5l/",
    "https://www.apartments.com/avid-minneapolis-mn/zzj68pw/",
    "https://www.apartments.com/welcome-to-equinox-apartments-saint-anthony-mn/0983tk1/",
    "https://www.apartments.com/sora-minneapolis-mn/",
]

# Student housing keywords
STUDENT_KEYWORDS = [
    "student housing", "student living", "off-campus housing",
    "student community", "by the bed", "individual lease",
    "per bedroom", "collegiate", "student apartments"
]

PER_BED_PATTERNS = [
    r"per\s+bed(?:room)?", r"by\s+the\s+bed", r"/\s*bed(?:room)?",
    r"individual\s+lease", r"bedroom\s+lease"
]

SHARED_BEDROOM_PATTERNS = [
    r"shared\s+bedroom", r"double\s+occupancy", r"2x\s+occupancy",
    r"roommate\s+matching", r"\d+\s+beds?\s+per\s+room"
]


# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class UnitListing:
    listing_id: str
    building_name: str
    full_address: str
    street: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    lat: Optional[float] = None
    lon: Optional[float] = None
    dist_to_campus_km: Optional[float] = None

    unit_label: str = ""
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
    rent_raw: str = ""
    rent_min: Optional[float] = None
    rent_max: Optional[float] = None
    price_type: str = "unknown"
    is_per_bed: Optional[bool] = None
    is_shared_bedroom: Optional[bool] = None

    year_built: Optional[int] = None
    num_units: Optional[int] = None
    building_type: str = ""
    stories: Optional[int] = None

    has_in_unit_laundry: Optional[bool] = None
    has_on_site_laundry: Optional[bool] = None
    has_dishwasher: Optional[bool] = None
    has_ac: Optional[bool] = None
    has_heat_included: Optional[bool] = None
    has_water_included: Optional[bool] = None
    has_internet_included: Optional[bool] = None
    is_furnished: Optional[bool] = None
    has_gym: Optional[bool] = None
    has_pool: Optional[bool] = None
    has_rooftop_or_clubroom: Optional[bool] = None
    has_parking_available: Optional[bool] = None
    has_garage: Optional[bool] = None
    pets_allowed: Optional[bool] = None
    is_student_branded: Optional[bool] = None

    scrape_date: str = field(default_factory=lambda: datetime.now().isoformat())
    source_url: str = ""


# ============================================================================
# PERSISTENCE AND DEDUPLICATION
# ============================================================================

def load_existing_listings(csv_path: Path, filter_by_distance: bool = True) -> Dict[str, UnitListing]:
    """Load existing listings from CSV file into a dict keyed by listing_id.
    
    Args:
        csv_path: Path to the CSV file
        filter_by_distance: If True, exclude listings beyond SEARCH_RADIUS_KM from UMN (default True)
    """
    existing = {}
    excluded_count = 0
    if csv_path.exists():
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    listing_id = row.get('listing_id', '')
                    if listing_id:
                        # Convert string values back to appropriate types
                        for key in ['lat', 'lon', 'dist_to_campus_km', 'beds', 'baths', 'rent_min', 'rent_max']:
                            if key in row and row[key]:
                                try:
                                    row[key] = float(row[key])
                                except (ValueError, TypeError):
                                    row[key] = None
                        for key in ['sqft', 'year_built', 'num_units', 'stories']:
                            if key in row and row[key]:
                                try:
                                    row[key] = int(float(row[key]))
                                except (ValueError, TypeError):
                                    row[key] = None
                        for key in ['is_per_bed', 'is_shared_bedroom', 'has_in_unit_laundry', 
                                    'has_on_site_laundry', 'has_dishwasher', 'has_ac',
                                    'has_heat_included', 'has_water_included', 'has_internet_included',
                                    'is_furnished', 'has_gym', 'has_pool', 'has_rooftop_or_clubroom',
                                    'has_parking_available', 'has_garage', 'pets_allowed', 'is_student_branded']:
                            if key in row:
                                if row[key] in ('True', 'true', '1'):
                                    row[key] = True
                                elif row[key] in ('False', 'false', '0'):
                                    row[key] = False
                                else:
                                    row[key] = None
                        
                        # Filter by distance if requested (enforce 10km radius)
                        if filter_by_distance:
                            dist = row.get('dist_to_campus_km')
                            if dist is not None and dist > SEARCH_RADIUS_KM:
                                building = row.get('building_name', 'Unknown')
                                logger.debug(f"Excluding {building}: {dist:.1f}km from UMN (>{SEARCH_RADIUS_KM}km)")
                                excluded_count += 1
                                continue
                        
                        existing[listing_id] = row
            if excluded_count > 0:
                logger.info(f"Excluded {excluded_count} listings beyond {SEARCH_RADIUS_KM}km radius")
            logger.info(f"Loaded {len(existing)} existing listings from {csv_path}")
        except Exception as e:
            logger.warning(f"Error loading existing listings: {e}")
    return existing


def load_scraped_urls(filepath: Path) -> Set[str]:
    """Load set of already-scraped building URLs, including known previously-scraped URLs."""
    # Start with known previously scraped URLs (from user's lost data)
    urls = set(KNOWN_SCRAPED_URLS)
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        urls.add(line.strip())
            logger.info(f"Loaded {len(urls)} previously scraped URLs (including {len(KNOWN_SCRAPED_URLS)} from history)")
        except Exception as e:
            logger.warning(f"Error loading scraped URLs: {e}")
    else:
        logger.info(f"Starting with {len(KNOWN_SCRAPED_URLS)} known scraped URLs from history")
    return urls


def save_scraped_url(filepath: Path, url: str):
    """Append a scraped URL to the tracking file."""
    try:
        with open(filepath, 'a') as f:
            f.write(url + '\n')
    except Exception as e:
        logger.warning(f"Error saving scraped URL: {e}")


def load_location_counts(filepath: Path) -> Dict[str, int]:
    """Load how many times each location has been scraped."""
    counts = {}
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if ':' in line:
                        loc, count = line.strip().rsplit(':', 1)
                        counts[loc] = int(count)
            logger.info(f"Loaded location counts for {len(counts)} locations")
        except Exception as e:
            logger.warning(f"Error loading location counts: {e}")
    return counts


def save_location_counts(filepath: Path, counts: Dict[str, int]):
    """Save location scrape counts to file."""
    try:
        with open(filepath, 'w') as f:
            for loc, count in counts.items():
                f.write(f"{loc}:{count}\n")
    except Exception as e:
        logger.warning(f"Error saving location counts: {e}")


def get_balanced_location_order(locations: List[str], counts: Dict[str, int]) -> List[str]:
    """
    Get locations ordered by how few times they've been scraped (least-scraped first).
    This ensures balanced coverage - no location is scraped more than twice before
    all others have been scraped twice.
    
    Also adds randomization within each tier to avoid predictable patterns.
    """
    # Group locations by their scrape count
    tiers = {}
    for loc in locations:
        count = counts.get(loc, 0)
        if count not in tiers:
            tiers[count] = []
        tiers[count].append(loc)
    
    # Shuffle each tier for randomization, then sort by count (lowest first)
    result = []
    for count in sorted(tiers.keys()):
        tier_locs = tiers[count]
        random.shuffle(tier_locs)  # Randomize within tier
        result.extend(tier_locs)
    
    return result


def merge_and_dedupe_units(new_units: List[UnitListing], existing: Dict[str, Any]) -> List[UnitListing]:
    """Merge new units with existing, keeping only unique listing_ids."""
    merged = dict(existing)  # Start with existing
    new_count = 0
    for unit in new_units:
        if unit.listing_id not in merged:
            merged[unit.listing_id] = asdict(unit)
            new_count += 1
    logger.info(f"Added {new_count} new unique listings (total: {len(merged)})")
    
    # Convert back to UnitListing objects
    result = []
    for listing_id, data in merged.items():
        if isinstance(data, dict):
            # Filter only valid UnitListing fields
            valid_fields = {f.name for f in fields(UnitListing)}
            filtered_data = {k: v for k, v in data.items() if k in valid_fields}
            try:
                result.append(UnitListing(**filtered_data))
            except Exception as e:
                logger.warning(f"Error creating UnitListing from data: {e}")
        elif isinstance(data, UnitListing):
            result.append(data)
    return result


def export_combined_csv(units: List[UnitListing], filename: Path):
    """Export all units to a combined CSV, overwriting previous."""
    fieldnames = list(UnitListing.__dataclass_fields__.keys())
    logger.info(f"Saving {len(units)} total listings to {filename}")
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if units:
            rows = [asdict(u) for u in units]
            writer.writerows(rows)
    logger.info(f"Combined CSV saved: {filename}")


# ============================================================================
# UTILITIES
# ============================================================================

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lon1_r, lat1_r, lon2_r, lat2_r = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2_r - lon1_r
    dlat = lat2_r - lat1_r
    a = sin(dlat/2)**2 + cos(lat1_r) * cos(lat2_r) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return 6371 * c


def geocode_address(address: str) -> Optional[Dict[str, float]]:
    """Geocode an address using Nominatim (OpenStreetMap). Tries polite jsonv2 + email and several cleaned variants.
    For numeric ranges (e.g. "3413-3433 ...") this will prefer the first house-number as the primary fallback.
    """
    try:
        headers = {'User-Agent': 'UMN-Housing-Research/1.0 (dillo370@umn.edu)'}
        base_url = "https://nominatim.openstreetmap.org/search"
        base_params = {
            'format': 'jsonv2',
            'limit': 1,
            'addressdetails': 1,
            'email': 'dillo370@umn.edu'
        }

        def generate_variants(addr: str):
            # original first
            yield addr.strip()

            # Replace numeric ranges like "3413-3433" with the first number only (prefer first endpoint)
            # e.g. "3413-3433 53rd Ave" -> "3413 53rd Ave"
            first_only = re.sub(r'^\s*(\d+)-\d+(\s+)', r'\1\2', addr)
            if first_only != addr:
                yield first_only.strip()

            # Remove any remaining simple ranges anywhere in the string (fallback to just the first number)
            no_range = re.sub(r'\b(\d+)-\d+\b', r'\1', addr)
            if no_range != addr and no_range != first_only:
                yield no_range.strip()

            # try removing anything before the first comma (some pages put building name first)
            if ',' in addr:
                after = addr.split(',', 1)[1].strip()
                if after:
                    yield after

            # try removing parenthetical content
            yield re.sub(r'\([^)]*\)', '', addr).strip()

            # try street + zip if zip present
            zip_match = re.search(r'(\d{5})', addr)
            if zip_match:
                street = addr.split(',')[0].strip()
                yield f"{street}, {zip_match.group(1)}"

        attempted = set()
        for variant in generate_variants(address):
            if not variant:
                continue
            if variant in attempted:
                continue
            attempted.add(variant)
            params = dict(base_params)
            params['q'] = variant

            logger.info(f"Geocoding: {variant}")
            time.sleep(GEOCODE_DELAY_SECONDS)  # polite pause

            # try with up to 2 attempts for transient errors
            for attempt in range(2):
                try:
                    response = requests.get(base_url, params=params, headers=headers, timeout=15)
                except Exception as e:
                    logger.warning(f"Geocoding request error for {variant}: {e}")
                    if attempt == 0:
                        time.sleep(1)
                        continue
                    break

                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data:
                            lat = float(data[0].get('lat'))
                            lon = float(data[0].get('lon'))
                            logger.info(f"Geocoding success for '{variant}': {lat:.6f}, {lon:.6f}")
                            return {'lat': lat, 'lon': lon}
                        else:
                            logger.warning(f"Geocoding returned empty result for: {variant}")
                            break
                    except Exception as e:
                        logger.warning(f"Error parsing geocode response for {variant}: {e}")
                        break
                elif 500 <= response.status_code < 600:
                    logger.warning(f"Geocoding HTTP {response.status_code} for {variant} (server error), retrying...")
                    time.sleep(1)
                    continue
                else:
                    logger.warning(f"Geocoding HTTP {response.status_code} for {variant}: {response.text[:200]}")
                    break

        # nothing matched
        return None

    except Exception as e:
        logger.error(f"Geocoding error for {address}: {e}")
        return None

def parse_price_text(price_text: str, full_text: str = "") -> Dict[str, Any]:
    result = {
        'rent_min': None,
        'rent_max': None,
        'price_type': 'unknown',
        'is_per_bed': None,
        'is_shared_bedroom': None
    }

    if not price_text:
        return result

    combined_text = f"{price_text} {full_text}".lower()

    for pattern in PER_BED_PATTERNS:
        if re.search(pattern, combined_text, re.IGNORECASE):
            result['is_per_bed'] = True
            result['price_type'] = 'per_bed'
            break

    for pattern in SHARED_BEDROOM_PATTERNS:
        if re.search(pattern, combined_text, re.IGNORECASE):
            result['is_shared_bedroom'] = True
            break

    numbers = re.findall(r'\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', price_text)
    numbers = [float(n.replace(',', '')) for n in numbers]

    if not numbers:
        return result

    text_lower = price_text.lower()
    if 'from' in text_lower:
        result['rent_min'] = numbers[0]
        result['rent_max'] = numbers[0]
        if result['price_type'] == 'unknown':
            result['price_type'] = 'from_price'
    elif len(numbers) == 2 and ('-' in price_text or '–' in price_text or 'to' in text_lower):
        result['rent_min'] = numbers[0]
        result['rent_max'] = numbers[1]
        if result['price_type'] == 'unknown':
            result['price_type'] = 'range'
    elif len(numbers) == 1:
        result['rent_min'] = numbers[0]
        result['rent_max'] = numbers[0]
        if result['price_type'] == 'unknown':
            result['price_type'] = 'per_unit'
    else:
        result['rent_min'] = min(numbers)
        result['rent_max'] = max(numbers)
        if result['price_type'] == 'unknown':
            result['price_type'] = 'range'

    if result['is_per_bed'] is None:
        result['is_per_bed'] = False
    if result['is_shared_bedroom'] is None:
        result['is_shared_bedroom'] = False

    return result


def parse_bedroom_count(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.lower().strip()
    if 'studio' in text:
        return 0.0
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bed|br)', text)
    if match:
        return float(match.group(1))
    return None


def parse_bathroom_count(text: str) -> Optional[float]:
    if not text:
        return None
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba)', text.lower())
    return float(match.group(1)) if match else None


def parse_sqft(text: str) -> Optional[int]:
    if not text:
        return None
    match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*(?:sq\.?\s*ft|sqft|sf)', text, re.IGNORECASE)
    return int(match.group(1).replace(',', '')) if match else None


def check_amenity(amenity_text: str, keywords: List[str]) -> bool:
    amm = amenity_text.lower() if amenity_text else ""
    return any(keyword.lower() in amm for keyword in keywords)


def is_student_housing(building_text: str) -> bool:
    text_lower = building_text.lower() if building_text else ""
    return any(keyword in text_lower for keyword in STUDENT_KEYWORDS)


def parse_address(address_text: str) -> Dict[str, str]:
    parts = {'street': '', 'city': '', 'state': '', 'zip': ''}
    try:
        address_text = address_text.strip()
        zip_match = re.search(r'\b(\d{5})\b', address_text)
        if zip_match:
            parts['zip'] = zip_match.group(1)

        segments = [s.strip() for s in address_text.split(',')]
        if len(segments) >= 1:
            parts['street'] = segments[0]
        if len(segments) >= 2:
            parts['city'] = segments[1]
        if len(segments) >= 3:
            state_match = re.search(r'\b([A-Z]{2})\b', segments[2])
            if state_match:
                parts['state'] = state_match.group(1)
    except Exception as e:
        logger.error(f"Error parsing address: {e}")
    return parts


# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

async def search_apartments(page: Page, location: str, max_pages: int = 10, start_page: int = 1) -> List[str]:
    """
    Search for apartments at a location.
    
    Args:
        page: Playwright page
        location: Search location string (can be a neighborhood slug, ZIP code, or filter URL path)
        max_pages: Maximum number of search result pages to scrape
        start_page: Page number to start from (1 = first page, 2 = skip to page 2, etc.)
    
    Returns:
        List of building URLs found
    """
    logger.info(f"Starting search for: {location}")
    if start_page > 1:
        logger.info(f"Skipping to page {start_page} (to find different buildings)")
    building_urls = set()
    try:
        # Build search URL - location is already formatted as a URL path segment
        # Examples: "dinkytown-minneapolis-mn", "55414/min-1000-max-1500/"
        search_url = f"{BASE_URL}/{location}/"
        
        # Clean up double slashes if any
        search_url = search_url.replace("//", "/").replace("https:/", "https://")
        
        # If starting from a later page, add page number to URL
        if start_page > 1:
            # Insert page number before any trailing filters
            if search_url.endswith('/'):
                search_url = search_url[:-1] + f"/{start_page}/"
            else:
                search_url = search_url + f"/{start_page}/"
        
        logger.info(f"Navigating to: {search_url}")

        for attempt in range(5):  # Increased retries
            try:
                response = await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(get_random_delay())
                
                # Check for redirect or access denied
                final_url = page.url
                if 'access' in final_url.lower() or 'denied' in final_url.lower():
                    logger.warning(f"Access denied detected! Redirected to: {final_url}")
                    raise Exception("Access denied - bot detection triggered")
                
                # Check page content for block messages
                body_text = await page.locator('body').inner_text()
                if 'access denied' in body_text.lower() or 'blocked' in body_text.lower():
                    logger.warning("Block message detected in page content!")
                    raise Exception("Access denied - bot detection in page content")
                
                # Human-like behavior: scroll down slowly
                await simulate_human_scrolling(page)
                
                # Wait for common result container
                await page.wait_for_selector('article.placard, .placard', timeout=20000)
                break
            except Exception as e:
                error_str = str(e)
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                
                # For HTTP2 errors, wait longer before retry
                if 'ERR_HTTP2' in error_str or 'PROTOCOL_ERROR' in error_str:
                    wait_time = 10 + (attempt * 5)  # 10, 15, 20, 25, 30 seconds
                    logger.info(f"HTTP/2 error detected. Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                elif 'access denied' in error_str.lower() or 'blocked' in error_str.lower():
                    # Use defined constants for bot detection wait times
                    wait_time = BOT_DETECTION_BASE_WAIT + (attempt * BOT_DETECTION_RETRY_INCREMENT)
                    logger.warning(f"Bot detection! Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                elif attempt < 4:
                    await asyncio.sleep(5 + attempt * 2 + random.uniform(0, 3))
                else:
                    logger.error("Max retries reached. Try running with --headless=False to debug.")
                    logger.error("If issue persists, check your network connection or try later.")
                    raise

        for page_num in range(start_page, start_page + max_pages):
            logger.info(f"Scraping search results page {page_num}")
            
            # Human-like behavior: random scroll before extracting
            await simulate_human_scrolling(page)
            
            # Try multiple selectors to find property links
            property_links = await page.locator('article.placard a.property-link, a.property-link').all()
            if not property_links:
                # Try alternative selectors
                property_links = await page.locator('.property-title a, a[data-listingid]').all()
            if not property_links:
                logger.warning("No property links found with standard selectors")
                # Try even broader selector as last resort
                property_links = await page.locator('a[href*="apartments.com/"]').all()

            for link in property_links:
                try:
                    href = await link.get_attribute('href')
                    if not href:
                        continue
                    
                    # Resolve relative URLs to absolute using the base URL
                    full_url = urljoin(BASE_URL, href)
                    full_url = full_url.split('?')[0]  # Remove query params
                    
                    # SECURITY: Validate the URL starts with our expected domain
                    # This prevents following malicious redirects to other domains
                    if not full_url.startswith('https://www.apartments.com/'):
                        continue
                        
                    # Filter out search/filter pages (not building detail pages)
                    excluded_patterns = ['/search/', 'bbox=']
                    if any(x in full_url for x in excluded_patterns):
                        continue
                    
                    # Make sure it looks like a building URL (has a building slug)
                    # Building slugs are typically like "the-laker-minneapolis-mn/abc123"
                    # which is always more than 5 characters and contains hyphens
                    path = full_url.replace('https://www.apartments.com/', '').strip('/')
                    parts = path.split('/')
                    if len(parts) >= 1:
                        slug = parts[0]
                        # Valid building slug: at least 6 chars, contains hyphen, 
                        # not a pure filter like "1-bedrooms" or a city like "minneapolis-mn"
                        is_valid_building = (
                            len(slug) > 5 and 
                            '-' in slug and 
                            not slug.endswith('-mn')  # City pages end with -mn
                        )
                        if is_valid_building:
                            building_urls.add(full_url)
                except Exception as e:
                    logger.warning(f"Error extracting link: {e}")

            logger.info(f"Found {len(building_urls)} unique buildings so far")

            next_button = page.locator('a.next, a[rel="next"]')
            if await next_button.count() > 0:
                try:
                    await next_button.first.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=30000)
                    await asyncio.sleep(get_random_delay())
                    
                    # Human-like scroll after page load
                    await simulate_human_scrolling(page)
                except Exception as e:
                    logger.info(f"Pagination ended: {e}")
                    break
            else:
                logger.info("No next page button found")
                break

    except Exception as e:
        logger.error(f"Error during search: {e}")

    logger.info(f"Search complete. Found {len(building_urls)} total buildings")
    return list(building_urls)


async def simulate_human_scrolling(page: Page):
    """Simulate human-like scrolling to avoid bot detection."""
    try:
        # Random scroll pattern
        scroll_steps = random.randint(2, 5)
        for _ in range(scroll_steps):
            scroll_amount = random.randint(200, 600)
            await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
        
        # Sometimes scroll back up a bit
        if random.random() < 0.3:
            scroll_back = random.randint(100, 300)
            await page.evaluate(f"window.scrollBy(0, -{scroll_back})")
            await asyncio.sleep(random.uniform(0.5, 1.0))
    except Exception as e:
        logger.debug(f"Scroll simulation error (non-fatal): {e}")


async def scrape_building(page: Page, url: str) -> List[UnitListing]:
    logger.info(f"Scraping building: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(get_random_delay())
        building_data = await extract_building_info(page, url)
        all_units = await extract_units(page, building_data)
        sampled_units = sample_units(all_units)
        logger.info(f"Extracted {len(sampled_units)} units from building")
        return sampled_units
    except Exception as e:
        logger.error(f"Error scraping building {url}: {e}")
        return []


async def extract_building_info(page: Page, url: str) -> Dict[str, Any]:
    building_data = {
        'source_url': url,
        'building_name': '',
        'full_address': '',
        'street': '',
        'city': '',
        'state': '',
        'zip': '',
        'lat': None,
        'lon': None,
        'year_built': None,
        'num_units': None,
        'building_type': '',
        'stories': None,
        'amenities': {},
        'is_student_branded': False,
        'full_page_text': ''
    }
    try:
        # Name
        name_selectors = ['h1.propertyName', 'h1.property-title', 'h1']
        for selector in name_selectors:
            name_elem = page.locator(selector).first
            if await name_elem.count() > 0:
                nm = (await name_elem.inner_text()).strip()
                if nm:
                    building_data['building_name'] = nm
                    logger.info(f"Building name: {nm}")
                    break

        # city/state from URL slug
        url_parts = url.lower().replace('https://www.apartments.com/', '').split('/')
        city_state_from_url = "Minneapolis, MN"
        if len(url_parts) > 0:
            slug = url_parts[0]
            if 'st-paul' in slug or 'saint-paul' in slug:
                city_state_from_url = "St Paul, MN"
            elif 'brooklyn-center' in slug:
                city_state_from_url = "Brooklyn Center, MN"
            elif 'brooklyn-park' in slug:
                city_state_from_url = "Brooklyn Park, MN"

        # Address extraction with multiple strategies
        address_found = False
        street_only = ""

        address_selectors = [
            '.propertyAddress', '.property-address', '[itemprop="address"]',
            '[class*="address"]', 'address', '.propertyAddressContainer'
        ]
        for selector in address_selectors:
            addr_elem = page.locator(selector).first
            if await addr_elem.count() > 0:
                addr = (await addr_elem.inner_text()).strip()
                if addr and len(addr) > 5:
                    if any(city in addr.lower() for city in ['minneapolis', 'st paul', 'brooklyn']):
                        building_data['full_address'] = addr
                        logger.info(f"Found COMPLETE address: {addr}")
                        address_found = True
                        break
                    else:
                        street_only = addr.rstrip(',').strip()
                        logger.info(f"Found street address: {street_only}")

        if not address_found and street_only:
            building_data['full_address'] = f"{street_only}, {city_state_from_url}"
            logger.info(f"Combined address: {building_data['full_address']}")
            address_found = True

        # Meta tags
        if not address_found:
            try:
                meta_street = await page.locator('meta[property="og:street-address"]').get_attribute('content')
                meta_city = await page.locator('meta[property="og:locality"]').get_attribute('content')
                meta_state = await page.locator('meta[property="og:region"]').get_attribute('content')
                meta_zip = await page.locator('meta[property="og:postal-code"]').get_attribute('content')
                if meta_street and meta_city:
                    building_data['full_address'] = f"{meta_street}, {meta_city}, {meta_state} {meta_zip}"
                    logger.info(f"Found address (via meta): {building_data['full_address']}")
                    address_found = True
            except:
                pass

        # JSON-LD: try to extract address and geo coordinates (prefer page-provided coords)
        try:
            script_elems = await page.locator('script[type="application/ld+json"]').all()
            for script in script_elems:
                try:
                    json_text = await script.inner_text()
                    data = json.loads(json_text)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        addr = item.get('address') or item.get('contactPoint') or {}
                        if isinstance(addr, dict):
                            street = addr.get('streetAddress') or addr.get('street') or ''
                            city = addr.get('addressLocality') or addr.get('city') or ''
                            state = addr.get('addressRegion') or addr.get('state') or ''
                            zipcode = addr.get('postalCode') or ''
                            if street and city and not address_found:
                                building_data['full_address'] = f"{street}, {city}, {state} {zipcode}".strip()
                                logger.info(f"Found address (via JSON-LD): {building_data['full_address']}")
                                address_found = True
                        geo = item.get('geo') or item.get('location') or item.get('hasMap') or {}
                        if isinstance(geo, dict):
                            lat = geo.get('latitude') or geo.get('lat') or geo.get('lng') or geo.get('lon')
                            lon = geo.get('longitude') or geo.get('lon') or geo.get('lng')
                            if lat and lon:
                                try:
                                    building_data['lat'] = float(lat)
                                    building_data['lon'] = float(lon)
                                    logger.info(f"Found coordinates (via JSON-LD): {building_data['lat']}, {building_data['lon']}")
                                except:
                                    pass
                    if address_found or (building_data.get('lat') is not None and building_data.get('lon') is not None):
                        break
                except:
                    pass
        except:
            pass

        # Map coords fallback
        if not address_found:
            try:
                map_elem = page.locator('#map, [id*="map"]').first
                if await map_elem.count() > 0:
                    lat_attr = await map_elem.get_attribute('data-latitude')
                    lon_attr = await map_elem.get_attribute('data-longitude')
                    if lat_attr and lon_attr:
                        building_data['lat'] = float(lat_attr)
                        building_data['lon'] = float(lon_attr)
                        logger.info(f"Found coordinates: {building_data['lat']}, {building_data['lon']}")
                        if street_only:
                            building_data['full_address'] = f"{street_only}, {city_state_from_url}"
                        else:
                            building_data['full_address'] = f"{building_data['building_name']}, {city_state_from_url}"
                        address_found = True
            except:
                pass

        # Final fallback
        if not address_found:
            if street_only:
                building_data['full_address'] = f"{street_only}, {city_state_from_url} 55414"
            elif building_data['building_name']:
                building_data['full_address'] = f"{building_data['building_name']}, {city_state_from_url} 55414"
            logger.warning(f"Using fallback address: {building_data['full_address']}")

        if building_data['full_address']:
            address_parts = parse_address(building_data['full_address'])
            building_data.update(address_parts)

        body_text = await page.locator('body').inner_text()
        building_data['full_page_text'] = body_text
        building_data['is_student_branded'] = is_student_housing(body_text)
        building_data['amenities'] = await extract_amenities(page)

    except Exception as e:
        logger.error(f"Error extracting building info: {e}")

    return building_data


async def extract_amenities(page: Page) -> Dict[str, bool]:
    amenities = {
        'has_in_unit_laundry': None,
        'has_on_site_laundry': None,
        'has_dishwasher': None,
        'has_ac': None,
        'has_heat_included': None,
        'has_water_included': None,
        'has_internet_included': None,
        'is_furnished': None,
        'has_gym': None,
        'has_pool': None,
        'has_rooftop_or_clubroom': None,
        'has_parking_available': None,
        'has_garage': None,
        'pets_allowed': None,
    }
    try:
        body_text = await page.locator('body').inner_text()
        amenity_text = (body_text or "").lower()
        amenities['has_in_unit_laundry'] = check_amenity(amenity_text,
                                                       ['in-unit laundry', 'washer/dryer in unit', 'in unit washer'])
        amenities['has_on_site_laundry'] = check_amenity(amenity_text, ['on-site laundry', 'laundry facilities'])
        amenities['has_dishwasher'] = check_amenity(amenity_text, ['dishwasher'])
        amenities['has_ac'] = check_amenity(amenity_text, ['air conditioning', 'central air', 'a/c'])
        amenities['has_heat_included'] = check_amenity(amenity_text, ['heat included'])
        amenities['has_water_included'] = check_amenity(amenity_text, ['water included'])
        amenities['has_internet_included'] = check_amenity(amenity_text, ['internet included', 'wifi included'])
        amenities['is_furnished'] = check_amenity(amenity_text, ['furnished'])
        amenities['has_gym'] = check_amenity(amenity_text, ['fitness center', 'gym'])
        amenities['has_pool'] = check_amenity(amenity_text, ['pool'])
        amenities['has_rooftop_or_clubroom'] = check_amenity(amenity_text, ['rooftop', 'clubhouse'])
        amenities['has_parking_available'] = check_amenity(amenity_text, ['parking'])
        amenities['has_garage'] = check_amenity(amenity_text, ['garage'])
        amenities['pets_allowed'] = check_amenity(amenity_text, ['pet friendly', 'pets allowed'])
    except Exception as e:
        logger.error(f"Error extracting amenities: {e}")
    return amenities


async def extract_units(page: Page, building_data: Dict[str, Any]) -> List[UnitListing]:
    units: List[UnitListing] = []
    try:
        selectors = [
            'tr.rentalGridRow',
            '.pricingGridItem',
            '.pricing-item',
            'article.pricingItem',
            '.floorplan-row',
            '[data-tid="floorplan"]'
        ]
        floorplan_rows = None
        for selector in selectors:
            floorplan_rows = page.locator(selector)
            num_rows = await floorplan_rows.count()
            if num_rows > 0:
                logger.info(f"Found {num_rows} floorplans using selector: {selector}")
                break

        if not floorplan_rows or await floorplan_rows.count() == 0:
            logger.warning("No floorplan rows found")
            return units

        for i in range(await floorplan_rows.count()):
            try:
                row = floorplan_rows.nth(i)
                unit = await parse_unit_row(row, building_data)
                if unit:
                    units.append(unit)
            except Exception as e:
                logger.warning(f"Error parsing unit row {i}: {e}")

    except Exception as e:
        logger.error(f"Error extracting units: {e}")

    return units


async def parse_unit_row(row, building_data: Dict[str, Any]) -> Optional[UnitListing]:
    try:
        row_text = await row.inner_text()
        beds = parse_bedroom_count(row_text)
        baths = parse_bathroom_count(row_text)
        sqft = parse_sqft(row_text)
        rent_raw = ""
        rent_match = re.search(r'\$[\d,]+(?:\s*-\s*\$[\d,]+)?', row_text)
        if rent_match:
            rent_raw = rent_match.group(0)
        if not rent_raw or 'call' in rent_raw.lower():
            return None
        price_info = parse_price_text(rent_raw, building_data.get('full_page_text', ''))
        unit = UnitListing(
            listing_id=f"{building_data['source_url'].split('/')[-2]}-{beds or 0}bed",
            building_name=building_data['building_name'],
            full_address=building_data['full_address'],
            street=building_data.get('street', ''),
            city=building_data.get('city', ''),
            state=building_data.get('state', ''),
            zip=building_data.get('zip', ''),
            lat=building_data.get('lat'),
            lon=building_data.get('lon'),
            beds=beds,
            baths=baths,
            sqft=sqft,
            rent_raw=rent_raw,
            rent_min=price_info['rent_min'],
            rent_max=price_info['rent_max'],
            price_type=price_info['price_type'],
            is_per_bed=price_info['is_per_bed'],
            is_student_branded=building_data.get('is_student_branded', False),
            source_url=building_data['source_url']
        )
        for key, value in building_data.get('amenities', {}).items():
            setattr(unit, key, value)
        return unit
    except Exception as e:
        logger.error(f"Error parsing unit row: {e}")
        return None


def sample_units(units: List[UnitListing]) -> List[UnitListing]:
    if not units:
        return []
    by_beds: Dict[float, List[UnitListing]] = {}
    for unit in units:
        if unit.beds is not None and unit.rent_min is not None:
            if unit.beds not in by_beds:
                by_beds[unit.beds] = []
            by_beds[unit.beds].append(unit)
    selected: List[UnitListing] = []
    if 1.0 in by_beds:
        selected.append(max(by_beds[1.0], key=lambda u: (u.sqft is not None, u.sqft or 0)))
    if 2.0 in by_beds:
        selected.append(max(by_beds[2.0], key=lambda u: (u.sqft is not None, u.sqft or 0)))
    if len(selected) < 2:
        remaining = [u for u in units if u not in selected and u.rent_min is not None]
        if remaining:
            remaining.sort(key=lambda u: (u.sqft is not None, u.beds or 0, u.sqft or 0))
            for unit in remaining:
                if unit not in selected:
                    selected.append(unit)
                    if len(selected) >= 2:
                        break
    return selected[:2]


def geocode_and_filter_units(units: List[UnitListing], existing_ids: Set[str] = None) -> List[UnitListing]:
    """
    Geocode and filter units. Optionally skip units that already exist in a previous run.
    
    Args:
        units: List of units to process
        existing_ids: Set of listing_ids that already exist (to skip geocoding for duplicates)
    
    Returns:
        List of filtered units within the search radius
    """
    logger.info(f"Geocoding and filtering {len(units)} units")
    
    # First, filter out units that already exist to save geocoding time
    if existing_ids:
        original_count = len(units)
        units = [u for u in units if u.listing_id not in existing_ids]
        skipped = original_count - len(units)
        if skipped > 0:
            logger.info(f"Skipped {skipped} duplicate units before geocoding (already in combined data)")
    
    if not units:
        logger.info("No new units to geocode")
        return []
    
    by_address: Dict[str, List[UnitListing]] = {}
    for unit in units:
        addr = unit.full_address or ""
        if addr not in by_address:
            by_address[addr] = []
        by_address[addr].append(unit)
    logger.info(f"Unique addresses to geocode: {len(by_address)}")
    for address, address_units in by_address.items():
        if not address:
            logger.warning("Empty address string, skipping geocode for this address key")
            continue
        if address_units[0].lat is None or address_units[0].lon is None:
            logger.info(f"Geocoding: {address}")
            coords = geocode_address(address)
            if coords:
                logger.info(f"  ✓ Found: {coords['lat']:.4f}, {coords['lon']:.4f}")
                for unit in address_units:
                    unit.lat = coords['lat']
                    unit.lon = coords['lon']
    filtered: List[UnitListing] = []
    excluded_too_far = 0
    for unit in units:
        if unit.lat is not None and unit.lon is not None:
            dist = haversine_distance(unit.lat, unit.lon, UMN_CAMPUS_LAT, UMN_CAMPUS_LON)
            unit.dist_to_campus_km = round(dist, 2)
            logger.info(f"{unit.building_name}: {dist:.2f} km from UMN")
            if dist <= SEARCH_RADIUS_KM:
                filtered.append(unit)
                logger.info("  ✓ INCLUDED (within 10km)")
            else:
                excluded_too_far += 1
                logger.info(f"  ✗ EXCLUDED (>{SEARCH_RADIUS_KM}km from UMN - too far)")
        else:
            logger.warning(f"Could not geocode: {unit.full_address}")
    if excluded_too_far > 0:
        logger.info(f"Excluded {excluded_too_far} units for being >{SEARCH_RADIUS_KM}km from UMN")
    logger.info(f"Filtered to {len(filtered)} units within {SEARCH_RADIUS_KM} km of UMN")
    return filtered


def export_to_csv(units: List[UnitListing], filename: Path):
    """Export unit listings to CSV file. Always writes header so file exists even if empty."""
    fieldnames = list(UnitListing.__dataclass_fields__.keys())
    logger.info(f"Exporting {len(units)} units to {filename}")
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if units:
            rows = [asdict(u) for u in units]
            writer.writerows(rows)
    logger.info(f"Export complete: {filename}")


async def main(headless: bool = True, max_search_pages: int = 25, max_buildings: int = None, 
               skip_scraped: bool = False, search_location: str = None, start_page: int = 1) -> int:
    """
    Main scraping function. Returns the number of units scraped in this session.
    
    Args:
        headless: Run browser in headless mode
        max_search_pages: Max search result pages to scrape
        max_buildings: Max buildings to scrape (None = unlimited)
        skip_scraped: Skip buildings that were already scraped (for auto-restart mode)
        search_location: Location to search (defaults to SEARCH_LOCATION if not specified)
        start_page: Search result page to start from (1 = first, 2+ = skip ahead to find different buildings)
    
    Returns:
        Number of units scraped in this session
    """
    location = search_location or SEARCH_LOCATION
    logger.info("="*80)
    logger.info("UMN HOUSING SCRAPER STARTED")
    logger.info("="*80)
    logger.info(f"Search location: {location}")
    logger.info(f"Search radius: {SEARCH_RADIUS_KM} km from UMN campus")
    logger.info(f"Headless mode: {headless}")
    logger.info(f"Max search pages: {max_search_pages}")
    logger.info(f"Start page: {start_page}" + (" (skipping ahead)" if start_page > 1 else ""))
    logger.info(f"Max buildings: {max_buildings if max_buildings else 'unlimited'}")
    logger.info(f"Skip already scraped: {skip_scraped}")
    logger.info(f"Output file: {OUTPUT_CSV}")

    all_units: List[UnitListing] = []
    scraped_urls = load_scraped_urls(SCRAPED_URLS_FILE) if skip_scraped else set()
    consecutive_failures = 0
    max_consecutive_failures = 10  # Stop session if too many failures in a row

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-http2',  # Fix for ERR_HTTP2_PROTOCOL_ERROR
            ]
        )

        # Select a random user agent to help avoid detection
        selected_user_agent = random.choice(USER_AGENTS)
        logger.info(f"Using user agent: {selected_user_agent[:50]}...")

        context = await browser.new_context(
            user_agent=selected_user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/Chicago',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
        )

        page = await context.new_page()

        try:
            building_urls = await search_apartments(page, location, max_search_pages, start_page)

            # Filter out already-scraped URLs
            if skip_scraped and scraped_urls:
                original_count = len(building_urls)
                building_urls = [url for url in building_urls if url not in scraped_urls]
                logger.info(f"Filtered out {original_count - len(building_urls)} already-scraped buildings")

            if max_buildings:
                building_urls = building_urls[:max_buildings]
                logger.info(f"Limited to {max_buildings} buildings for testing")

            for idx, url in enumerate(building_urls, 1):
                logger.info(f"Processing building {idx}/{len(building_urls)}")
                try:
                    units = await scrape_building(page, url)
                    if units:
                        all_units.extend(units)
                        consecutive_failures = 0  # Reset on success
                        # Track this URL as scraped
                        save_scraped_url(SCRAPED_URLS_FILE, url)
                    else:
                        consecutive_failures += 1
                    logger.info(f"Total units collected: {len(all_units)}")
                except Exception as e:
                    logger.error(f"Failed to scrape {url}: {e}")
                    consecutive_failures += 1
                    
                    # Check for bot detection indicators
                    error_str = str(e).lower()
                    if 'access denied' in error_str or 'blocked' in error_str or 'captcha' in error_str:
                        logger.warning("Bot detection likely triggered!")
                        if consecutive_failures >= 3:
                            logger.error("Multiple consecutive failures - ending session early")
                            break

                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({consecutive_failures}) - ending session")
                    break

                # Use randomized delay to avoid detection patterns
                delay = get_random_delay()
                logger.debug(f"Waiting {delay:.1f}s before next building...")
                await asyncio.sleep(delay)

        finally:
            await browser.close()

    # Save ALL data first (before filtering)
    logger.info("Saving unfiltered data...")
    export_to_csv(all_units, OUTPUT_CSV_ALL)

    # Load existing listings to skip geocoding duplicates
    existing_listings = load_existing_listings(PERSISTENT_CSV)
    existing_ids = set(existing_listings.keys())
    
    # Now filter and save filtered data (skipping already-known duplicates before geocoding)
    filtered_units = geocode_and_filter_units(all_units, existing_ids)
    export_to_csv(filtered_units, OUTPUT_CSV)

    # Merge with existing data (don't reload - use what we already have)
    merged_units = merge_and_dedupe_units(filtered_units, existing_listings)
    export_combined_csv(merged_units, PERSISTENT_CSV)

    logger.info("="*80)
    logger.info("SCRAPING COMPLETE")
    logger.info("="*80)
    logger.info(f"This session scraped: {len(all_units)} units")
    logger.info(f"Units within {SEARCH_RADIUS_KM} km: {len(filtered_units)}")
    logger.info(f"Total accumulated (deduplicated): {len(merged_units)}")
    logger.info(f"Unique buildings: {len(set(u.building_name for u in filtered_units))}")
    logger.info(f"Student-branded properties: {sum(1 for u in filtered_units if u.is_student_branded)}")
    logger.info(f"Per-bed pricing detected: {sum(1 for u in filtered_units if u.is_per_bed)}")
    logger.info(f"Session data: {OUTPUT_CSV}")
    logger.info(f"Combined data: {PERSISTENT_CSV}")
    logger.info(f"Log: {LOG_FILE}")
    
    return len(all_units)


async def auto_restart_scraper(headless: bool = True, max_search_pages: int = 25, 
                                max_buildings: int = 100, max_sessions: int = 50,
                                session_cooldown: int = 600, target_listings: int = 1000,
                                turbo: bool = False):
    """
    Automatically run multiple scraping sessions with cooldowns between them.
    
    Uses balanced location ordering to ensure all locations get searched before
    any location is searched a second time.
    
    STRATEGY FOR FINDING UNIQUE LISTINGS (v2 - VERIFIED URLS ONLY):
    - Use ONLY verified working apartments.com URL patterns
    - Main city search + ZIP codes = most reliable
    - Bedroom/price/property type filters = surfaces different buildings
    - Each session searches a different location with many pages
    - Skip already-scraped buildings to save time
    
    Args:
        headless: Run browser in headless mode
        max_search_pages: Max search pages per session (default 25 - increased for fewer locations)
        max_buildings: Max buildings per session (default 100)
        max_sessions: Maximum number of sessions to run (default 50)
        session_cooldown: Seconds to wait between sessions (default 600 = 10 minutes)
        target_listings: Stop when this many total listings are collected
        turbo: Use faster delays (higher risk of detection but more data)
    """
    # If turbo mode, use faster delays
    global PAGE_DELAY_SECONDS
    if turbo:
        PAGE_DELAY_SECONDS = TURBO_PAGE_DELAY
        logger.info("🚀 TURBO MODE ENABLED - Using faster delays")
    
    logger.info("="*80)
    logger.info("AUTO-RESTART MODE ENABLED")
    logger.info("="*80)
    logger.info(f"Max sessions: {max_sessions}")
    logger.info(f"Cooldown between sessions: {session_cooldown} seconds")
    logger.info(f"Target listings: {target_listings}")
    logger.info(f"Buildings per session: {max_buildings}")
    logger.info(f"Search locations: {len(SEARCH_LOCATIONS)} different searches")
    logger.info(f"Strategy: Search ALL pages from page 1 for each location")
    if turbo:
        logger.info(f"⚡ TURBO: Using {PAGE_DELAY_SECONDS}s delays instead of normal 5s")
    
    # Load location scrape counts for balanced coverage
    location_counts = load_location_counts(LOCATION_COUNTER_FILE)
    logger.info("Using balanced location ordering (least-scraped first)")
    
    total_scraped = 0
    session_num = 0
    zero_sessions_in_a_row = 0
    
    while session_num < max_sessions:
        session_num += 1
        
        # Get balanced location order (least-scraped locations first)
        balanced_locations = get_balanced_location_order(SEARCH_LOCATIONS, location_counts)
        
        # Pick the first location (least scraped)
        current_location = balanced_locations[0]
        current_count = location_counts.get(current_location, 0)
        
        logger.info(f"\n{'='*80}")
        logger.info(f"STARTING SESSION {session_num}/{max_sessions}")
        logger.info(f"Searching: {current_location}")
        logger.info(f"URL: https://www.apartments.com/{current_location}/")
        logger.info(f"Previously scraped this location: {current_count} times")
        logger.info(f"Will scrape up to {max_search_pages} pages")
        logger.info(f"{'='*80}\n")
        
        try:
            # ALWAYS start from page 1 to get ALL buildings in each location
            # The key to finding unique listings is searching DIFFERENT locations,
            # not skipping to later pages of the same location
            units_scraped = await main(
                headless=headless,
                max_search_pages=max_search_pages,
                max_buildings=max_buildings,
                skip_scraped=True,
                search_location=current_location,
                start_page=1  # Always start from page 1
            )
            total_scraped += units_scraped
            
            # Update and save location count
            location_counts[current_location] = location_counts.get(current_location, 0) + 1
            save_location_counts(LOCATION_COUNTER_FILE, location_counts)
            
            # Check if we've reached target
            existing = load_existing_listings(PERSISTENT_CSV)
            total_listings = len(existing)
            logger.info(f"Total accumulated listings: {total_listings}")
            
            if total_listings >= target_listings:
                logger.info(f"✓ Reached target of {target_listings} listings!")
                break
                
            if units_scraped == 0:
                zero_sessions_in_a_row += 1
                logger.warning(f"Session produced 0 units - may be blocked ({zero_sessions_in_a_row} in a row)")
                
                # If we've had multiple zeros, the location may be exhausted or blocked
                # The balanced ordering will automatically move to another location next time
                if zero_sessions_in_a_row >= 3:
                    logger.info("Multiple zero sessions - bot detection may be active")
                    zero_sessions_in_a_row = 0
                
                # Increase cooldown if blocked
                extended_cooldown = session_cooldown * 2
                logger.info(f"Extended cooldown: {extended_cooldown} seconds")
                await asyncio.sleep(extended_cooldown)
            else:
                zero_sessions_in_a_row = 0  # Reset on success
                if session_num < max_sessions:
                    # Add some randomness to cooldown timing
                    actual_cooldown = session_cooldown + random.randint(-60, 120)
                    actual_cooldown = max(60, actual_cooldown)  # At least 1 minute
                    logger.info(f"Cooling down for {actual_cooldown} seconds before next session...")
                    await asyncio.sleep(actual_cooldown)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user - stopping auto-restart")
            break
        except Exception as e:
            logger.error(f"Session {session_num} failed with error: {e}")
            logger.info(f"Waiting {session_cooldown} seconds before retry...")
            await asyncio.sleep(session_cooldown)
    
    # Final summary
    existing = load_existing_listings(PERSISTENT_CSV)
    logger.info("\n" + "="*80)
    logger.info("AUTO-RESTART COMPLETE")
    logger.info("="*80)
    logger.info(f"Sessions run: {session_num}")
    logger.info(f"Total unique listings collected: {len(existing)}")
    logger.info(f"Combined data file: {PERSISTENT_CSV}")
    
    # Show location coverage
    logger.info("\nLocation coverage:")
    for loc in sorted(location_counts.keys(), key=lambda x: location_counts[x], reverse=True):
        logger.info(f"  {loc}: {location_counts[loc]} sessions")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Apartments.com Web Scraper for UMN Housing Research',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test run (visible browser, 1 page, 2 buildings)
  python3 -m scraper.main --headless=False --max_search_pages=1 --max_buildings=2

  # Full headless run (background scrape)
  python3 -m scraper.main --headless=True --max_search_pages=50 --max_buildings=800

  # Auto-restart mode (runs multiple sessions, deduplicates, accumulates results)
  python3 -m scraper.main --auto_restart --max_sessions=5 --session_cooldown=300 --target_listings=1000

  # Overnight auto-restart (runs until target reached or max sessions)
  nohup python3 -m scraper.main --auto_restart --headless=True --max_sessions=10 --target_listings=2000 > output/auto.log 2>&1 &

  # Direct URL scraping (re-scrape specific buildings)
  python3 -m scraper.main --scrape_urls --headless=False
  
  # Direct URL scraping from a file
  python3 -m scraper.main --scrape_urls --url_file=my_urls.txt --headless=False
        """
    )

    def str_to_bool(value):
        """Convert string to boolean for argparse."""
        if isinstance(value, bool):
            return value
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off'):
            return False
        else:
            raise argparse.ArgumentTypeError(f"Boolean value expected, got '{value}'")

    parser.add_argument(
        '--headless',
        type=str_to_bool,
        default=True,
        help='Run browser in headless mode (True/False). Default: True'
    )
    parser.add_argument(
        '--max_search_pages',
        type=int,
        default=50,
        help='Maximum number of search result pages to scrape. Default: 50'
    )
    parser.add_argument(
        '--max_buildings',
        type=int,
        default=None,
        help='Maximum number of buildings to scrape per session. Default: unlimited'
    )
    parser.add_argument(
        '--start_page',
        type=int,
        default=1,
        help='Search result page to start from (1=first, 2=skip to page 2, etc.). Useful for finding different buildings. Default: 1'
    )
    
    # Auto-restart mode arguments
    parser.add_argument(
        '--auto_restart',
        action='store_true',
        help='Enable auto-restart mode: runs multiple sessions with cooldowns, deduplicates results'
    )
    parser.add_argument(
        '--max_sessions',
        type=int,
        default=50,
        help='Maximum number of sessions in auto-restart mode. Default: 50'
    )
    parser.add_argument(
        '--session_cooldown',
        type=int,
        default=600,
        help='Seconds to wait between sessions in auto-restart mode. Default: 600 (10 minutes)'
    )
    parser.add_argument(
        '--target_listings',
        type=int,
        default=1000,
        help='Stop auto-restart when this many listings are collected. Default: 1000'
    )
    parser.add_argument(
        '--turbo',
        action='store_true',
        help='🚀 TURBO MODE: Use faster delays for quicker scraping (higher risk of detection). Good when you need data fast.'
    )
    
    # Direct URL scraping mode
    parser.add_argument(
        '--scrape_urls',
        action='store_true',
        help='Scrape specific URLs directly (uses KNOWN_SCRAPED_URLS list or --url_file)'
    )
    parser.add_argument(
        '--url_file',
        type=str,
        default=None,
        help='Path to a text file containing URLs to scrape (one per line)'
    )
    
    return parser.parse_args()


async def scrape_direct_urls(urls: List[str], headless: bool = True) -> int:
    """
    Directly scrape a list of specific building URLs.
    
    This allows re-scraping specific buildings without searching for them.
    
    Args:
        urls: List of building URLs to scrape
        headless: Run browser in headless mode
    
    Returns:
        Number of units scraped
    """
    logger.info("="*80)
    logger.info("DIRECT URL SCRAPING MODE")
    logger.info("="*80)
    logger.info(f"URLs to scrape: {len(urls)}")
    logger.info(f"Headless mode: {headless}")
    
    all_units: List[UnitListing] = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-http2',
            ]
        )
        
        selected_user_agent = random.choice(USER_AGENTS)
        logger.info(f"Using user agent: {selected_user_agent[:50]}...")
        
        context = await browser.new_context(
            user_agent=selected_user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/Chicago',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
        )
        
        page = await context.new_page()
        
        try:
            for idx, url in enumerate(urls, 1):
                logger.info(f"Processing URL {idx}/{len(urls)}: {url}")
                try:
                    units = await scrape_building(page, url)
                    if units:
                        all_units.extend(units)
                        logger.info(f"  ✓ Got {len(units)} units")
                    else:
                        logger.warning(f"  ✗ No units found")
                except Exception as e:
                    logger.error(f"  ✗ Failed: {e}")
                
                # Delay between buildings
                if idx < len(urls):
                    delay = get_random_delay()
                    logger.info(f"Waiting {delay:.1f}s before next URL...")
                    await asyncio.sleep(delay)
        finally:
            await browser.close()
    
    # Save and process results
    logger.info("Saving results...")
    export_to_csv(all_units, OUTPUT_CSV_ALL)
    
    # Load existing and merge
    existing_listings = load_existing_listings(PERSISTENT_CSV)
    existing_ids = set(existing_listings.keys())
    
    filtered_units = geocode_and_filter_units(all_units, existing_ids)
    export_to_csv(filtered_units, OUTPUT_CSV)
    
    merged_units = merge_and_dedupe_units(filtered_units, existing_listings)
    export_combined_csv(merged_units, PERSISTENT_CSV)
    
    logger.info("="*80)
    logger.info("DIRECT URL SCRAPING COMPLETE")
    logger.info("="*80)
    logger.info(f"URLs processed: {len(urls)}")
    logger.info(f"Units scraped: {len(all_units)}")
    logger.info(f"Units within radius: {len(filtered_units)}")
    logger.info(f"Total accumulated: {len(merged_units)}")
    logger.info(f"Combined data: {PERSISTENT_CSV}")
    
    return len(all_units)


if __name__ == "__main__":
    args = parse_args()
    
    # Apply turbo mode if requested (affects global delay settings)
    if args.turbo:
        PAGE_DELAY_SECONDS = TURBO_PAGE_DELAY
        logger.info("🚀 TURBO MODE ENABLED - Using faster delays for quicker scraping")
    
    if args.scrape_urls:
        # Direct URL scraping mode
        urls_to_scrape = []
        
        if args.url_file:
            # Load URLs from file
            try:
                with open(args.url_file, 'r') as f:
                    urls_to_scrape = [line.strip() for line in f if line.strip() and line.strip().startswith('http')]
                logger.info(f"Loaded {len(urls_to_scrape)} URLs from {args.url_file}")
            except Exception as e:
                logger.error(f"Error loading URL file: {e}")
                sys.exit(1)
        else:
            # Use the known scraped URLs list
            urls_to_scrape = list(KNOWN_SCRAPED_URLS)
            logger.info(f"Using {len(urls_to_scrape)} known URLs from KNOWN_SCRAPED_URLS")
        
        if not urls_to_scrape:
            logger.error("No URLs to scrape. Use --url_file to specify a file with URLs.")
            sys.exit(1)
        
        asyncio.run(scrape_direct_urls(
            urls=urls_to_scrape,
            headless=args.headless
        ))
    elif args.auto_restart:
        # Auto-restart mode
        asyncio.run(auto_restart_scraper(
            headless=args.headless,
            max_search_pages=args.max_search_pages,
            max_buildings=args.max_buildings or 100,  # Increased from 50 to 100 per session
            max_sessions=args.max_sessions,
            session_cooldown=args.session_cooldown,
            target_listings=args.target_listings,
            turbo=args.turbo
        ))
    else:
        # Single session mode
        asyncio.run(main(
            headless=args.headless,
            max_search_pages=args.max_search_pages,
            max_buildings=args.max_buildings,
            start_page=args.start_page
        ))