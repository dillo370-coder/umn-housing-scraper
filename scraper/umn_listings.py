"""
UMN Listings (listings.umn.edu) Web Scraper

Scrapes the official University of Minnesota Off-Campus Housing Marketplace
(powered by Rent College Pads) for student housing listings.

Usage:
  python3 -m scraper.umn_listings --headless=False  # test run with visible browser
  python3 -m scraper.umn_listings --headless=True   # full headless run
"""
import argparse
import asyncio
import csv
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import List, Optional, Dict, Any, Set

from playwright.async_api import async_playwright, Page
import requests


# ============================================================================
# CONFIGURATION
# ============================================================================

UMN_CAMPUS_LAT = 44.9731
UMN_CAMPUS_LON = -93.2359
SEARCH_RADIUS_KM = 10.0  # 10 km radius

BASE_URL = "https://listings.umn.edu"
LISTING_PAGE = f"{BASE_URL}/listing"

# Rate limiting settings - more conservative for university site
PAGE_DELAY_SECONDS = 3.0
PAGE_DELAY_VARIANCE = 2.0
GEOCODE_DELAY_SECONDS = 1.0

# Scroll settings
SCROLL_DELAY_MIN = 0.5
SCROLL_DELAY_MAX = 1.5

# User agents
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

# Output
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = OUTPUT_DIR / f"umn_listings_data_{TIMESTAMP}.csv"
LOG_FILE = OUTPUT_DIR / f"umn_listings_log_{TIMESTAMP}.log"

# Persistent output file for accumulating results
PERSISTENT_CSV = OUTPUT_DIR / "umn_listings_combined.csv"


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
# DATA STRUCTURES (same format as apartments.com scraper for consistency)
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
    
    # UMN-specific fields
    available_date: str = ""
    lease_term: str = ""
    property_manager: str = ""

    scrape_date: str = field(default_factory=lambda: datetime.now().isoformat())
    source_url: str = ""


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in km."""
    R = 6371  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


def get_random_delay() -> float:
    """Get a randomized delay to avoid detection patterns."""
    return PAGE_DELAY_SECONDS + random.uniform(0, PAGE_DELAY_VARIANCE)


def geocode_address(address: str) -> Optional[Dict[str, float]]:
    """Use Nominatim to geocode an address."""
    try:
        time.sleep(GEOCODE_DELAY_SECONDS)  # Rate limit
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": address,
                "format": "json",
                "limit": 1,
                "countrycodes": "us"
            },
            headers={"User-Agent": "UMNHousingResearch/1.0"},
            timeout=10
        )
        resp.raise_for_status()  # Check for HTTP errors
        data = resp.json()
        if data:
            return {"lat": float(data[0]["lat"]), "lon": float(data[0]["lon"])}
    except Exception as e:
        logger.warning(f"Geocoding failed for {address}: {e}")
    return None


def parse_rent(rent_text: str) -> tuple:
    """Parse rent string to extract min/max values."""
    if not rent_text:
        return None, None, "unknown"
    
    rent_text = rent_text.replace(',', '').replace('$', '').strip()
    
    # Check if it's per bed pricing
    if 'bed' in rent_text.lower() or '/bed' in rent_text.lower():
        price_type = "per_bed"
    else:
        price_type = "total"
    
    # Extract numbers
    numbers = re.findall(r'\d+\.?\d*', rent_text)
    if not numbers:
        return None, None, price_type
    
    numbers = [float(n) for n in numbers]
    if len(numbers) == 1:
        return numbers[0], numbers[0], price_type
    else:
        return min(numbers), max(numbers), price_type


def parse_beds_baths(text: str) -> tuple:
    """Parse bedroom/bathroom text."""
    beds, baths = None, None
    
    # Look for beds
    bed_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bed|br|bedroom)', text.lower())
    if bed_match:
        beds = float(bed_match.group(1))
    elif 'studio' in text.lower():
        beds = 0
    
    # Look for baths
    bath_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba|bathroom)', text.lower())
    if bath_match:
        baths = float(bath_match.group(1))
    
    return beds, baths


# ============================================================================
# UMN LISTINGS SCRAPER
# ============================================================================

async def simulate_human_scrolling(page: Page):
    """Scroll through the page like a human would."""
    try:
        # Get page height
        height = await page.evaluate("document.body.scrollHeight")
        viewport_height = 1080
        
        current_pos = 0
        while current_pos < height:
            scroll_amount = random.randint(300, 600)
            current_pos += scroll_amount
            await page.evaluate(f"window.scrollTo(0, {current_pos})")
            await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
    except Exception as e:
        logger.debug(f"Scroll error: {e}")


async def extract_listing_urls(page: Page) -> List[str]:
    """
    Extract listing URLs from the UMN listings page.
    
    The listings.umn.edu site is powered by Rent College Pads and displays
    listings in a grid/list format with clickable cards.
    """
    urls = set()
    
    try:
        # Wait for listings to load
        await page.wait_for_load_state("networkidle", timeout=15000)
        await asyncio.sleep(2)  # Extra wait for dynamic content
        
        # Scroll to load all content
        await simulate_human_scrolling(page)
        
        # Try multiple selectors for listing links
        selectors = [
            'a[href*="/listing/"]',
            '.listing-card a',
            '.property-card a',
            'a.listing-link',
            '[data-listing-id] a',
            '.search-results a',
            'article a',
            '.card a',
        ]
        
        for selector in selectors:
            try:
                links = await page.query_selector_all(selector)
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        # Make absolute URL if needed
                        if href.startswith('/'):
                            href = BASE_URL + href
                        # Only include listing URLs
                        if '/listing/' in href and href.startswith(BASE_URL):
                            urls.add(href)
            except Exception:
                continue
        
        logger.info(f"Found {len(urls)} listing URLs")
        
    except Exception as e:
        logger.error(f"Error extracting listing URLs: {e}")
    
    return list(urls)


async def scrape_listing(page: Page, url: str) -> Optional[UnitListing]:
    """Scrape a single listing from listings.umn.edu."""
    logger.info(f"Scraping listing: {url}")
    
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        
        # Check for errors
        content = await page.content()
        if 'Page not found' in content or '404' in content:
            logger.warning(f"Listing not found: {url}")
            return None
        
        # Generate listing ID from URL (handle trailing slash)
        url_parts = [part for part in url.rstrip('/').split('/') if part]
        listing_id = f"umn_{url_parts[-1]}" if url_parts else f"umn_{hash(url)}"
        
        # Try to extract listing data
        listing = UnitListing(
            listing_id=listing_id,
            building_name="",
            full_address="",
            source_url=url,
            is_student_branded=True,  # UMN listings are student-focused
        )
        
        # Extract property name
        name_selectors = [
            'h1.property-name', 'h1.listing-title', 'h1',
            '.property-name', '.listing-name', '.title'
        ]
        for sel in name_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    listing.building_name = (await el.text_content()).strip()
                    break
            except Exception:
                continue
        
        # Extract address
        address_selectors = [
            '.address', '.property-address', '.listing-address',
            '[data-address]', '.location'
        ]
        for sel in address_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    listing.full_address = (await el.text_content()).strip()
                    # Parse address components
                    parts = listing.full_address.split(',')
                    if len(parts) >= 1:
                        listing.street = parts[0].strip()
                    if len(parts) >= 2:
                        listing.city = parts[1].strip()
                    if len(parts) >= 3:
                        state_zip = parts[2].strip().split()
                        if len(state_zip) >= 1:
                            listing.state = state_zip[0]
                        if len(state_zip) >= 2:
                            listing.zip = state_zip[1]
                    break
            except Exception:
                continue
        
        # Extract rent
        rent_selectors = [
            '.price', '.rent', '.listing-price', '.cost',
            '[data-price]', '.amount'
        ]
        for sel in rent_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    listing.rent_raw = (await el.text_content()).strip()
                    listing.rent_min, listing.rent_max, listing.price_type = parse_rent(listing.rent_raw)
                    listing.is_per_bed = listing.price_type == "per_bed"
                    break
            except Exception:
                continue
        
        # Extract beds/baths
        bed_bath_selectors = [
            '.beds-baths', '.bed-bath', '.details',
            '.listing-details', '.property-details'
        ]
        for sel in bed_bath_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.text_content()).strip()
                    listing.beds, listing.baths = parse_beds_baths(text)
                    break
            except Exception:
                continue
        
        # Extract sqft
        sqft_selectors = ['.sqft', '.square-feet', '.size']
        for sel in sqft_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.text_content()).strip()
                    sqft_match = re.search(r'(\d+(?:,\d+)?)\s*(?:sq|sqft|sf)', text.lower())
                    if sqft_match:
                        listing.sqft = int(sqft_match.group(1).replace(',', ''))
                    break
            except Exception:
                continue
        
        # Extract available date
        date_selectors = [
            '.available-date', '.availability', '.move-in',
            '[data-available]', '.date'
        ]
        for sel in date_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    listing.available_date = (await el.text_content()).strip()
                    break
            except Exception:
                continue
        
        # Extract amenities by looking for common keywords in page content
        page_text = content.lower()
        listing.has_in_unit_laundry = 'in-unit' in page_text and 'laundry' in page_text
        listing.has_on_site_laundry = 'on-site laundry' in page_text or 'laundry room' in page_text
        listing.has_dishwasher = 'dishwasher' in page_text
        listing.has_ac = 'air condition' in page_text or ' a/c' in page_text or 'central air' in page_text
        listing.has_heat_included = 'heat included' in page_text
        listing.has_water_included = 'water included' in page_text
        listing.has_internet_included = 'internet included' in page_text or 'wifi included' in page_text
        listing.is_furnished = 'furnished' in page_text and 'unfurnished' not in page_text
        listing.has_gym = 'gym' in page_text or 'fitness' in page_text
        listing.has_pool = 'pool' in page_text
        listing.has_parking_available = 'parking' in page_text
        listing.has_garage = 'garage' in page_text
        listing.pets_allowed = 'pet friendly' in page_text or 'pets allowed' in page_text
        
        # Extract property manager
        manager_selectors = [
            '.property-manager', '.landlord', '.management',
            '.contact-name', '.owner'
        ]
        for sel in manager_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    listing.property_manager = (await el.text_content()).strip()
                    break
            except Exception:
                continue
        
        logger.info(f"  ✓ Scraped: {listing.building_name or 'Unknown'} - ${listing.rent_raw}")
        return listing
        
    except Exception as e:
        logger.error(f"Error scraping listing {url}: {e}")
        return None


async def load_more_listings(page: Page) -> bool:
    """Click 'Load More' or 'Show More' button if available."""
    load_more_selectors = [
        'button:has-text("Load More")',
        'button:has-text("Show More")',
        'a:has-text("Load More")',
        '.load-more',
        '.show-more',
        '[data-load-more]',
    ]
    
    for selector in load_more_selectors:
        try:
            button = await page.query_selector(selector)
            if button:
                await button.click()
                await asyncio.sleep(2)
                return True
        except Exception:
            continue
    
    return False


def geocode_and_filter_units(units: List[UnitListing]) -> List[UnitListing]:
    """Geocode units and filter to those within search radius."""
    # Group by address
    by_address = {}
    for unit in units:
        if unit.full_address:
            if unit.full_address not in by_address:
                by_address[unit.full_address] = []
            by_address[unit.full_address].append(unit)
    
    # Geocode each unique address
    for address, address_units in by_address.items():
        if not address:
            continue
        if address_units[0].lat is None or address_units[0].lon is None:
            logger.info(f"Geocoding: {address}")
            coords = geocode_address(address)
            if coords:
                logger.info(f"  ✓ Found: {coords['lat']:.4f}, {coords['lon']:.4f}")
                for unit in address_units:
                    unit.lat = coords['lat']
                    unit.lon = coords['lon']
    
    # Filter by distance
    filtered = []
    for unit in units:
        if unit.lat is not None and unit.lon is not None:
            dist = haversine_distance(unit.lat, unit.lon, UMN_CAMPUS_LAT, UMN_CAMPUS_LON)
            unit.dist_to_campus_km = round(dist, 2)
            if dist <= SEARCH_RADIUS_KM:
                filtered.append(unit)
                logger.info(f"{unit.building_name}: {dist:.2f} km from UMN ✓ INCLUDED")
            else:
                logger.info(f"{unit.building_name}: {dist:.2f} km from UMN ✗ EXCLUDED (>{SEARCH_RADIUS_KM}km)")
        else:
            # Include units we couldn't geocode but are in Minneapolis area
            if unit.city and 'minneapolis' in unit.city.lower():
                filtered.append(unit)
                logger.warning(f"Could not geocode but including Minneapolis listing: {unit.building_name}")
    
    logger.info(f"Filtered to {len(filtered)} units within {SEARCH_RADIUS_KM} km of UMN")
    return filtered


def load_existing_listings(csv_path: Path) -> Dict[str, Any]:
    """Load existing listings from CSV file as dictionaries."""
    existing: Dict[str, Any] = {}
    if csv_path.exists():
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    listing_id = row.get('listing_id', '')
                    if listing_id:
                        existing[listing_id] = row
            logger.info(f"Loaded {len(existing)} existing listings from {csv_path}")
        except Exception as e:
            logger.warning(f"Could not load existing listings: {e}")
    return existing


def export_to_csv(units: List[UnitListing], filename: Path):
    """Export unit listings to CSV file."""
    fieldnames = list(UnitListing.__dataclass_fields__.keys())
    logger.info(f"Exporting {len(units)} units to {filename}")
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if units:
            rows = [asdict(u) for u in units]
            writer.writerows(rows)
    logger.info(f"Export complete: {filename}")


def merge_and_export(new_units: List[UnitListing], existing: Dict[str, Any], output_path: Path):
    """Merge new units with existing and export."""
    # Convert new units to dict format
    for unit in new_units:
        if unit.listing_id not in existing:
            existing[unit.listing_id] = asdict(unit)
    
    # Export all
    if existing:
        fieldnames = list(UnitListing.__dataclass_fields__.keys())
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing.values())
        logger.info(f"Merged {len(existing)} total listings to {output_path}")


async def main(headless: bool = True, max_listings: int = None) -> int:
    """
    Main scraping function for listings.umn.edu.
    
    Args:
        headless: Run browser in headless mode
        max_listings: Max listings to scrape (None = unlimited)
    
    Returns:
        Number of units scraped in this session
    """
    logger.info("="*80)
    logger.info("UMN LISTINGS SCRAPER STARTED")
    logger.info("="*80)
    logger.info(f"Target site: {LISTING_PAGE}")
    logger.info(f"Search radius: {SEARCH_RADIUS_KM} km from UMN campus")
    logger.info(f"Headless mode: {headless}")
    logger.info(f"Max listings: {max_listings if max_listings else 'unlimited'}")
    logger.info(f"Output file: {OUTPUT_CSV}")

    all_units: List[UnitListing] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )

        selected_user_agent = random.choice(USER_AGENTS)
        logger.info(f"Using user agent: {selected_user_agent[:50]}...")

        context = await browser.new_context(
            user_agent=selected_user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/Chicago',
        )

        page = await context.new_page()

        try:
            # Navigate to listings page
            logger.info(f"Navigating to: {LISTING_PAGE}")
            await page.goto(LISTING_PAGE, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)
            
            # Try to load all listings by clicking "Load More" multiple times
            load_attempts = 0
            max_load_attempts = 20  # Limit to prevent infinite loop
            while load_attempts < max_load_attempts:
                if await load_more_listings(page):
                    load_attempts += 1
                    logger.info(f"Loaded more listings (attempt {load_attempts})")
                else:
                    break
            
            # Extract all listing URLs
            listing_urls = await extract_listing_urls(page)
            logger.info(f"Found {len(listing_urls)} listings to scrape")
            
            if max_listings:
                listing_urls = listing_urls[:max_listings]
                logger.info(f"Limited to {max_listings} listings")
            
            # Scrape each listing
            for idx, url in enumerate(listing_urls, 1):
                logger.info(f"Processing listing {idx}/{len(listing_urls)}")
                try:
                    unit = await scrape_listing(page, url)
                    if unit:
                        all_units.append(unit)
                        logger.info(f"Total units collected: {len(all_units)}")
                except Exception as e:
                    logger.error(f"Failed to scrape {url}: {e}")
                
                # Random delay between listings
                delay = get_random_delay()
                await asyncio.sleep(delay)

        finally:
            await browser.close()

    # Geocode and filter
    logger.info("Geocoding and filtering listings...")
    filtered_units = geocode_and_filter_units(all_units)
    
    # Export session data
    export_to_csv(filtered_units, OUTPUT_CSV)
    
    # Merge with existing data
    existing = load_existing_listings(PERSISTENT_CSV)
    merge_and_export(filtered_units, existing, PERSISTENT_CSV)

    logger.info("="*80)
    logger.info("SCRAPING COMPLETE")
    logger.info("="*80)
    logger.info(f"This session scraped: {len(all_units)} units")
    logger.info(f"Units within {SEARCH_RADIUS_KM} km: {len(filtered_units)}")
    logger.info(f"Session data: {OUTPUT_CSV}")
    logger.info(f"Combined data: {PERSISTENT_CSV}")
    logger.info(f"Log: {LOG_FILE}")
    
    return len(all_units)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='UMN Listings (listings.umn.edu) Web Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test run (visible browser)
  python3 -m scraper.umn_listings --headless=False

  # Full headless run
  python3 -m scraper.umn_listings --headless=True

  # Limit number of listings
  python3 -m scraper.umn_listings --headless=False --max_listings=10
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
        '--max_listings',
        type=int,
        default=None,
        help='Maximum number of listings to scrape. Default: unlimited'
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(
        headless=args.headless,
        max_listings=args.max_listings
    ))
