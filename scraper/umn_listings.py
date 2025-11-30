"""
UMN Listings (listings.umn.edu) Web Scraper

Scrapes the official University of Minnesota Off-Campus Housing Marketplace
(powered by Rent College Pads) for student housing listings.

SITE STRUCTURE (as documented):
- Home page: https://listings.umn.edu/listing
- Heavy JavaScript - requires headless browser (Playwright)
- Filters at top: Beds, Baths, Price (Per Unit vs. Per Person)
- Neighborhood tabs: Dinkytown, Marcy-Holmes, Como, Prospect Park
- Listings grid: Cards displayed as <div>, no traditional <a> links
  - Clicking a card triggers JavaScript to load property details
  - Cards show: image, name, price range, bed range, address, availability, walk time
- Property modal: URL becomes ?property=<id>
  - Shows: property name, full address, contact info (phone/email)
  - Unit table: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
- Infinite scroll: Need to scroll to load more listings
- No lat/lon exposed: Must geocode addresses ourselves

Usage:
  python3 -m scraper.umn_listings --headless=False  # test run with visible browser
  python3 -m scraper.umn_listings --headless=True   # full headless run
  python3 -m scraper.umn_listings --neighborhood=dinkytown  # filter by neighborhood
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
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout
import requests


# ============================================================================
# CONFIGURATION
# ============================================================================

UMN_CAMPUS_LAT = 44.9731
UMN_CAMPUS_LON = -93.2359
SEARCH_RADIUS_KM = 10.0  # 10 km radius

BASE_URL = "https://listings.umn.edu"
LISTING_PAGE = f"{BASE_URL}/listing"

# Neighborhood tabs available on the site
NEIGHBORHOODS = [
    "dinkytown",
    "marcy-holmes",
    "como",
    "prospect-park",
]

# Rate limiting settings - faster scraping while avoiding detection
PAGE_DELAY_SECONDS = 2.0
PAGE_DELAY_VARIANCE = 1.5
MODAL_WAIT_SECONDS = 1.5  # Wait for modal to render
GEOCODE_DELAY_SECONDS = 1.0

# Navigation settings
NAV_TIMEOUT = 60000  # 60 seconds for page load
MODAL_TIMEOUT = 10000  # 10 seconds for modal to appear
RETRY_ATTEMPTS = 3
RETRY_DELAY = 3

# Scroll settings for infinite scroll
SCROLL_DELAY_MIN = 1.5
SCROLL_DELAY_MAX = 3.0
MAX_SCROLL_ATTEMPTS = 100  # Maximum scroll iterations to find all listings
MAX_NO_CHANGE_SCROLLS = 5  # Number of scrolls without change before stopping

# Content loading delay
CONTENT_LOAD_DELAY = 1.0  # Seconds to wait for dynamic content to load

# Address extraction patterns
STREET_INDICATORS = ['ave', 'st', 'street', 'avenue', 'rd', 'road', 'drive', 'dr', 'blvd', 'boulevard', 'lane', 'ln', 'way', 'ct', 'court']
SKIP_TEXT_TERMS = ['menu', 'home', 'search', 'login', 'sign', 'navigation', 'footer', 'header']

# Pre-compiled regex patterns for performance
ADDRESS_PATTERNS = [
    re.compile(r'(\d+\s+[\w\s]+(?:Ave|St|Street|Avenue|Rd|Road|Drive|Dr|Blvd|Boulevard|Lane|Ln|Way|Ct|Court)[^,]*,\s*(?:Minneapolis|St\.?\s*Paul)[^,]*,\s*MN\s*\d{5})', re.IGNORECASE),
    re.compile(r'(\d+\s+[\w\s]+,\s*(?:Minneapolis|St\.?\s*Paul),?\s*MN\s*\d{5})', re.IGNORECASE),
]
PRICE_PATTERN = re.compile(r'\$[\d,]+(?:\s*[-–]\s*\$?[\d,]+)?(?:\s*/\s*(?:month|mo|bed))?')
BED_PATTERN = re.compile(r'(\d+)\s*(?:bed|br|bedroom)', re.IGNORECASE)
BATH_PATTERN = re.compile(r'(\d+(?:\.\d+)?)\s*(?:bath|ba|bathroom)', re.IGNORECASE)

# User agents - more variety helps avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
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
    """Data structure matching the CSV columns specified in the PDF notes."""
    listing_id: str  # property ID from the URL (?property=<id>)
    building_name: str
    full_address: str
    street: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    lat: Optional[float] = None
    lon: Optional[float] = None
    dist_to_campus_km: Optional[float] = None  # Calculated via haversine from geocoded address

    unit_label: str = ""  # Unit identifier from unit table "Name" column
    beds: Optional[float] = None  # From unit table "Beds" column
    baths: Optional[float] = None  # From unit table "Baths" column
    sqft: Optional[int] = None  # From unit table "Sq.Ft" column (size_sqft)
    rent_raw: str = ""  # Raw price text from unit table "Price From" column
    rent_min: Optional[float] = None  # Parsed from rent_raw (same as rent_raw for single price)
    rent_max: Optional[float] = None  # Parsed from rent_raw (same as rent_raw for single price)
    price_type: str = "unknown"  # "per_bed" or "per_unit"
    is_per_bed: Optional[bool] = None  # Determined from price filter or text
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
    
    # UMN-specific fields from the modal
    available_date: str = ""  # From unit table "Available" column
    walk_time_to_campus: str = ""  # "Walk Time To Campus" from card
    has_virtual_tour: Optional[bool] = None  # From unit table "Tour" column
    
    # Contact info from modal
    landlord_phone: str = ""
    landlord_email: str = ""
    property_manager: str = ""
    
    # Metadata
    neighborhood: str = ""  # Which tab: dinkytown, marcy-holmes, como, prospect-park
    listing_url: str = ""  # Full URL with ?property=<id>
    scrape_date: str = field(default_factory=lambda: datetime.now().isoformat())
    source_url: str = ""  # Kept for compatibility


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


def parse_beds(text: str) -> Optional[float]:
    """Parse bedroom count from text."""
    if not text:
        return None
    text_lower = text.lower().strip()
    if 'studio' in text_lower:
        return 0.0
    match = re.search(r'(\d+(?:\.\d+)?)', text_lower)
    if match:
        return float(match.group(1))
    return None


def parse_baths(text: str) -> Optional[float]:
    """Parse bathroom count from text."""
    if not text:
        return None
    match = re.search(r'(\d+(?:\.\d+)?)', text.lower())
    if match:
        return float(match.group(1))
    return None


def parse_sqft(text: str) -> Optional[int]:
    """Parse square footage from text."""
    if not text:
        return None
    # Remove commas and extract number
    text_clean = text.replace(',', '')
    match = re.search(r'(\d+)', text_clean)
    if match:
        return int(match.group(1))
    return None


# ============================================================================
# UMN LISTINGS SCRAPER - Handles JavaScript-heavy site with modal-based details
# ============================================================================

async def simulate_human_scrolling(page: Page):
    """Scroll through the page like a human would."""
    try:
        # Get page height
        height = await page.evaluate("document.body.scrollHeight")
        
        current_pos = 0
        while current_pos < height:
            scroll_amount = random.randint(300, 600)
            current_pos += scroll_amount
            await page.evaluate(f"window.scrollTo(0, {current_pos})")
            await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
    except Exception as e:
        logger.debug(f"Scroll error: {e}")


async def scroll_to_load_all_listings(page: Page) -> int:
    """
    Scroll to bottom repeatedly to trigger infinite scroll and load all listings.
    Returns the number of listing cards found.
    """
    previous_count = 0
    no_change_count = 0
    
    # First, scroll to bottom to trigger initial load
    logger.info("Starting infinite scroll to load all listings...")
    
    for scroll_attempt in range(MAX_SCROLL_ATTEMPTS):
        # Get current scroll height
        scroll_height = await page.evaluate("document.body.scrollHeight")
        
        # Scroll to bottom in steps for more realistic behavior
        current_scroll = await page.evaluate("window.pageYOffset")
        step_size = random.randint(500, 1000)
        
        while current_scroll < scroll_height:
            current_scroll += step_size
            await page.evaluate(f"window.scrollTo(0, {current_scroll})")
            await asyncio.sleep(random.uniform(0.3, 0.6))
        
        # Final scroll to absolute bottom
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))
        
        # Count current cards - try multiple selectors
        card_selectors = [
            '[data-property-id]',  # Most specific
            '.listing-card',
            '.property-card',
            'div[class*="ListingCard"]',
            'div[class*="listing-card"]',
            'div[class*="property-card"]',
            'article',
        ]
        
        current_count = 0
        for selector in card_selectors:
            try:
                cards = await page.query_selector_all(selector)
                if cards and len(cards) > current_count:
                    current_count = len(cards)
            except Exception:
                continue
        
        logger.debug(f"Scroll {scroll_attempt + 1}: Found {current_count} cards")
        
        if current_count == previous_count:
            no_change_count += 1
            if no_change_count >= MAX_NO_CHANGE_SCROLLS:
                # No new cards after several scrolls, we've loaded everything
                logger.info(f"Finished scrolling after {scroll_attempt + 1} attempts. Total cards found: {current_count}")
                break
        else:
            no_change_count = 0
            previous_count = current_count
            logger.info(f"Scroll {scroll_attempt + 1}: Loaded {current_count} cards so far...")
    
    # Scroll back to top to reset view
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(1)
    
    return previous_count


async def click_neighborhood_tab(page: Page, neighborhood: str) -> bool:
    """
    Click on a neighborhood tab to filter listings.
    Neighborhood tabs trigger AJAX calls to update the listings grid.
    """
    try:
        # Normalize neighborhood name for display text matching
        neighborhood_display = neighborhood.replace('-', ' ').title()
        
        # Try data attribute selector first (most reliable)
        tab = await page.query_selector(f'[data-neighborhood="{neighborhood}"]')
        if tab:
            await tab.click()
            await asyncio.sleep(2)
            logger.info(f"Clicked neighborhood tab: {neighborhood}")
            return True
        
        # Try using Playwright's get_by_role and get_by_text for better reliability
        try:
            tab = page.get_by_role("tab", name=re.compile(neighborhood_display, re.IGNORECASE))
            if await tab.count() > 0:
                await tab.first.click()
                await asyncio.sleep(2)
                logger.info(f"Clicked neighborhood tab: {neighborhood}")
                return True
        except Exception:
            pass
        
        # Try using get_by_text
        try:
            tab = page.get_by_text(re.compile(neighborhood_display, re.IGNORECASE))
            if await tab.count() > 0:
                await tab.first.click()
                await asyncio.sleep(2)
                logger.info(f"Clicked neighborhood tab: {neighborhood}")
                return True
        except Exception:
            pass
        
        # Fallback: Search all clickable elements by text content
        clickables = await page.query_selector_all('button, a, [role="tab"], [class*="tab"]')
        for el in clickables:
            try:
                text = (await el.text_content() or "").lower()
                if neighborhood.replace('-', ' ') in text or neighborhood.replace('-', '') in text:
                    await el.click()
                    await asyncio.sleep(2)
                    logger.info(f"Clicked neighborhood tab: {neighborhood}")
                    return True
            except Exception:
                continue
        
        logger.warning(f"Could not find tab for neighborhood: {neighborhood}")
        return False
    except Exception as e:
        logger.error(f"Error clicking neighborhood tab: {e}")
        return False


async def get_property_ids_from_cards(page: Page) -> List[str]:
    """
    Extract property IDs from listing cards.
    Cards don't use <a> links - they use JavaScript click handlers.
    The property ID is used in the URL as ?property=<id>.
    """
    property_ids = []
    
    try:
        # Try to find cards with data attributes containing property IDs
        card_selectors = [
            '[data-property-id]',
            '[data-listing-id]',
            '[data-id]',
            'div[class*="listing"][id]',
            'div[class*="property"][id]',
        ]
        
        for selector in card_selectors:
            try:
                cards = await page.query_selector_all(selector)
                for card in cards:
                    # Try different attribute names for the ID
                    for attr in ['data-property-id', 'data-listing-id', 'data-id', 'id']:
                        try:
                            prop_id = await card.get_attribute(attr)
                            if prop_id and prop_id not in property_ids:
                                property_ids.append(prop_id)
                                break
                        except Exception:
                            continue
            except Exception:
                continue
        
        if not property_ids:
            # Fallback: try clicking cards and capturing URL changes
            logger.warning("No property IDs found via data attributes, will try click-based extraction")
        
        logger.info(f"Found {len(property_ids)} property IDs from cards")
        
    except Exception as e:
        logger.error(f"Error extracting property IDs: {e}")
    
    return property_ids


async def click_card_and_get_property_id(page: Page, card_index: int) -> Optional[str]:
    """
    Click on a listing card by index and extract the property ID from the URL.
    The site appends ?property=<id> to the URL when a modal opens.
    """
    try:
        # Get all cards
        card_selectors = [
            'div[class*="listing"]:not([class*="grid"])',
            'div[class*="property"]:not([class*="grid"])',
            'article',
            '.card',
        ]
        
        cards = []
        for selector in card_selectors:
            try:
                cards = await page.query_selector_all(selector)
                if cards and len(cards) > card_index:
                    break
            except Exception:
                continue
        
        if not cards or len(cards) <= card_index:
            return None
        
        card = cards[card_index]
        
        # Click the card
        await card.click()
        await asyncio.sleep(MODAL_WAIT_SECONDS)
        
        # Get URL and extract property ID
        current_url = page.url
        if '?property=' in current_url or '&property=' in current_url:
            parsed = urlparse(current_url)
            params = parse_qs(parsed.query)
            if 'property' in params:
                return params['property'][0]
        
        return None
        
    except Exception as e:
        logger.debug(f"Error clicking card {card_index}: {e}")
        return None


async def open_property_modal(page: Page, property_id: str) -> bool:
    """
    Open the property detail modal by navigating to ?property=<id>.
    The modal overlay contains full property details.
    """
    try:
        # Construct URL with property parameter
        target_url = f"{LISTING_PAGE}?property={property_id}"
        
        # Navigate to the property modal URL
        await page.goto(target_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        await asyncio.sleep(MODAL_WAIT_SECONDS)
        
        # Wait for modal to appear - try various selectors
        modal_selectors = [
            '[class*="modal"]',
            '[role="dialog"]',
            '.property-detail',
            '.property-modal',
            '[class*="overlay"]',
            '[class*="detail"]',
        ]
        
        for selector in modal_selectors:
            try:
                modal = await page.wait_for_selector(selector, timeout=MODAL_TIMEOUT)
                if modal:
                    logger.debug(f"Modal opened for property {property_id}")
                    return True
            except PlaywrightTimeout:
                continue
            except Exception:
                continue
        
        # Even if we don't find a specific modal selector, the page may have loaded the detail view
        # Check if we're on the property page
        if property_id in page.url:
            logger.debug(f"Property page loaded for {property_id}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error opening property modal for {property_id}: {e}")
        return False


async def close_property_modal(page: Page) -> bool:
    """
    Close the property detail modal so we can click the next card.
    """
    try:
        original_url = page.url
        
        # Try various close methods
        close_selectors = [
            'button[aria-label="Close"]',
            '[class*="close"]',
            'button:has-text("×")',
            'button:has-text("Close")',
            '.modal-close',
            '[data-dismiss="modal"]',
        ]
        
        for selector in close_selectors:
            try:
                close_btn = await page.query_selector(selector)
                if close_btn:
                    await close_btn.click()
                    await asyncio.sleep(0.5)
                    # Check if we're back to the listing page
                    if '?property=' not in page.url:
                        return True
            except Exception:
                continue
        
        # Alternative: press Escape key
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.5)
        if '?property=' not in page.url:
            return True
        
        # Alternative: navigate back to listing page without property param
        if '?property=' in page.url:
            try:
                await page.goto(LISTING_PAGE, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                await asyncio.sleep(1)
                # Verify navigation was successful
                if page.url.startswith(BASE_URL) and '?property=' not in page.url:
                    return True
                else:
                    logger.warning("Navigation away from modal may have failed")
                    return False
            except Exception as nav_error:
                logger.warning(f"Navigation error while closing modal: {nav_error}")
                return False
        
        return True
        
    except Exception as e:
        logger.debug(f"Error closing modal: {e}")
        return False


async def extract_property_from_modal(page: Page, property_id: str) -> List[UnitListing]:
    """
    Extract property details from the modal/detail view.
    
    Modal contains:
    - Property name & full address (street, city, state, ZIP)
    - Contact info (phone, email)
    - Unit table with columns: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
    - Bed-range and price-range summary at top
    - Amenities section with Property Highlights, Unit Features, Property Features
    """
    units = []
    
    try:
        # Wait for content to load
        await asyncio.sleep(CONTENT_LOAD_DELAY)
        
        # Get full page content for amenities extraction
        page_content = await page.content()
        page_text_lower = page_content.lower()
        
        # Extract property name - try multiple selectors
        building_name = ""
        name_selectors = [
            'h1', 'h2', '.property-name', '.property-title', 
            '[class*="title"]', '[class*="name"]', '.listing-name'
        ]
        for sel in name_selectors:
            try:
                elements = await page.query_selector_all(sel)
                for el in elements:
                    text = (await el.text_content() or "").strip()
                    # Filter out navigation/UI text
                    if text and len(text) < 200 and len(text) > 2:
                        if not any(skip in text.lower() for skip in SKIP_TEXT_TERMS):
                            building_name = text
                            break
                if building_name:
                    break
            except Exception:
                continue
        
        # Extract full address - more comprehensive selectors
        full_address = ""
        street = ""
        city = ""
        state = ""
        zipcode = ""
        
        address_selectors = [
            '.address', '.property-address', '[class*="address"]',
            '[itemprop="address"]', '.location', '[class*="location"]',
            'address', '[data-address]'
        ]
        for sel in address_selectors:
            try:
                elements = await page.query_selector_all(sel)
                for el in elements:
                    text = (await el.text_content() or "").strip()
                    # Look for text that looks like an address (has numbers and street indicators)
                    if text and len(text) > 5:
                        # Check if it looks like a real address
                        if any(indicator in text.lower() for indicator in STREET_INDICATORS):
                            full_address = text.replace('\n', ', ').strip()
                            break
                if full_address:
                    break
            except Exception:
                continue
        
        # If still no address, try to find it in page text using regex
        if not full_address:
            # Look for Minneapolis/St. Paul addresses
            for pattern in ADDRESS_PATTERNS:
                match = pattern.search(page_content)
                if match:
                    full_address = match.group(1).strip()
                    break
        
        # Parse address components
        if full_address:
            # Clean up the address
            full_address = re.sub(r'\s+', ' ', full_address).strip()
            parts = full_address.split(',')
            if len(parts) >= 1:
                street = parts[0].strip()
            if len(parts) >= 2:
                city = parts[1].strip()
            if len(parts) >= 3:
                state_zip = parts[2].strip().split()
                if len(state_zip) >= 1:
                    state = state_zip[0]
                if len(state_zip) >= 2:
                    zipcode = state_zip[1]
            # Default city if not found
            if not city:
                city = "Minneapolis"
            if not state:
                state = "MN"
        
        # Extract contact info (phone and email)
        landlord_phone = ""
        landlord_email = ""
        
        # Phone extraction
        phone_selectors = [
            'a[href^="tel:"]', '.phone', '[class*="phone"]', 
            '[itemprop="telephone"]', '[data-phone]'
        ]
        for sel in phone_selectors:
            try:
                elements = await page.query_selector_all(sel)
                for el in elements:
                    phone_text = await el.get_attribute('href') or await el.text_content()
                    if phone_text:
                        phone_text = phone_text.replace('tel:', '').strip()
                        # Validate phone number format
                        phone_match = re.search(r'[\d\-\(\)\s\+\.]{10,}', phone_text)
                        if phone_match:
                            landlord_phone = phone_match.group(0).strip()
                            break
                if landlord_phone:
                    break
            except Exception:
                continue
        
        # Email extraction
        email_selectors = [
            'a[href^="mailto:"]', '.email', '[class*="email"]', 
            '[itemprop="email"]', '[data-email]'
        ]
        for sel in email_selectors:
            try:
                elements = await page.query_selector_all(sel)
                for el in elements:
                    email_text = await el.get_attribute('href') or await el.text_content()
                    if email_text:
                        email_text = email_text.replace('mailto:', '').strip()
                        if '@' in email_text:
                            landlord_email = email_text
                            break
                if landlord_email:
                    break
            except Exception:
                continue
        
        # Extract amenities from full page content
        amenities = extract_amenities_from_text(page_text_lower)
        
        # Extract unit table rows
        # Table columns: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
        table_selectors = [
            'table',
            '[class*="unit-table"]',
            '[class*="floor-plan"]',
            '[class*="pricing"]',
            '[class*="units"]',
        ]
        
        rows_found = False
        for table_sel in table_selectors:
            try:
                table = await page.query_selector(table_sel)
                if not table:
                    continue
                
                # Get all rows
                rows = await table.query_selector_all('tr')
                if not rows or len(rows) <= 1:  # Need header + at least one data row
                    continue
                
                rows_found = True
                
                for row in rows[1:]:  # Skip header row
                    try:
                        cells = await row.query_selector_all('td')
                        if not cells:
                            continue
                        
                        cell_texts = []
                        for cell in cells:
                            text = (await cell.text_content() or "").strip()
                            cell_texts.append(text)
                        
                        # Parse row based on expected columns:
                        # Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
                        unit_label = cell_texts[0] if len(cell_texts) > 0 else ""
                        beds = parse_beds(cell_texts[1]) if len(cell_texts) > 1 else None
                        baths = parse_baths(cell_texts[2]) if len(cell_texts) > 2 else None
                        rent_raw = cell_texts[3] if len(cell_texts) > 3 else ""
                        sqft = parse_sqft(cell_texts[4]) if len(cell_texts) > 4 else None
                        available_date = cell_texts[5] if len(cell_texts) > 5 else ""
                        
                        # Parse tour availability - check for various positive indicators
                        tour_text = cell_texts[6].lower() if len(cell_texts) > 6 else ""
                        has_tour = any(indicator in tour_text for indicator in [
                            'yes', 'available', 'virtual', 'tour', '✓', '✔', 'true', '1'
                        ]) if tour_text else False
                        
                        rent_min, rent_max, price_type = parse_rent(rent_raw)
                        
                        unit = UnitListing(
                            listing_id=f"{property_id}_{unit_label or 'unit'}",
                            building_name=building_name,
                            full_address=full_address,
                            street=street,
                            city=city,
                            state=state,
                            zip=zipcode,
                            unit_label=unit_label,
                            beds=beds,
                            baths=baths,
                            sqft=sqft,
                            rent_raw=rent_raw,
                            rent_min=rent_min,
                            rent_max=rent_max,
                            price_type=price_type,
                            is_per_bed=(price_type == "per_bed"),
                            available_date=available_date,
                            has_virtual_tour=has_tour,
                            landlord_phone=landlord_phone,
                            landlord_email=landlord_email,
                            is_student_branded=True,
                            listing_url=f"{LISTING_PAGE}?property={property_id}",
                            source_url=f"{LISTING_PAGE}?property={property_id}",
                            **amenities
                        )
                        units.append(unit)
                        
                    except Exception as e:
                        logger.debug(f"Error parsing row: {e}")
                        continue
                
                if units:
                    break  # Found units, don't try other table selectors
                    
            except Exception as e:
                logger.debug(f"Error with table selector {table_sel}: {e}")
                continue
        
        # If no table found, create a single listing from header info
        if not rows_found or not units:
            # Try to get bed/bath/price from summary at top of modal
            summary_text = ""
            summary_selectors = [
                '[class*="summary"]', '[class*="overview"]',
                '.bed-bath', '.price-range', '[class*="detail"]',
                '[class*="info"]', '[class*="specs"]'
            ]
            for sel in summary_selectors:
                try:
                    elements = await page.query_selector_all(sel)
                    for el in elements:
                        summary_text += " " + (await el.text_content() or "")
                except Exception:
                    continue
            
            # Also try to extract from page content using regex
            beds = None
            baths = None
            
            if summary_text:
                beds = parse_beds(summary_text)
                baths = parse_baths(summary_text)
            
            # Fallback: look for bed/bath patterns in page content
            if beds is None:
                bed_match = BED_PATTERN.search(page_content)
                if bed_match:
                    beds = float(bed_match.group(1))
                elif 'studio' in page_text_lower:
                    beds = 0.0
            
            if baths is None:
                bath_match = BATH_PATTERN.search(page_content)
                if bath_match:
                    baths = float(bath_match.group(1))
            
            # Look for price - more comprehensive selectors
            rent_raw = ""
            price_selectors = [
                '.price', '[class*="price"]', '[class*="rent"]',
                '[class*="cost"]', '[class*="rate"]'
            ]
            for sel in price_selectors:
                try:
                    elements = await page.query_selector_all(sel)
                    for el in elements:
                        price_text = (await el.text_content() or "").strip()
                        # Check if it looks like a price
                        if '$' in price_text or re.search(r'\d{3,}', price_text):
                            rent_raw = price_text
                            break
                    if rent_raw:
                        break
                except Exception:
                    continue
            
            # Fallback: look for price in page content using regex
            if not rent_raw:
                price_match = PRICE_PATTERN.search(page_content)
                if price_match:
                    rent_raw = price_match.group(0)
            
            rent_min, rent_max, price_type = parse_rent(rent_raw)
            
            unit = UnitListing(
                listing_id=property_id,
                building_name=building_name,
                full_address=full_address,
                street=street,
                city=city,
                state=state,
                zip=zipcode,
                beds=beds,
                baths=baths,
                rent_raw=rent_raw,
                rent_min=rent_min,
                rent_max=rent_max,
                price_type=price_type,
                is_per_bed=(price_type == "per_bed"),
                landlord_phone=landlord_phone,
                landlord_email=landlord_email,
                is_student_branded=True,
                listing_url=f"{LISTING_PAGE}?property={property_id}",
                source_url=f"{LISTING_PAGE}?property={property_id}",
                **amenities
            )
            units.append(unit)
        
        logger.info(f"Extracted {len(units)} units from property {property_id}: {building_name}")
        
    except Exception as e:
        logger.error(f"Error extracting property {property_id}: {e}")
    
    return units


def extract_amenities_from_text(text: str) -> Dict[str, Optional[bool]]:
    """
    Extract amenity flags from page text.
    
    Based on UMN listings site structure, amenities are found in:
    - Property Highlights section (laundry type, pet policy)
    - Amenities section with categories: Parking, Unit Features, Property Features
    
    Sample amenities from listing 60211:
    - Unit Features: Air Conditioning, Dishwasher, Furnished, Washer & Dryer, etc.
    - Property Features: Fitness Center, Laundry On-Site, Pets Allowed, Cats, Dogs, etc.
    """
    text_lower = text.lower()
    
    # In-unit laundry: look for "washer & dryer", "washer/dryer", "in-unit laundry", "w/d in unit"
    has_in_unit_laundry = any(phrase in text_lower for phrase in [
        'washer & dryer', 'washer/dryer', 'washer and dryer',
        'in-unit laundry', 'in unit laundry', 'w/d in unit',
        'laundry in unit', 'in-unit washer', 'in unit washer'
    ])
    
    # On-site laundry: "laundry on-site", "laundry room", "laundry facilities", "on-site laundry"
    has_on_site_laundry = any(phrase in text_lower for phrase in [
        'laundry on-site', 'laundry on site', 'on-site laundry', 'on site laundry',
        'laundry room', 'laundry facilities', 'shared laundry', 'common laundry'
    ])
    
    # Dishwasher
    has_dishwasher = 'dishwasher' in text_lower
    
    # Air conditioning: "air conditioning", "a/c", "central air", "central heat and air"
    has_ac = any(phrase in text_lower for phrase in [
        'air conditioning', 'air conditioner', 'a/c', 'ac ',
        'central air', 'central heat and air', 'hvac', 'climate control'
    ])
    
    # Heat included: look for explicit "heat included" or utility inclusion mentions
    has_heat_included = any(phrase in text_lower for phrase in [
        'heat included', 'heating included', 'heat & water included',
        'utilities included'  # Often means heat is included
    ])
    
    # Water included
    has_water_included = any(phrase in text_lower for phrase in [
        'water included', 'water & sewer included', 'heat & water included',
        'water/sewer included'
    ])
    
    # Internet included: "internet included", "wifi included", "internet ready" is NOT included
    has_internet_included = any(phrase in text_lower for phrase in [
        'internet included', 'wifi included', 'wi-fi included',
        'free internet', 'free wifi', 'free wi-fi'
    ])
    # Note: "internet ready" or "cable ready" means available but NOT included
    
    # Furnished: check for "furnished" but exclude "unfurnished"
    is_furnished = (
        'furnished' in text_lower and 
        'unfurnished' not in text_lower and
        'semi-furnished' not in text_lower
    )
    
    # Gym/Fitness: "fitness center", "gym", "fitness room", "24 hour fitness"
    has_gym = any(phrase in text_lower for phrase in [
        'fitness center', 'fitness room', 'gym', 'exercise room',
        '24 hour fitness', 'workout room', 'fitness facility'
    ])
    
    # Pool - use word boundary to avoid false positives like "ping pong" or "carpool"
    has_pool = bool(re.search(r'\b(?:swimming\s+)?pool\b', text_lower)) or \
               any(phrase in text_lower for phrase in ['indoor pool', 'outdoor pool'])
    
    # Rooftop/Clubroom: "rooftop", "club room", "clubhouse", "entertainment room", "lounge"
    has_rooftop_or_clubroom = any(phrase in text_lower for phrase in [
        'rooftop', 'roof deck', 'club room', 'clubroom', 'clubhouse',
        'entertainment room', 'community room', 'resident lounge', 'lounge'
    ])
    
    # Parking available: "parking", but not just "no parking"
    has_parking_available = (
        'parking' in text_lower and 
        'no parking' not in text_lower
    ) or 'garage' in text_lower
    
    # Garage parking specifically
    has_garage = any(phrase in text_lower for phrase in [
        'garage parking', 'garage', 'underground parking', 'covered parking'
    ])
    
    # Pets allowed: "pets allowed", "pet friendly", "cats", "dogs"
    pets_allowed = any(phrase in text_lower for phrase in [
        'pets allowed', 'pet friendly', 'pet-friendly', 'cats allowed',
        'dogs allowed', 'cats ok', 'dogs ok'
    ])
    # Also check if cats or dogs are mentioned as amenities
    if not pets_allowed:
        # If "cats" or "dogs" appear as standalone amenities (not "no cats", "no dogs")
        if ('cats' in text_lower and 'no cats' not in text_lower) or \
           ('dogs' in text_lower and 'no dogs' not in text_lower):
            pets_allowed = True
    
    return {
        'has_in_unit_laundry': has_in_unit_laundry,
        'has_on_site_laundry': has_on_site_laundry,
        'has_dishwasher': has_dishwasher,
        'has_ac': has_ac,
        'has_heat_included': has_heat_included,
        'has_water_included': has_water_included,
        'has_internet_included': has_internet_included,
        'is_furnished': is_furnished,
        'has_gym': has_gym,
        'has_pool': has_pool,
        'has_rooftop_or_clubroom': has_rooftop_or_clubroom,
        'has_parking_available': has_parking_available,
        'has_garage': has_garage,
        'pets_allowed': pets_allowed,
    }


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


async def main(headless: bool = True, max_listings: int = None, neighborhood: str = None) -> int:
    """
    Main scraping function for listings.umn.edu.
    
    Based on PDF notes:
    - Site is JavaScript-heavy, uses headless browser (Playwright)
    - Listing cards don't use <a> links; clicking triggers JS to open modal
    - Property modal URL: ?property=<id>
    - Modal contains unit table with columns: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
    - Need infinite scroll to load all cards
    - No lat/lon from site; must geocode addresses ourselves
    
    Args:
        headless: Run browser in headless mode
        max_listings: Max listings to scrape (None = unlimited)
        neighborhood: Filter by neighborhood tab (dinkytown, marcy-holmes, como, prospect-park)
    
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
    if neighborhood:
        logger.info(f"Neighborhood filter: {neighborhood}")
    logger.info(f"Output file: {OUTPUT_CSV}")
    
    # Recommend non-headless for this site
    if headless:
        logger.warning("NOTE: listings.umn.edu uses heavy JavaScript.")
        logger.warning("If scraping fails, try with --headless=False")

    all_units: List[UnitListing] = []

    async with async_playwright() as p:
        selected_user_agent = random.choice(USER_AGENTS)
        logger.info(f"Using user agent: {selected_user_agent[:50]}...")
        
        # Enhanced browser arguments for stealth
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-site-isolation-trials',
        ]
        
        browser = await p.chromium.launch(
            headless=headless,
            args=browser_args
        )

        # Enhanced context with more realistic settings
        context = await browser.new_context(
            user_agent=selected_user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/Chicago',
            java_script_enabled=True,
            has_touch=False,
            is_mobile=False,
            device_scale_factor=1,
            permissions=['geolocation'],
            geolocation={'latitude': UMN_CAMPUS_LAT, 'longitude': UMN_CAMPUS_LON},
        )
        
        # Add script to hide automation indicators
        await context.add_init_script("""
            // Override the navigator.webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override the plugins property
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override the languages property
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)

        page = await context.new_page()

        try:
            # Navigate to listings page with retries
            logger.info(f"Navigating to: {LISTING_PAGE}")
            navigation_success = False
            
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    logger.info(f"Navigation attempt {attempt + 1}/{RETRY_ATTEMPTS}...")
                    await page.goto(LISTING_PAGE, wait_until="load", timeout=NAV_TIMEOUT)
                    navigation_success = True
                    break
                except PlaywrightTimeout as e:
                    logger.warning(f"Attempt {attempt + 1} timed out: {e}")
                    if attempt < RETRY_ATTEMPTS - 1:
                        logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                        await asyncio.sleep(RETRY_DELAY)
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < RETRY_ATTEMPTS - 1:
                        logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                        await asyncio.sleep(RETRY_DELAY)
            
            if not navigation_success:
                logger.error("All navigation attempts failed.")
                logger.error("Please check your internet connection and that listings.umn.edu is accessible.")
                return 0
            
            # Wait for page to stabilize
            await asyncio.sleep(5)
            
            # Check if page loaded correctly
            page_content = await page.content()
            if len(page_content) < 1000:
                logger.error("Page content seems too short - may not have loaded correctly")
                logger.info(f"Page content length: {len(page_content)}")
            
            page_title = await page.title()
            logger.info(f"Page title: {page_title}")
            
            # Click neighborhood tab if specified
            if neighborhood:
                await click_neighborhood_tab(page, neighborhood)
                await asyncio.sleep(2)
            
            # Scroll to load all listings (infinite scroll)
            logger.info("Loading all listings via infinite scroll...")
            total_cards = await scroll_to_load_all_listings(page)
            logger.info(f"Finished loading. Total cards visible: {total_cards}")
            
            # Get property IDs from cards
            property_ids = await get_property_ids_from_cards(page)
            
            # If no IDs found via data attributes, try click-based extraction
            if not property_ids:
                logger.info("No property IDs from data attributes. Trying click-based extraction...")
                # Try clicking cards to get property IDs
                for i in range(min(total_cards, max_listings or 100)):
                    prop_id = await click_card_and_get_property_id(page, i)
                    if prop_id and prop_id not in property_ids:
                        property_ids.append(prop_id)
                    await close_property_modal(page)
                    await asyncio.sleep(0.5)
            
            logger.info(f"Found {len(property_ids)} properties to scrape")
            
            if max_listings:
                property_ids = property_ids[:max_listings]
                logger.info(f"Limited to {max_listings} listings")
            
            # Scrape each property by navigating directly to its URL
            # No need to go back to listing page between properties
            for idx, prop_id in enumerate(property_ids, 1):
                logger.info(f"Processing property {idx}/{len(property_ids)}: {prop_id}")
                try:
                    # Navigate directly to property page
                    property_url = f"{LISTING_PAGE}?property={prop_id}"
                    await page.goto(property_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                    
                    # Wait for content to fully render
                    await asyncio.sleep(MODAL_WAIT_SECONDS)
                    
                    # Verify we're on the right page
                    if prop_id not in page.url:
                        logger.warning(f"Navigation to property {prop_id} may have failed, URL: {page.url}")
                        continue
                    
                    # Extract data from the property page
                    units = await extract_property_from_modal(page, prop_id)
                    
                    # Add neighborhood info if filtering
                    if neighborhood:
                        for unit in units:
                            unit.neighborhood = neighborhood
                    
                    if units:
                        all_units.extend(units)
                        logger.info(f"Extracted {len(units)} units from property {prop_id}")
                        logger.info(f"Total units collected: {len(all_units)}")
                    else:
                        logger.warning(f"No units extracted from property {prop_id}")
                        
                except PlaywrightTimeout as e:
                    logger.warning(f"Timeout scraping property {prop_id}: {e}")
                except Exception as e:
                    logger.error(f"Failed to scrape property {prop_id}: {e}")
                
                # Random delay between properties to avoid detection
                delay = get_random_delay()
                await asyncio.sleep(delay)

        finally:
            await browser.close()

    # Geocode and filter (site doesn't provide lat/lon)
    logger.info("Geocoding addresses and filtering by distance...")
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
Site Structure (from PDF documentation):
  - Heavy JavaScript site - requires headless browser
  - Listing cards: Displayed as <div>, no <a> links (click triggers JS)
  - Property modal: ?property=<id> URL pattern
  - Unit table columns: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
  - Neighborhood tabs: dinkytown, marcy-holmes, como, prospect-park
  - Infinite scroll to load all listings
  - No lat/lon exposed - addresses are geocoded

Examples:
  # Test run (visible browser)
  python3 -m scraper.umn_listings --headless=False

  # Full headless run
  python3 -m scraper.umn_listings --headless=True

  # Limit number of listings
  python3 -m scraper.umn_listings --headless=False --max_listings=10
  
  # Filter by neighborhood
  python3 -m scraper.umn_listings --neighborhood=dinkytown
  
  # Scrape all neighborhoods
  python3 -m scraper.umn_listings --all_neighborhoods
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
        help='Maximum number of listings to scrape per neighborhood. Default: unlimited'
    )
    parser.add_argument(
        '--neighborhood',
        type=str,
        choices=NEIGHBORHOODS,
        default=None,
        help=f'Filter by neighborhood tab. Options: {", ".join(NEIGHBORHOODS)}'
    )
    parser.add_argument(
        '--all_neighborhoods',
        action='store_true',
        help='Scrape all neighborhoods one by one'
    )
    
    return parser.parse_args()


async def scrape_all_neighborhoods(headless: bool = True, max_listings: int = None) -> int:
    """Scrape all neighborhoods sequentially."""
    total_units = 0
    for neighborhood in NEIGHBORHOODS:
        logger.info(f"\n{'='*80}")
        logger.info(f"SCRAPING NEIGHBORHOOD: {neighborhood.upper()}")
        logger.info(f"{'='*80}\n")
        units = await main(
            headless=headless,
            max_listings=max_listings,
            neighborhood=neighborhood
        )
        total_units += units
        await asyncio.sleep(get_random_delay())  # Delay between neighborhoods
    return total_units


if __name__ == "__main__":
    args = parse_args()
    
    if args.all_neighborhoods:
        asyncio.run(scrape_all_neighborhoods(
            headless=args.headless,
            max_listings=args.max_listings
        ))
    else:
        asyncio.run(main(
            headless=args.headless,
            max_listings=args.max_listings,
            neighborhood=args.neighborhood
        ))
