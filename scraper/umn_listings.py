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

# Rate limiting settings - more conservative for university site
PAGE_DELAY_SECONDS = 4.0
PAGE_DELAY_VARIANCE = 3.0
MODAL_WAIT_SECONDS = 2.0  # Wait for modal to render
GEOCODE_DELAY_SECONDS = 1.0

# Navigation settings
NAV_TIMEOUT = 90000  # 90 seconds for page load
MODAL_TIMEOUT = 15000  # 15 seconds for modal to appear
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# Scroll settings for infinite scroll
SCROLL_DELAY_MIN = 0.8
SCROLL_DELAY_MAX = 2.0
MAX_SCROLL_ATTEMPTS = 50  # Maximum scroll iterations to find all listings

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
    
    for scroll_attempt in range(MAX_SCROLL_ATTEMPTS):
        # Scroll to bottom
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        # Count current cards - try multiple selectors
        card_selectors = [
            '[data-property-id]',  # Most specific
            '.listing-card',
            '.property-card',
            'div[class*="listing"]',
            'div[class*="property"]',
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
            if no_change_count >= 3:
                # No new cards after 3 scrolls, we've loaded everything
                logger.info(f"Finished scrolling. Total cards found: {current_count}")
                break
        else:
            no_change_count = 0
            previous_count = current_count
    
    return previous_count


async def click_neighborhood_tab(page: Page, neighborhood: str) -> bool:
    """
    Click on a neighborhood tab to filter listings.
    Neighborhood tabs trigger AJAX calls to update the listings grid.
    """
    try:
        # Try various tab selectors
        tab_selectors = [
            f'[data-neighborhood="{neighborhood}"]',
            f'button:has-text("{neighborhood}")',
            f'a:has-text("{neighborhood}")',
            f'[class*="tab"]:has-text("{neighborhood}")',
            f'div[role="tab"]:has-text("{neighborhood}")',
        ]
        
        for selector in tab_selectors:
            try:
                tab = await page.query_selector(selector)
                if tab:
                    await tab.click()
                    await asyncio.sleep(2)  # Wait for AJAX to complete
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
                    return True
            except Exception:
                continue
        
        # Alternative: press Escape key
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.5)
        
        # Alternative: navigate back to listing page without property param
        if '?property=' in page.url:
            await page.goto(LISTING_PAGE, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
            await asyncio.sleep(1)
            return True
        
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
    """
    units = []
    
    try:
        # Extract property name
        building_name = ""
        name_selectors = ['h1', '.property-name', '.property-title', '[class*="title"]']
        for sel in name_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = (await el.text_content() or "").strip()
                    if text and len(text) < 200:  # Reasonable name length
                        building_name = text
                        break
            except Exception:
                continue
        
        # Extract full address
        full_address = ""
        street = ""
        city = ""
        state = ""
        zipcode = ""
        
        address_selectors = [
            '.address', '.property-address', '[class*="address"]',
            '[itemprop="address"]', '.location'
        ]
        for sel in address_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    full_address = (await el.text_content() or "").strip()
                    if full_address:
                        # Parse address components
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
                        break
            except Exception:
                continue
        
        # Extract contact info (phone and email)
        landlord_phone = ""
        landlord_email = ""
        
        phone_selectors = ['.phone', '[class*="phone"]', 'a[href^="tel:"]', '[itemprop="telephone"]']
        for sel in phone_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    phone_text = await el.get_attribute('href') or await el.text_content()
                    if phone_text:
                        phone_text = phone_text.replace('tel:', '').strip()
                        if re.match(r'[\d\-\(\)\s\+]+', phone_text):
                            landlord_phone = phone_text
                            break
            except Exception:
                continue
        
        email_selectors = ['.email', '[class*="email"]', 'a[href^="mailto:"]', '[itemprop="email"]']
        for sel in email_selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    email_text = await el.get_attribute('href') or await el.text_content()
                    if email_text:
                        email_text = email_text.replace('mailto:', '').strip()
                        if '@' in email_text:
                            landlord_email = email_text
                            break
            except Exception:
                continue
        
        # Extract amenities from page content
        page_text = (await page.content()).lower()
        amenities = extract_amenities_from_text(page_text)
        
        # Extract unit table rows
        # Table columns: Name, Beds, Baths, Price From, Sq.Ft, Available, Tour
        table_selectors = [
            'table',
            '[class*="unit-table"]',
            '[class*="floor-plan"]',
            '[class*="pricing"]',
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
                        has_tour = "yes" in (cell_texts[6].lower() if len(cell_texts) > 6 else "")
                        
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
                '.bed-bath', '.price-range'
            ]
            for sel in summary_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        summary_text += " " + (await el.text_content() or "")
                except Exception:
                    continue
            
            beds = parse_beds(summary_text) if summary_text else None
            baths = parse_baths(summary_text) if summary_text else None
            
            # Look for price
            rent_raw = ""
            price_selectors = ['.price', '[class*="price"]', '[class*="rent"]']
            for sel in price_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        rent_raw = (await el.text_content() or "").strip()
                        break
                except Exception:
                    continue
            
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
    """Extract amenity flags from page text."""
    text_lower = text.lower()
    return {
        'has_in_unit_laundry': 'in-unit' in text_lower and 'laundry' in text_lower,
        'has_on_site_laundry': 'on-site laundry' in text_lower or 'laundry room' in text_lower,
        'has_dishwasher': 'dishwasher' in text_lower,
        'has_ac': 'air condition' in text_lower or ' a/c' in text_lower or 'central air' in text_lower,
        'has_heat_included': 'heat included' in text_lower,
        'has_water_included': 'water included' in text_lower,
        'has_internet_included': 'internet included' in text_lower or 'wifi included' in text_lower,
        'is_furnished': 'furnished' in text_lower and 'unfurnished' not in text_lower,
        'has_gym': 'gym' in text_lower or 'fitness' in text_lower,
        'has_pool': 'pool' in text_lower,
        'has_rooftop_or_clubroom': 'rooftop' in text_lower or 'clubhouse' in text_lower or 'club room' in text_lower,
        'has_parking_available': 'parking' in text_lower,
        'has_garage': 'garage' in text_lower,
        'pets_allowed': 'pet friendly' in text_lower or 'pets allowed' in text_lower,
    }


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
            
            # Scrape each property by opening its modal
            for idx, prop_id in enumerate(property_ids, 1):
                logger.info(f"Processing property {idx}/{len(property_ids)}: {prop_id}")
                try:
                    # Open the property modal
                    if await open_property_modal(page, prop_id):
                        # Wait for modal to fully render
                        await asyncio.sleep(MODAL_WAIT_SECONDS)
                        
                        # Extract data from modal
                        units = await extract_property_from_modal(page, prop_id)
                        
                        # Add neighborhood info if filtering
                        if neighborhood:
                            for unit in units:
                                unit.neighborhood = neighborhood
                        
                        all_units.extend(units)
                        logger.info(f"Total units collected: {len(all_units)}")
                        
                        # Close modal before processing next property
                        await close_property_modal(page)
                    else:
                        logger.warning(f"Could not open modal for property {prop_id}")
                        
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
