"""
UMN Listings (listings.umn.edu) Web Scraper

Scrapes the official University of Minnesota Off-Campus Housing Marketplace
(powered by Rent College Pads) for student housing listings.

IMPORTANT: This site is powered by Rent College Pads and may have bot protection.
The scraper uses enhanced stealth settings to work around these restrictions.

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
import traceback
from dataclasses import dataclass, asdict, field, fields
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import List, Optional, Dict, Any, Set

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

# Rate limiting settings - more conservative for university site
PAGE_DELAY_SECONDS = 4.0
PAGE_DELAY_VARIANCE = 3.0
GEOCODE_DELAY_SECONDS = 1.0

# Navigation settings
NAV_TIMEOUT = 120000  # 120 seconds for initial page load (domcontentloaded)
NETWORKIDLE_TIMEOUT = 60000  # 60 seconds for network idle wait
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5

# Bot/captcha detection settings
MIN_PAGE_CONTENT_LENGTH = 2000  # Minimum content length for a valid page
CHALLENGE_WAIT_TIMEOUT = 120  # Seconds to wait for user to complete challenge
CHALLENGE_CHECK_INTERVAL = 5  # Seconds between checks during challenge wait

# Bot/captcha detection keywords - these indicate a challenge page, not the actual listings
# Be specific to avoid false positives from normal page content
BOT_BLOCK_KEYWORDS = [
    'please verify you are a human',
    'checking your browser',
    'just a moment',  # Cloudflare "Just a moment..."
    'enable javascript and cookies',
    'ray id',  # Cloudflare Ray ID
    'challenge-platform',  # Cloudflare challenge
    'cf-browser-verification',
    'hcaptcha',
    'recaptcha',
]

# Scroll settings
SCROLL_DELAY_MIN = 0.8
SCROLL_DELAY_MAX = 2.0

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


def detect_bot_block(page_content: str) -> bool:
    """
    Check if page content indicates bot blocking or captcha.
    
    Returns True only if:
    1. A bot-block keyword is found, AND
    2. No positive indicators of actual listing content are present
    """
    content_lower = page_content.lower()
    
    # Positive indicators that the actual page loaded successfully
    positive_indicators = [
        'off-campus housing',
        'rent college pads',
        'listings.umn.edu',
        'bedroom',
        'apartment',
        'housing',
        '/listing/',  # URL patterns in the page
        'property',
        'lease',
    ]
    
    # If we find positive indicators of real content, it's not a bot block
    for indicator in positive_indicators:
        if indicator in content_lower:
            return False
    
    # Check for bot block keywords
    for keyword in BOT_BLOCK_KEYWORDS:
        if keyword in content_lower:
            return True
    
    # If page is very short and has no positive indicators, might be blocked
    if len(page_content) < MIN_PAGE_CONTENT_LENGTH:
        return True
        
    return False


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
        # Give page a moment to stabilize after load more clicks
        await asyncio.sleep(3)
        
        # Scroll to ensure all lazy-loaded content is visible
        await simulate_human_scrolling(page)
        await asyncio.sleep(2)
        
        # Get all links on the page first for debugging
        all_links = await page.query_selector_all('a')
        logger.info(f"Total links found on page: {len(all_links)}")
        
        # Log a sample of hrefs for debugging
        sample_hrefs = []
        for link in all_links[:20]:
            try:
                href = await link.get_attribute('href')
                if href:
                    sample_hrefs.append(href)
            except:
                pass
        logger.debug(f"Sample hrefs: {sample_hrefs}")
        
        # Try multiple selectors for listing links
        # Rent College Pads uses various card/listing structures
        selectors = [
            'a[href*="/listing/"]',  # Direct listing links
            'a[href*="listings.umn.edu/listing/"]',  # Full URL listing links
            '.listing-card a',
            '.property-card a', 
            'a.listing-link',
            '[data-listing-id] a',
            '.search-results a',
            'article a',
            '.card a',
            '.MuiCard-root a',  # Material UI cards
            '[class*="listing"] a',  # Any class containing "listing"
            '[class*="property"] a',  # Any class containing "property"
            '[class*="card"] a',  # Any class containing "card"
            'div[class*="List"] a',  # List containers
        ]
        
        for selector in selectors:
            try:
                links = await page.query_selector_all(selector)
                logger.debug(f"Selector '{selector}' found {len(links)} elements")
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        # Make absolute URL if needed
                        if href.startswith('/'):
                            href = BASE_URL + href
                        # Only include listing URLs (check for listing path pattern)
                        if '/listing/' in href:
                            # Clean the URL
                            if href.startswith('http'):
                                urls.add(href)
                            elif href.startswith('/'):
                                urls.add(BASE_URL + href)
            except Exception as e:
                logger.debug(f"Selector '{selector}' error: {e}")
                continue
        
        # Also try to find listing URLs in onclick handlers or data attributes
        try:
            all_elements = await page.query_selector_all('[onclick*="listing"], [data-href*="listing"], [data-url*="listing"]')
            for el in all_elements:
                onclick = await el.get_attribute('onclick') or ''
                data_href = await el.get_attribute('data-href') or ''
                data_url = await el.get_attribute('data-url') or ''
                
                for attr in [onclick, data_href, data_url]:
                    if '/listing/' in attr:
                        # Extract URL from attribute
                        url_match = re.search(r'(/listing/[^\s\'"]+)', attr)
                        if url_match:
                            urls.add(BASE_URL + url_match.group(1))
        except Exception as e:
            logger.debug(f"Data attribute search error: {e}")
        
        logger.info(f"Found {len(urls)} listing URLs")
        
        # Log sample URLs for verification
        if urls:
            sample = list(urls)[:3]
            logger.info(f"Sample listing URLs: {sample}")
        
    except Exception as e:
        logger.error(f"Error extracting listing URLs: {e}")
        logger.debug(traceback.format_exc())
    
    return list(urls)


async def scrape_listings_from_cards(page: Page) -> List[UnitListing]:
    """
    Scrape listing data directly from the listing cards on the main page.
    
    The UMN listings site shows cards with property info visible in the card itself:
    - Property name
    - Address (e.g., "2508 Delaware Street SE, Minneapolis, MN 55414")
    - Bed count (e.g., "2 - 4 Bed")
    - Featured badge
    
    We extract this info directly from the cards without clicking to avoid navigation issues.
    """
    listings = []
    
    try:
        logger.info("Scraping listings directly from cards...")
        
        # Wait for content to settle
        await asyncio.sleep(2)
        
        # Find cards using data-property-id which the site uses
        card_selectors = [
            '[data-property-id]',
            '[data-listing-id]',
            '[class*="listing-card"]',
            '[class*="property-card"]', 
            '[class*="ListingCard"]',
            '[class*="PropertyCard"]',
        ]
        
        # Find the best selector that returns cards
        best_selector = None
        cards = []
        for selector in card_selectors:
            try:
                found = await page.query_selector_all(selector)
                if found and len(found) > len(cards):
                    cards = found
                    best_selector = selector
                    logger.debug(f"Card selector '{selector}' found {len(found)} cards")
            except Exception:
                continue
        
        if not cards:
            logger.warning("No listing cards found on page")
            return listings
        
        logger.info(f"Found {len(cards)} listing cards using selector: {best_selector}")
        
        # Process each card - extract data directly from card content
        for idx, card in enumerate(cards):
            try:
                # Get the full text content from the card
                card_text = await card.text_content() or ""
                card_html = await card.inner_html() or ""
                
                # Try to get property ID from data attribute
                property_id = await card.get_attribute('data-property-id') or await card.get_attribute('data-listing-id') or f"card_{idx}"
                listing_id = f"umn_{property_id}"
                
                # Create listing object
                listing = UnitListing(
                    listing_id=listing_id,
                    building_name="",
                    full_address="",
                    source_url=page.url,
                    is_student_branded=True,
                )
                
                # Parse card text to extract info
                # Card text example: "The Quad on Delaware 2508 Delaware Street SE, Minneapolis, MN 55414    Featured     2 - 4 Bed"
                lines = [line.strip() for line in card_text.split('\n') if line.strip()]
                
                # First line is usually the property name
                if lines:
                    listing.building_name = lines[0]
                
                # Look for address pattern (number + street + city, state zip)
                # Pattern matches: "2508 Delaware Street SE, Minneapolis, MN 55414"
                address_pattern = (
                    r'(\d+\s+[^,]+(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Way|Lane|Ln|Court|Ct)[^,]*)'  # Street address
                    r',?\s*(Minneapolis|St\.?\s*Paul|Saint Paul)'  # City
                    r'[^,]*,?\s*(MN|Minnesota)?'  # State (optional)
                    r'\s*(\d{5})?'  # ZIP code (optional)
                )
                address_match = re.search(address_pattern, card_text, re.IGNORECASE)
                if address_match:
                    listing.street = address_match.group(1).strip()
                    listing.city = address_match.group(2).strip()
                    listing.state = address_match.group(3).strip() if address_match.group(3) else 'MN'
                    listing.zip = address_match.group(4).strip() if address_match.group(4) else ''
                    listing.full_address = f"{listing.street}, {listing.city}, {listing.state} {listing.zip}".strip()
                
                # Look for bed count (e.g., "2 - 4 Bed", "3 Bed", "Studio")
                # For ranges like "2 - 4 Bed", we store the minimum
                bed_match = re.search(r'(\d+)\s*(?:-\s*(\d+))?\s*Bed', card_text, re.IGNORECASE)
                if bed_match:
                    listing.beds = float(bed_match.group(1))
                elif 'studio' in card_text.lower():
                    listing.beds = 0
                
                # Look for bath count
                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*Bath', card_text, re.IGNORECASE)
                if bath_match:
                    listing.baths = float(bath_match.group(1))
                
                # Look for price (e.g., "$1,200", "$800 - $1,500", "$650/bed")
                price_pattern = r'\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*/\s*(?:mo|month|bed|person))?'
                price_match = re.search(price_pattern, card_text, re.IGNORECASE)
                if price_match:
                    listing.rent_raw = price_match.group(0)
                    listing.rent_min, listing.rent_max, listing.price_type = parse_rent(listing.rent_raw)
                    listing.is_per_bed = 'bed' in listing.rent_raw.lower() or 'person' in listing.rent_raw.lower()
                
                # Try to get detail URL from card link
                try:
                    link = await card.query_selector('a')
                    if link:
                        href = await link.get_attribute('href')
                        if href:
                            if href.startswith('/'):
                                href = 'https://listings.umn.edu' + href
                            listing.source_url = href
                except Exception:
                    pass
                
                # Only add if we got at least a name or address
                if listing.building_name or listing.full_address:
                    listings.append(listing)
                    if (idx + 1) % 50 == 0 or idx == 0:
                        logger.info(f"  Processed {idx + 1}/{len(cards)} cards - Latest: {listing.building_name[:30] if listing.building_name else 'N/A'}...")
                
            except Exception as e:
                logger.debug(f"Error processing card {idx}: {e}")
                continue
        
        logger.info(f"Successfully extracted {len(listings)} listings from cards")
        
    except Exception as e:
        logger.error(f"Error in scrape_listings_from_cards: {e}")
        logger.debug(traceback.format_exc())
    
    return listings


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
    
    # Recommend non-headless for this site
    if headless:
        logger.warning("NOTE: listings.umn.edu may block headless browsers.")
        logger.warning("If scraping fails, try with --headless=False")

    all_units: List[UnitListing] = []

    async with async_playwright() as p:
        # Use Firefox which is often better at avoiding detection
        selected_user_agent = random.choice(USER_AGENTS)
        logger.info(f"Using user agent: {selected_user_agent}")
        
        # Enhanced browser arguments for stealth
        browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-site-isolation-trials',
        ]
        
        # Context options for browser - reused for Firefox fallback
        context_options = {
            'user_agent': selected_user_agent,
            'viewport': {'width': 1920, 'height': 1080},
            'locale': 'en-US',
            'timezone_id': 'America/Chicago',
            'java_script_enabled': True,
            'has_touch': False,
            'is_mobile': False,
            'device_scale_factor': 1,
            'permissions': ['geolocation'],
            'geolocation': {'latitude': UMN_CAMPUS_LAT, 'longitude': UMN_CAMPUS_LON},
        }
        
        # Init script to hide automation indicators
        init_script = """
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
        """
        
        async def attempt_navigation(browser_type, browser_name: str):
            """
            Attempt to navigate to the listings page with the given browser.
            
            Returns:
                Tuple of (browser, context, page) on success, or (None, None, None) on failure.
            """
            logger.info(f"Launching {browser_name} browser...")
            if browser_name == "Chromium":
                browser = await browser_type.launch(headless=headless, args=browser_args)
            else:
                browser = await browser_type.launch(headless=headless)
            
            context = await browser.new_context(**context_options)
            await context.add_init_script(init_script)
            page = await context.new_page()
            
            try:
                # Navigate to listings page with retries
                logger.info(f"Navigating to: {LISTING_PAGE}")
                navigation_success = False
                
                for attempt in range(RETRY_ATTEMPTS):
                    try:
                        logger.info(f"Navigation attempt {attempt + 1}/{RETRY_ATTEMPTS} ({browser_name})...")
                        # Step 1: Use domcontentloaded for initial HTML load (more reliable)
                        await page.goto(LISTING_PAGE, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                        
                        # Step 2: Wait for network idle separately with its own timeout
                        try:
                            await page.wait_for_load_state('networkidle', timeout=NETWORKIDLE_TIMEOUT)
                        except PlaywrightTimeout:
                            logger.warning("Network idle timeout - continuing with partially loaded page")
                        
                        navigation_success = True
                        break
                    except PlaywrightTimeout as e:
                        # Log detailed debugging info on timeout
                        try:
                            page_content = await page.content()
                            content_len = len(page_content)
                            snippet = page_content[:500] if page_content else "(empty)"
                            logger.warning(f"Attempt {attempt + 1} timed out: {e}")
                            logger.warning(f"Page content length: {content_len}")
                            logger.debug(f"Page content snippet: {snippet}")
                        except Exception:
                            logger.warning(f"Attempt {attempt + 1} timed out: {e} (could not retrieve page content)")
                        
                        if attempt < RETRY_ATTEMPTS - 1:
                            logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                            await asyncio.sleep(RETRY_DELAY)
                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt < RETRY_ATTEMPTS - 1:
                            logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                            await asyncio.sleep(RETRY_DELAY)
                
                if not navigation_success:
                    await browser.close()
                    return None, None, None  # Signal failure
                
                # Wait for page to stabilize
                await asyncio.sleep(5)
                
                # Check if page loaded correctly
                page_content = await page.content()
                if len(page_content) < 1000:
                    logger.error("Page content seems too short - may not have loaded correctly")
                    logger.info(f"Page content length: {len(page_content)}")
                
                # Check for bot blocking/captcha
                if detect_bot_block(page_content):
                    if not headless:
                        # In non-headless mode, give user time to complete challenge
                        logger.warning("="*60)
                        logger.warning("CHALLENGE PAGE DETECTED")
                        logger.warning("="*60)
                        logger.warning("Please complete any challenge in the browser window.")
                        logger.warning(f"Waiting up to {CHALLENGE_WAIT_TIMEOUT} seconds for you to complete it...")
                        logger.warning("="*60)
                        
                        # Wait for challenge to be completed
                        elapsed = 0
                        
                        while elapsed < CHALLENGE_WAIT_TIMEOUT:
                            await asyncio.sleep(CHALLENGE_CHECK_INTERVAL)
                            elapsed += CHALLENGE_CHECK_INTERVAL
                            
                            # Re-check page content
                            page_content = await page.content()
                            if not detect_bot_block(page_content):
                                logger.info("Challenge completed! Continuing with scraping...")
                                break
                            
                            remaining = CHALLENGE_WAIT_TIMEOUT - elapsed
                            if remaining > 0:
                                logger.info(f"Still waiting for challenge completion... ({remaining}s remaining)")
                        
                        # Final check
                        page_content = await page.content()
                        if detect_bot_block(page_content):
                            logger.error("Challenge was not completed in time.")
                            await browser.close()
                            return None, None, None
                    else:
                        logger.error("="*60)
                        logger.error("BOT DETECTION / CAPTCHA DETECTED")
                        logger.error("="*60)
                        logger.error("The site appears to be blocking automated access.")
                        logger.error("Suggestions:")
                        logger.error("  1. Run with --headless=False to solve captcha manually")
                        logger.error("  2. Try using a different network or VPN")
                        logger.error("  3. Wait and try again later")
                        logger.error("="*60)
                        await browser.close()
                        return None, None, None
                
                # Check for common error pages
                page_title = await page.title()
                logger.info(f"Page title: {page_title}")
                
                return browser, context, page
                
            except Exception as e:
                logger.error(f"Unexpected error during navigation: {e}")
                await browser.close()
                return None, None, None
        
        # Try Chromium first
        browser, context, page = await attempt_navigation(p.chromium, "Chromium")
        
        # If Chromium fails, try Firefox as fallback
        if browser is None:
            logger.warning("="*60)
            logger.warning("Chromium navigation failed. Attempting Firefox fallback...")
            logger.warning("="*60)
            browser, context, page = await attempt_navigation(p.firefox, "Firefox")
        
        if browser is None:
            logger.error("All navigation attempts failed with both Chromium and Firefox.")
            logger.error("Please check your internet connection and that listings.umn.edu is accessible.")
            logger.error("If you're on a university network, you may need to use VPN or connect from a different network.")
            return 0
        
        try:
            
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
            
            # If we found URLs, scrape each listing individually
            if listing_urls:
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
            else:
                # Fallback: scrape listings directly from cards on main page
                logger.warning("No listing URLs found. Attempting to scrape from listing cards directly...")
                card_listings = await scrape_listings_from_cards(page)
                
                if card_listings:
                    logger.info(f"Successfully extracted {len(card_listings)} listings from cards")
                    if max_listings:
                        card_listings = card_listings[:max_listings]
                    all_units.extend(card_listings)
                else:
                    logger.warning("Could not extract listings from cards either.")
                    logger.warning("The page structure may have changed. Please report this issue.")

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
