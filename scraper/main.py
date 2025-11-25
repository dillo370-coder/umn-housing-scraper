"""
Apartments.com Web Scraper for UMN Housing Research

Usage:
  python3 -m scraper.main --headless=False --max_search_pages=1 --max_buildings=2  # test run
  python3 -m scraper.main --headless=True --max_search_pages=50 --max_buildings=800  # full run
"""
import argparse
import asyncio
import csv
import json
import logging
import random
import re
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from typing import List, Optional, Dict, Any
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

# Rate limiting (increase if getting blocked)
PAGE_DELAY_SECONDS = 4.0
GEOCODE_DELAY_SECONDS = 1.5

# User agents to rotate (helps avoid bot detection)
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
OUTPUT_CSV = OUTPUT_DIR / f"umn_housing_data_{TIMESTAMP}.csv"
OUTPUT_CSV_ALL = OUTPUT_DIR / f"umn_housing_ALL_{TIMESTAMP}.csv"
LOG_FILE = OUTPUT_DIR / f"scraper_log_{TIMESTAMP}.log"

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

async def search_apartments(page: Page, location: str, max_pages: int = 10) -> List[str]:
    logger.info(f"Starting search for: {location}")
    building_urls = set()
    try:
        search_url = f"{BASE_URL}/{location.lower().replace(' ', '-').replace(',', '')}/"
        logger.info(f"Navigating to: {search_url}")

        for attempt in range(5):  # Increased retries
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(PAGE_DELAY_SECONDS)
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
                elif attempt < 4:
                    await asyncio.sleep(5 + attempt * 2)
                else:
                    logger.error("Max retries reached. Try running with --headless=False to debug.")
                    logger.error("If issue persists, check your network connection or try later.")
                    raise

        for page_num in range(1, max_pages + 1):
            logger.info(f"Scraping search results page {page_num}")
            property_links = await page.locator('article.placard a.property-link, a.property-link').all()
            if not property_links:
                logger.warning("No property links found, trying broader selector")
                property_links = await page.locator('a[href*="/"] .property-title, a').all()

            for link in property_links:
                try:
                    href = await link.get_attribute('href')
                    if href:
                        full_url = urljoin(BASE_URL, href)
                        full_url = full_url.split('?')[0]
                        building_urls.add(full_url)
                except Exception as e:
                    logger.warning(f"Error extracting link: {e}")

            logger.info(f"Found {len(building_urls)} unique buildings so far")

            next_button = page.locator('a.next, a[rel="next"]')
            if await next_button.count() > 0:
                try:
                    await next_button.first.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=30000)
                    await asyncio.sleep(PAGE_DELAY_SECONDS)
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


async def scrape_building(page: Page, url: str) -> List[UnitListing]:
    logger.info(f"Scraping building: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(PAGE_DELAY_SECONDS)
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


def geocode_and_filter_units(units: List[UnitListing]) -> List[UnitListing]:
    logger.info(f"Geocoding and filtering {len(units)} units")
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
    for unit in units:
        if unit.lat is not None and unit.lon is not None:
            dist = haversine_distance(unit.lat, unit.lon, UMN_CAMPUS_LAT, UMN_CAMPUS_LON)
            unit.dist_to_campus_km = round(dist, 2)
            logger.info(f"{unit.building_name}: {dist:.2f} km from UMN")
            if dist <= SEARCH_RADIUS_KM:
                filtered.append(unit)
                logger.info("  ✓ INCLUDED")
            else:
                logger.info("  ✗ Excluded")
        else:
            logger.warning(f"Could not geocode: {unit.full_address}")
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


async def main(headless: bool = True, max_search_pages: int = 25, max_buildings: int = None):
    logger.info("="*80)
    logger.info("UMN HOUSING SCRAPER STARTED")
    logger.info("="*80)
    logger.info(f"Search location: {SEARCH_LOCATION}")
    logger.info(f"Search radius: {SEARCH_RADIUS_KM} km from UMN campus")
    logger.info(f"Headless mode: {headless}")
    logger.info(f"Max search pages: {max_search_pages}")
    logger.info(f"Max buildings: {max_buildings if max_buildings else 'unlimited'}")
    logger.info(f"Output file: {OUTPUT_CSV}")

    all_units: List[UnitListing] = []

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
            building_urls = await search_apartments(page, SEARCH_LOCATION, max_search_pages)

            if max_buildings:
                building_urls = building_urls[:max_buildings]
                logger.info(f"Limited to {max_buildings} buildings for testing")

            for idx, url in enumerate(building_urls, 1):
                logger.info(f"Processing building {idx}/{len(building_urls)}")
                try:
                    units = await scrape_building(page, url)
                    all_units.extend(units)
                    logger.info(f"Total units collected: {len(all_units)}")
                except Exception as e:
                    logger.error(f"Failed to scrape {url}: {e}")

                await asyncio.sleep(PAGE_DELAY_SECONDS)

        finally:
            await browser.close()

    # Save ALL data first (before filtering)
    logger.info("Saving unfiltered data...")
    export_to_csv(all_units, OUTPUT_CSV_ALL)

    # Now filter and save filtered data
    filtered_units = geocode_and_filter_units(all_units)
    export_to_csv(filtered_units, OUTPUT_CSV)

    logger.info("="*80)
    logger.info("SCRAPING COMPLETE")
    logger.info("="*80)
    logger.info(f"Total units scraped: {len(all_units)}")
    logger.info(f"Units within {SEARCH_RADIUS_KM} km: {len(filtered_units)}")
    logger.info(f"Unique buildings: {len(set(u.building_name for u in filtered_units))}")
    logger.info(f"Student-branded properties: {sum(1 for u in filtered_units if u.is_student_branded)}")
    logger.info(f"Per-bed pricing detected: {sum(1 for u in filtered_units if u.is_per_bed)}")
    logger.info(f"Unfiltered data: {OUTPUT_CSV_ALL}")
    logger.info(f"Filtered data: {OUTPUT_CSV}")
    logger.info(f"Log: {LOG_FILE}")


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

  # Overnight run with no building limit
  nohup python3 -m scraper.main --headless=True --max_search_pages=100 > output/run.log 2>&1 &
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
        help='Maximum number of buildings to scrape. Default: unlimited'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(
        headless=args.headless,
        max_search_pages=args.max_search_pages,
        max_buildings=args.max_buildings
    ))