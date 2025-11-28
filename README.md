# UMN Housing Scraper

Playwright-based web scrapers that extract apartment listings from multiple sources around the University of Minnesota – Twin Cities campus for housing economics research.

## Overview

This project includes two scrapers:

1. **Apartments.com Scraper** (`scraper/main.py`) - Scrapes the commercial apartments.com website
2. **UMN Listings Scraper** (`scraper/umn_listings.py`) - Scrapes the official UMN Off-Campus Marketplace (listings.umn.edu)

Both scrapers collect address-level apartment listing data for hedonic housing analysis, focusing on properties within 10 km of the UMN East Bank campus (44.9731, -93.2359). They handle special cases like student housing pricing (per-bed, shared bedrooms) and capture building-level amenity data.

## Features

- Extracts 1-2 representative units per building (1-bed and 2-bed preferred)
- Handles student housing "per-bed" and "From $X" pricing
- Captures building amenities (laundry, gym, parking, etc.)
- Geocodes addresses using OpenStreetMap Nominatim
- Filters results by distance to UMN campus (10 km radius)
- Exports R-friendly CSV with lowercase_snake_case columns
- **Auto-restart mode**: Automatically runs multiple sessions with cooldowns, deduplicates results, and accumulates listings until target reached
- **UMN Listings support**: Scrape the official UMN student housing marketplace

## Quick Start

### 1. Install Dependencies

```bash
# Install Python packages
python3 -m pip install --user -r requirements.txt

# Install Playwright browser
python3 -m playwright install chromium
```

### 2. Test Run (visible browser, limited scope)

```bash
python3 -m scraper.main --headless=False --max_search_pages=1 --max_buildings=2
```

### 3. Full Scrape (headless, larger scope)

```bash
python3 -m scraper.main --headless=True --max_search_pages=50 --max_buildings=800
```

## UMN Listings Scraper (listings.umn.edu)

The UMN Listings scraper collects listings from the official University of Minnesota Off-Campus Housing Marketplace. This is a separate data source with student-focused listings.

### Run UMN Listings Scraper

```bash
# Test run (visible browser)
python3 -m scraper.umn_listings --headless=False

# Full headless run
python3 -m scraper.umn_listings --headless=True

# Limit number of listings
python3 -m scraper.umn_listings --headless=False --max_listings=10
```

### UMN Listings Output

- `output/umn_listings_data_{timestamp}.csv` - Session data
- `output/umn_listings_combined.csv` - All unique listings accumulated
- `output/umn_listings_log_{timestamp}.log` - Log file

### 4. Auto-Restart Mode (recommended for large datasets)

The auto-restart mode automatically:
- Runs multiple scraping sessions with cooldowns between them
- Skips buildings already scraped in previous sessions
- Deduplicates results and accumulates them in a single combined CSV
- Stops when target number of listings is reached

```bash
# Run with auto-restart (5 sessions, 5-min cooldowns, target 1000 listings)
python3 -m scraper.main --auto_restart --max_sessions=5 --session_cooldown=300 --target_listings=1000

# Overnight auto-restart (10 sessions, 2000 listing target)
nohup python3 -m scraper.main --auto_restart --headless=True --max_sessions=10 --target_listings=2000 > output/auto.log 2>&1 &
```

## Command-Line Options

### Basic Options

| Option | Default | Description |
|--------|---------|-------------|
| `--headless` | `True` | Run browser in headless mode (True/False) |
| `--max_search_pages` | `50` | Maximum search result pages to scrape |
| `--max_buildings` | unlimited | Maximum buildings to scrape per session |

### Auto-Restart Options

| Option | Default | Description |
|--------|---------|-------------|
| `--auto_restart` | off | Enable auto-restart mode |
| `--max_sessions` | `50` | Maximum number of sessions to run |
| `--session_cooldown` | `600` | Seconds to wait between sessions (10 min default) |
| `--target_listings` | `1000` | Stop when this many listings are collected |

## Output

All output files are saved to `output/` (git-ignored):

### Per-Session Files
- `umn_housing_data_{timestamp}.csv` - Filtered data from this session (within 10 km of UMN)
- `umn_housing_ALL_{timestamp}.csv` - All scraped data from this session (unfiltered)
- `scraper_log_{timestamp}.log` - Detailed log file

### Accumulated Files (for auto-restart mode)
- `umn_housing_combined.csv` - **All unique listings** accumulated across sessions (deduplicated)
- `scraped_urls.txt` - Tracking file for buildings already scraped (prevents duplicates)

### CSV Schema

**Identification/Location:**
- `listing_id`, `building_name`, `full_address`, `street`, `city`, `state`, `zip`
- `lat`, `lon`, `dist_to_campus_km`

**Unit Characteristics:**
- `unit_label`, `beds`, `baths`, `sqft`
- `rent_raw`, `rent_min`, `rent_max`
- `price_type` (per_unit, per_bed, range, from_price, unknown)
- `is_per_bed`, `is_shared_bedroom`

**Building Characteristics:**
- `year_built`, `num_units`, `building_type`, `stories`

**Amenity Flags:**
- `has_in_unit_laundry`, `has_on_site_laundry`, `has_dishwasher`, `has_ac`
- `has_heat_included`, `has_water_included`, `has_internet_included`
- `is_furnished`, `has_gym`, `has_pool`, `has_rooftop_or_clubroom`
- `has_parking_available`, `has_garage`, `pets_allowed`
- `is_student_branded`

## R Integration

```r
library(readr)
library(dplyr)

# Load the combined data (accumulated across sessions)
df <- read_csv("output/umn_housing_combined.csv")

# Or load a specific session's data
df <- read_csv("output/umn_housing_data_YYYYMMDD_HHMMSS.csv")

# Example: Filter to Dinkytown area (within 1 km of campus)
dinkytown <- df %>% filter(dist_to_campus_km <= 1.0)

# Example: Compare student vs non-student housing
df %>%
  group_by(is_student_branded) %>%
  summarise(
    mean_rent = mean(rent_min, na.rm = TRUE),
    n = n()
  )
```

## Files

- `scraper/main.py` — Apartments.com async scraper
- `scraper/umn_listings.py` — UMN Listings (listings.umn.edu) scraper
- `output/` — Runtime CSV and logs (git-ignored)
- `requirements.txt` — Python dependencies

## Notes

- Respects rate limits with configurable delays between requests
- Uses polite geocoding with Nominatim (includes email in requests)
- Handles missing data gracefully (NA values for missing fields)
- Student housing detection via keyword matching in property descriptions
- Auto-restart mode handles bot detection by pausing between sessions

## Troubleshooting

**"Access Denied" or bot detection:**
- Use `--headless=False` to appear more like a real browser
- Increase `--session_cooldown` to give more time between sessions
- Run during off-peak hours (late night/early morning)
- Try a different network (some are blocked more aggressively)

**0 units scraped:**
- Check the log file for specific errors
- The site may be blocking - wait and try again later
- Use auto-restart mode which handles this automatically

## License

See [LICENSE](LICENSE) for details.