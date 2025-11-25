# UMN Housing Scraper

Playwright-based web scraper that extracts apartment listings from Apartments.com around the University of Minnesota – Twin Cities campus for housing economics research.

## Overview

This scraper collects address-level apartment listing data for hedonic housing analysis, focusing on properties within 10 km of the UMN East Bank campus (44.9731, -93.2359). It handles special cases like student housing pricing (per-bed, shared bedrooms) and captures building-level amenity data.

## Features

- Extracts 1-2 representative units per building (1-bed and 2-bed preferred)
- Handles student housing "per-bed" and "From $X" pricing
- Captures building amenities (laundry, gym, parking, etc.)
- Geocodes addresses using OpenStreetMap Nominatim
- Filters results by distance to UMN campus (10 km radius)
- Exports R-friendly CSV with lowercase_snake_case columns

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

### 4. Overnight Run (background)

```bash
nohup python3 -m scraper.main --headless=True --max_search_pages=100 > output/overnight.log 2>&1 &
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--headless` | `True` | Run browser in headless mode (True/False) |
| `--max_search_pages` | `50` | Maximum search result pages to scrape |
| `--max_buildings` | unlimited | Maximum buildings to scrape |

## Output

All output files are saved to `output/` (git-ignored):

- `umn_housing_data_{timestamp}.csv` - Filtered data (within 10 km of UMN)
- `umn_housing_ALL_{timestamp}.csv` - All scraped data (unfiltered)
- `scraper_log_{timestamp}.log` - Detailed log file

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

# Load the data
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

- `scraper/main.py` — Main async scraper
- `output/` — Runtime CSV and logs (git-ignored)
- `requirements.txt` — Python dependencies

## Notes

- Respects rate limits with configurable delays between requests
- Uses polite geocoding with Nominatim (includes email in requests)
- Handles missing data gracefully (NA values for missing fields)
- Student housing detection via keyword matching in property descriptions

## License

See [LICENSE](LICENSE) for details.