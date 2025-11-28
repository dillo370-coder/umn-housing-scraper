# UMN Housing Scraper

Playwright-based scraper that extracts apartment listings (apartments.com) around the UMN campus.

Quick start
1. Setup Python environment
   python3 -m pip install --user -r requirements.txt
   python3 -m playwright install chromium

2. Smoke test (visible):
   python3 -m scraper.main --headless=False --max_search_pages=1 --max_buildings=1

3. Full headless run (background example):
   nohup bash -c "python3 -m scraper.main --headless=True --max_search_pages=200 --max_buildings=1000" > output/overnight_run.log 2>&1 &

Files
- scraper/main.py — main async scraper (already in repo)
- output/ — runtime CSV and logs (ignored by git)

Notes
- Add API keys (if needed) to CI / runner secrets, never commit them.
- Consider running CI on a self-hosted runner for heavy scraping.