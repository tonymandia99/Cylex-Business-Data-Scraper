# Cylex Business Data Scraper

Automated web scraper to collect business data (name, phone, address) from [Cylex.us.com](https://www.cylex.us.com) for multiple locations, with proxy rotation, deduplication, and robust error handling.

---

## Features

- **Multi-location support:** Scrapes multiple city URLs sequentially.
- **Dynamic pagination:** Automatically continues scraping until 3 consecutive pages yield no new data.
- **Proxy rotation:** Supports proxy list with automatic rotation to avoid IP bans.
- **Undetected ChromeDriver:** Uses `undetected_chromedriver` to evade bot detection.
- **Data deduplication:** Avoids duplicate phone numbers across sessions.
- **Persistent storage:** Saves results in CSV, loads previous data on restart.
- **Error handling & retries:** Handles Selenium timeouts and other exceptions gracefully.
- **Configurable:** Easily add more URLs, proxies, and adjust intervals.
- **Logging:** Detailed logs of scraping progress, errors, and data entries.
- **Controlled runtime:** Automatically stops after a set time interval to avoid indefinite runs.

---

## Requirements

- Python 3.8+
- `selenium`
- `undetected_chromedriver`
- `requests`

Install dependencies with:

```bash
pip install selenium undetected-chromedriver requests
