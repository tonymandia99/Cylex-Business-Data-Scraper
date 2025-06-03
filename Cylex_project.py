import csv
import time
import random
import os
import gc
import requests
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc


# --- CONFIGURATION ---
base_urls = [
    "https://www.cylex.us.com/s?q=plumbers&c=Salt%20Lake%20City&z=&p={page}&dst=&sUrl=&cUrl=",
    "https://www.cylex.us.com/s?q=plumbers&c=Provo&z=&p={page}&dst=&sUrl=&cUrl="
    # add more URLs here as needed
]

proxy_list = [
    "38.154.197.225:6891",
    "154.30.251.252:5393",
    "142.147.131.223:6123",
    "107.173.93.96:6050",
    "142.147.132.60:6255",
    "173.239.219.6:5915",
    "154.6.129.78:5548",
    "103.3.226.63:6339",
    "46.202.224.75:5627",
    "23.27.210.240:6610",
    "192.186.172.63:9063",
    "142.147.131.183:6083",
    "23.27.208.140:5850",
    "142.147.131.144:6044",
    "23.27.209.24:6043",
    "64.64.115.6:5641",
    "64.64.110.223:6746"
]

proxy_rotation_interval = 5
proxy_rotation_max_pages = 1000
restart_interval_minutes = 180
save_interval = 50
last_page_file = "last_page_cylex.txt"
output_file = f"cylex_results_{datetime.now().strftime('%Y%m%d')}.csv"

all_data = set()
entry_count = 0
proxy_index = 0


def log(msg, tag="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{tag}] {timestamp} - {msg}"
    print(line)
    with open("scraper.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_existing_data():
    global all_data, entry_count
    if os.path.exists(output_file):
        with open(output_file, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    all_data.add(
                        (row[0].strip(), row[1].strip(), row[2].strip()))
        entry_count = len(all_data)
        log(f"Loaded {entry_count} existing entries", "RESUME")


def load_last_page():
    if os.path.exists(last_page_file):
        with open(last_page_file, "r") as f:
            line = f.read().strip()
            if line.isdigit():
                return int(line)
    return 1


def save_last_page(page_num):
    with open(last_page_file, "w") as f:
        f.write(str(page_num))


def save_data():
    with open(output_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Business Name", "Phone Number", "Address"])
        for row in all_data:
            writer.writerow(row)
    log(f"Saved {len(all_data)} unique records", "SAVE")


def create_driver(proxy_ip=None):
    options = uc.ChromeOptions()
    options.headless = False  # disable headless for debugging
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 2,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.plugins": 2,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.notifications": 2
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--window-size=1200,800")
    options.add_argument("--remote-debugging-port=9222")
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def measure_proxy_latency(proxy_ip, timeout=5):
    test_url = "http://example.com"
    try:
        start = time.time()
        requests.get(test_url, timeout=timeout)
        latency_ms = int((time.time() - start) * 1000)
        return latency_ms
    except Exception:
        return -1


def get_fast_proxies(proxy_list, max_latency=3000):
    # Simple version: just return the proxy list as-is (or filter by latency if you want)
    log(f"Using {len(proxy_list)} proxies", "PROXY")
    return proxy_list


def scrape_page(driver):
    global entry_count
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, ".h4.bold.d-inline-block.pr-4 > a"))
    )
    name_elements = driver.find_elements(
        By.CSS_SELECTOR, ".h4.bold.d-inline-block.pr-4 > a")
    phone_elements = driver.find_elements(By.CSS_SELECTOR, ".lm-ph span")
    address_elements = driver.find_elements(By.CSS_SELECTOR, ".pl-3.addr")

    new_entries_count = 0
    for i in range(min(len(name_elements), len(phone_elements), len(address_elements))):
        name = name_elements[i].text.strip()
        phone = phone_elements[i].text.strip()
        address = address_elements[i].text.strip()
        if name and phone and address:
            exists = any(phone == entry[1] for entry in all_data)
            if not exists:
                all_data.add((name, phone, address))
                entry_count += 1
                new_entries_count += 1
                log(f"{name} | {phone} | {address}", "DATA")
                if entry_count % save_interval == 0:
                    save_data()
    return new_entries_count


def run_scraper():
    global proxy_index, entry_count
    load_existing_data()
    proxies = get_fast_proxies(proxy_list)
    if proxies:
        current_proxy = proxies[proxy_index % len(proxies)]
    else:
        current_proxy = None
        log("Proxy usage disabled; running without proxies.", "PROXY")

    pages_since_proxy_switch = 0
    total_pages_scraped = 0
    start_time = datetime.now()

    # Loop through all base URLs
    for base_url in base_urls:
        current_page_num = 1
        consecutive_no_data_pages = 0
        log(f"Starting scraping for base URL: {base_url}", "START")

        while True:  # Loop pages indefinitely until 3 consecutive no data pages
            elapsed = datetime.now() - start_time
            if elapsed > timedelta(minutes=restart_interval_minutes):
                log("Restart interval reached. Exiting (no auto-restart on Windows).", "RESTART")
                return

            if proxies and (pages_since_proxy_switch >= proxy_rotation_interval or total_pages_scraped >= proxy_rotation_max_pages):
                proxy_index = (proxy_index + 1) % len(proxies)
                current_proxy = proxies[proxy_index]
                pages_since_proxy_switch = 0
                total_pages_scraped = 0
                log(f"Rotated proxy to {current_proxy}", "PROXY")

            driver = None
            try:
                page_url = base_url.format(page=current_page_num)
                driver = create_driver(current_proxy)
                log(f"Scraping URL: {page_url} using proxy {current_proxy}", "START")
                driver.get(page_url)
                time.sleep(random.uniform(1.0, 2.0))

                new_entries = scrape_page(driver)

                if new_entries == 0:
                    consecutive_no_data_pages += 1
                    log(
                        f"No new data found on page {current_page_num}. Consecutive no-data pages: {consecutive_no_data_pages}", "INFO")
                else:
                    consecutive_no_data_pages = 0  # reset counter on new data

                if consecutive_no_data_pages >= 3:
                    log("3 consecutive pages with no new data scraped. Stopping scraper for this URL.", "STOP")
                    save_data()
                    save_last_page(current_page_num)
                    driver.quit()
                    gc.collect()
                    break

                current_page_num += 1
                save_last_page(current_page_num)

                driver.quit()
                gc.collect()
                pages_since_proxy_switch += 1
                total_pages_scraped += 1

            except Exception as e:
                log(f"Error scraping page: {e}", "ERROR")
                try:
                    if driver:
                        driver.quit()
                except:
                    pass
                gc.collect()
                log("Retrying after error...", "RETRY")
                time.sleep(2)

    save_data()
    log(f"Scraping complete. Total unique entries: {entry_count}", "DONE")


if __name__ == "__main__":
    run_scraper()
