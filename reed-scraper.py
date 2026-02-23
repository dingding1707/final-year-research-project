"""
Reed UK Job Scraper
Scrapes ICT job descriptions from reed.co.uk
Outputs ONE JSON file
"""

import json
import time
import random
import logging
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup


# ---------- LOGGING ----------
logging.basicConfig(
    filename="reed_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- SEARCH TERMS ----------
SEARCH_TERMS = {
    "computer_science": [
        "Software Engineer"
    ],
    "information_technology": [
        "IT Support Specialist"
    ],
    "ai_ml": [
        "Data Scientist"
    ],
    "control_non_ict": [
        "Registered Nurse"
    ]
}



BASE_URL = "https://www.reed.co.uk/jobs/{}-jobs?pageno={}"


# ---------- DRIVER ----------
def setup_driver():

    import tempfile, os, shutil, uuid, logging
    opts = Options()
    opts.add_argument("--headless=new")  # Use new headless mode
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # Use a unique temp directory for Chrome user data each run
    temp_profile = os.path.join(tempfile.gettempdir(), f"chrome_tmp_profile_{uuid.uuid4().hex}")
    if os.path.exists(temp_profile):
        try:
            shutil.rmtree(temp_profile)
        except Exception as e:
            print(f"Could not remove old temp profile: {temp_profile}\n{e}")
            logging.error(f"Could not remove old temp profile: {temp_profile} {e}")
    os.makedirs(temp_profile, exist_ok=True)
    opts.add_argument(f"--user-data-dir={temp_profile}")

    try:
        driver = webdriver.Chrome(options=opts)
        return driver
    except Exception as e:
        print(f"Failed to start ChromeDriver. Temp profile: {temp_profile}\nError: {e}")
        logging.error(f"Failed to start ChromeDriver. Temp profile: {temp_profile} Error: {e}")
        raise


# ---------- GET JOB LINKS ----------
def get_job_urls(driver, term, pages=2):

    urls = []

    keyword = term.lower().replace(" ", "-")

    for page in range(1, pages + 1):

        url = BASE_URL.format(keyword, page)
        print("Opening:", url)

        driver.get(url)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
        except:
            logging.warning(f"No listings loaded for {term} page {page}")
            continue

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Reed job cards contain links like /jobs/.../12345678
        for a in soup.select("article a[href*='/jobs/']"):

            link = a.get("href")

            if link and "/jobs/" in link:

                if link.startswith("/"):
                    link = "https://www.reed.co.uk" + link

                if link not in urls:
                    urls.append(link)

        print(f"Found {len(urls)} links so far")
        time.sleep(random.uniform(2,4))

    return urls


# ---------- SCRAPE JOB ----------
def scrape_job(driver, url, term):

    job = {
        "url": url,
        "searched_role": term,
        "title": None,
        "company": None,
        "location": None,
        "description": None,
        "source": "reed"
    }

    driver.get(url)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except:
        logging.warning(f"Page did not load: {url}")
        return job

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # TITLE
    title = soup.find("h1")
    if title:
        job["title"] = title.get_text(strip=True)

    # COMPANY
    company = soup.select_one("[data-qa='company-name'], .company")
    if company:
        job["company"] = company.get_text(strip=True)

    # LOCATION
    location = soup.select_one("[data-qa='job-location'], .location")
    if location:
        job["location"] = location.get_text(strip=True)

    # DESCRIPTION  ⭐ IMPORTANT
    desc = (
        soup.select_one("#jobDescription")
        or soup.select_one(".job-description")
        or soup.select_one("[data-qa='job-description']")
        or soup.find("main")
    )

    if desc:
        job["description"] = desc.get_text(" ", strip=True)
    else:
        logging.warning(f"No description for {url}")

    print("Collected:", job["title"])

    return job


# ---------- SAVE JSON ----------
def save_json(jobs):

    Path("data").mkdir(exist_ok=True)

    with open("data/reed_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    print("Saved", len(jobs), "jobs to data/reed_jobs.json")


# ---------- MAIN ----------
def main():

    driver = setup_driver()
    all_jobs = []

    try:

        # loop categories
        for category, titles in SEARCH_TERMS.items():

            print(f"\nCATEGORY: {category}")

            # loop actual job titles
            for term in titles:

                print("Searching:", term)

                urls = get_job_urls(driver, term, pages=2)
                print("Total URLs:", len(urls))

                for link in urls:

                    try:
                        job = scrape_job(driver, link, term)

                        if job["description"]:
                            job["category"] = category   # preserve category
                            all_jobs.append(job)

                        time.sleep(random.uniform(1,3))

                    except Exception as e:
                        logging.error(str(e))

                time.sleep(random.uniform(3,6))

    finally:
        driver.quit()
        save_json(all_jobs)
        print("Done")

if __name__ == "__main__":
    main()
