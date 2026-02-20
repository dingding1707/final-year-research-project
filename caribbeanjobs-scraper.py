"""
CaribbeanJobs Web Scraper
CS/IT/AI Curriculum-NLP-Analysis Project
Scrapes job descriptions from CaribbeanJobs.com
Saves ALL jobs into one JSON file
"""

import os
import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json
from pathlib import Path

# GLOBAL JOB STORAGE
all_jobs = []

# ── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ── Search Terms ──────────────────────────────────────────────────────────────
SEARCH_TERMS = {
    "computer_science": [
        "Software Engineer", "Software Developer", "Backend Developer", "Frontend Developer", "Full Stack Developer",
        "Mobile Application Developer", "Game Developer", "Embedded Systems Engineer", "DevOps Engineer", "Cloud Engineer"
    ],
    "information_technology": [
        "IT Support Specialist", "Help Desk Technician", "Network Administrator", "System Administrator", "Cybersecurity Analyst",
        "Information Security Analyst", "Database Administrator", "IT Project Manager", "IT Operations Analyst", "Infrastructure Engineer"
    ],
    "ai_ml": [
        "Data Scientist", "Machine Learning Engineer", "AI Engineer", "Data Analyst", "Business Intelligence Analyst",
        "NLP Engineer", "Computer Vision Engineer", "Data Engineer", "MLOps Engineer", "Applied Scientist"
    ]
}

BASE_URL = "https://www.caribbeanjobs.com/ShowResults.aspx?Keywords={}&autosuggest=False"

# ── Driver Setup ───────────────────────────────────────────────────────────────
def setup_driver():
    opts = Options()
    # opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/53736"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=opts)

# ── Get Job URLs ───────────────────────────────────────────────────────────────
def get_job_urls(driver, search_term, max_pages=3):
    urls = []
    search_query = search_term.replace(" ", "+")
    url = BASE_URL.format(search_query)

    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))

        soup = BeautifulSoup(driver.page_source, "html.parser")

        job_cards = soup.select("div.job-result-title a")

        if not job_cards:
            logging.info(f"No jobs found for '{search_term}'")
            return urls

        for card in job_cards:
            href = card.get("href")
            if href:
                full_url = (
                    "https://www.caribbeanjobs.com" + href
                    if href.startswith("/")
                    else href
                )
                if full_url not in urls:
                    urls.append(full_url)

        logging.info(f"Found {len(urls)} jobs for '{search_term}'")

    except Exception as e:
        logging.error(f"Error getting URLs for '{search_term}': {e}")

    return urls


# ── Scrape Job Metadata ────────────────────────────────────────────────────────
def scrape_job_metadata(driver, job_url, category, search_term):

    metadata = {
        "url": job_url,
        "category": category,
        "search_term": search_term,
        "title": None,
        "company": None,
        "location": None,
        "salary": None,
        "job_type": None,
        "description": None,
        "posted_date": None,
        "region": "Caribbean"
    }

    try:
        driver.get(job_url)

        # WAIT LONGER FOR DESCRIPTION (site loads dynamically)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-description, section.description, #job-description"))
            )
        except:
            logging.warning(f"Job description not loaded for {job_url}")

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # TITLE
        title_tag = soup.find("h1") or soup.find("h2")
        if title_tag:
            metadata["title"] = title_tag.get_text(strip=True)

        # COMPANY
        company_tag = soup.find("a", class_="company-name") or soup.find("h3")
        if company_tag:
            metadata["company"] = company_tag.get_text(strip=True)

        # LOCATION
        location_tag = soup.select_one(".location")
        if location_tag:
            metadata["location"] = location_tag.get_text(strip=True)

        # DESCRIPTION (multiple fallback selectors)
        desc_tag = (
            soup.find("div", class_="job-description")
            or soup.find("section", class_="description")
            or soup.find(id="job-description")
            or soup.select_one(".job-body")
        )

        if desc_tag:
            metadata["description"] = desc_tag.get_text(separator=" ", strip=True)

        logging.info(f"Scraped: {metadata['title']}")

    except Exception as e:
        logging.error(f"Error scraping {job_url}: {e}")

    return metadata


# ── SAVE ALL JOBS TO SINGLE JSON ───────────────────────────────────────────────
def save_all_jobs_json():

    output_path = Path("data/raw/jobs.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_jobs, f, indent=4, ensure_ascii=False)

    print(f"\nSaved {len(all_jobs)} TOTAL jobs to {output_path}")
    logging.info(f"Saved {len(all_jobs)} total jobs to JSON")


# ── Main Scraper ───────────────────────────────────────────────────────────────
def main():

    driver = setup_driver()
    print("Driver started successfully!")

    try:

        for category, terms in SEARCH_TERMS.items():

            print(f"\n{'='*50}")
            print(f"Scraping category: {category.upper()}")
            print(f"{'='*50}")

            for term in terms:

                print(f"\nSearching for: '{term}'")

                urls = get_job_urls(driver, term)

                print(f"Found {len(urls)} job URLs")

                for i, url in enumerate(urls):

                    print(f"  Scraping job {i+1}/{len(urls)}")

                    job = scrape_job_metadata(driver, url, category, term)

                    if job["description"]:
                        all_jobs.append(job)

                    time.sleep(random.uniform(1, 3))

                time.sleep(random.uniform(3, 6))

    finally:
        driver.quit()
        print("\nScraping complete! Driver closed.")

        # SAVE EVERYTHING HERE
        save_all_jobs_json()


if __name__ == "__main__":
    main()
