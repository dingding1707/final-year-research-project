"""
CaribbeanJobs Web Scraper
CS/IT/AI Curriculum-NLP-Analysis Project
Scrapes job descriptions from CaribbeanJobs.com for:
- Computer Science roles
- Information Technology roles
- AI/ML roles
"""

import os
import time
import random
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

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
    # opts.add_argument("--headless")  # Uncomment to run headless
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    return driver

# ── Get Job URLs ───────────────────────────────────────────────────────────────
def get_job_urls(driver, search_term, max_pages=3):
    urls = []
    search_query = search_term.replace(" ", "+")
    url = BASE_URL.format(search_query)

    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))

        for page in range(max_pages):
            soup = BeautifulSoup(driver.page_source, "html.parser")

            # NEW selector matching current job listing structure
            job_cards = soup.select("div.job-result-title a")

            if not job_cards:
                logging.info(f"Page {page+1}: No jobs found for '{search_term}'")
                break

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

            logging.info(f"Page {page+1}: Found {len(job_cards)} jobs for '{search_term}'")

            # Try to go to next page — site doesn't have "Next" button for search results
            # CaribbeanJobs shows all jobs on one page by default
            break

    except Exception as e:
        logging.error(f"Error getting URLs for '{search_term}': {e}")

    return urls



# ── Scrape Job Metadata ────────────────────────────────────────────────────────
def scrape_job_metadata(driver, job_url, category, search_term):
    metadata = {
        "url":         job_url,
        "category":    category,
        "search_term": search_term,
        "title":       None,
        "company":     None,
        "location":    None,
        "salary":      None,
        "job_type":    None,
        "description": None,
        "posted_date": None,
        "region":      "Caribbean"
    }

    try:
        driver.get(job_url)
        
        # Wait up to 5 seconds for the description div to appear
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-description, section.description"))
            )
        except:
            logging.warning(f"Job description not loaded for {job_url}")

        soup = BeautifulSoup(driver.page_source, "html.parser")

        # ── Job Title ────────────────────────────────
        title_tag = soup.find("h1") or soup.find("h2", class_="title")
        if title_tag:
            metadata["title"] = title_tag.get_text(strip=True)

        # ── Company Name ─────────────────────────────
        company_tag = soup.find("a", class_="company-name") or soup.find("h3")
        if company_tag:
            metadata["company"] = company_tag.get_text(strip=True)

        # ── Location ─────────────────────────────────
        location_tag = soup.find("span", class_="location") or soup.find("div", class_="location")
        if location_tag:
            metadata["location"] = location_tag.get_text(strip=True)

        # ── Job Description ──────────────────────────
        desc_tag = soup.find("div", class_="job-description") or soup.find("section", class_="description")
        if desc_tag:
            metadata["description"] = desc_tag.get_text(separator=" ", strip=True)
        else:
            logging.warning(f"Job description still not found for {job_url}")

        logging.info(f"Scraped: {metadata['title']} at {metadata['company']}")

    except Exception as e:
        logging.error(f"Error scraping {job_url}: {e}")

    return metadata

# ── Save Data ──────────────────────────────────────────────────────────────────
def save_data(jobs, category, search_term):
    os.makedirs("data/raw", exist_ok=True)
    fname = search_term.lower().replace(" ", "_")
    path = f"data/raw/{category}_{fname}.csv"
    df = pd.DataFrame(jobs)
    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)
    logging.info(f"Saved {len(jobs)} jobs to {path}")
    print(f"Saved {len(jobs)} jobs to {path}")

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
                all_jobs = []
                urls = get_job_urls(driver, term, max_pages=3)
                print(f"Found {len(urls)} job URLs")
                for i, url in enumerate(urls):
                    print(f"  Scraping job {i+1}/{len(urls)}: {url[:60]}...")
                    job = scrape_job_metadata(driver, url, category, term)
                    if job["description"]:
                        all_jobs.append(job)
                    time.sleep(random.uniform(1, 3))
                if all_jobs:
                    save_data(all_jobs, category, term)
                    print(f"Saved {len(all_jobs)} jobs for '{term}'")
                else:
                    print(f"No valid jobs found for '{term}'")
                time.sleep(random.uniform(3, 6))
    finally:
        driver.quit()
        print("\nScraping complete! Driver closed.")
        print("Check data/raw/ for your CSV files.")

if __name__ == "__main__":
    main()
