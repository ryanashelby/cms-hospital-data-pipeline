"""
CMS Hospital Data Pipeline
Author: Ryan Shelby
Date: 2-28-2025
Description: 
    - Fetches hospital-related datasets from CMS API
    - Downloads & processes CSVs (only if modified)
    - Converts column names to snake case
    - Uses multiprocessing for efficiency
    - See requirements.txt
"""
import os
import requests
import pandas as pd
import re
import json
import logging
from multiprocessing import Pool
import threading

# CONFIGURATIONS
CMS_API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DATA_DIR = "cms_hospital_data"
METADATA_FILE = "metadata.json"
PROCESSES = 4  # Users up to 4 CPU cores for parallel processing.

# SETUP LOGGING
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CMS-Hospital-Data")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)


# Fetch the Dataset Metadata:
def fetch_dataset_metadata():
    try:
        response = requests.get(CMS_API_URL)
        response.raise_for_status()
        metadata = response.json()
        logger.info(f"Successfully fetched dataset metadata ({len(metadata)} datasets)")
        return metadata
    except requests.RequestException as e:
        logger.error(f"Failed to fetch metadata: {e}")
        return []


# Filter the Hospital Datasets
def filter_hospital_datasets(metadata):
    hospital_datasets = [
        dataset for dataset in metadata if "hospital" in " ".join(dataset.get("theme", [])).lower()
    ]
    logger.info(f"Found {len(hospital_datasets)} hospital-related datasets")
    return hospital_datasets


# Extract the CSV URLs
def extract_csv_urls(datasets):
    csv_urls = [
        dataset["distribution"][0]["downloadURL"]
        for dataset in datasets
        if "distribution" in dataset and dataset["distribution"] and "downloadURL" in dataset["distribution"][0]
    ]
    logger.info(f"Extracted {len(csv_urls)} valid CSV URLs")
    return csv_urls


# Load the metadata (to track the modified files)
def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as file:
            return json.load(file)
    return {}


# Save the metadata
def save_metadata(metadata):
    with open(METADATA_FILE, "w") as file:
        json.dump(metadata, file, indent=4)


# Convert the column names to snake case
def convert_to_snake_case(column_name):
    return re.sub(r'\W+', '_', column_name).lower().strip('_')


# Download & Process CSV Responses
def process_csv_reponse(file_url):
    file_name = file_url.split("/")[-1]
    local_file_path = os.path.join(DATA_DIR, file_name)
    metadata = load_metadata()
    last_modified = metadata.get(file_name, "")

    try:
        headers = requests.head(file_url).headers
        new_last_modified = headers.get("Last-Modified", "")

        # Skip if file is unchanged
        if last_modified == new_last_modified:
            logger.info(f"Skipping {file_name} (not modified)")
            return

        logger.info(f"Downloading {file_name}...")

        # Download the file
        response = requests.get(file_url)
        response.raise_for_status()

        with open(local_file_path, "wb") as file:
            file.write(response.content)

        logger.info(f"Processing {file_name}...")

        # Process the CSV
        df = pd.read_csv(local_file_path)
        df.columns = [convert_to_snake_case(col) for col in df.columns]
        df.to_csv(local_file_path, index=False)

        # Update the metadata
        metadata[file_name] = new_last_modified
        save_metadata(metadata)

        logger.info(f"Successfully processed {file_name}")

    except requests.RequestException as e:
        logger.error(f"Failed to download {file_name}: {e}")
    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")


# Main Function
def main():
    metadata = fetch_dataset_metadata()
    hospital_datasets = filter_hospital_datasets(metadata)
    csv_urls = extract_csv_urls(hospital_datasets)

    if not csv_urls:
        logger.warning("No valid CSV URLs found. Check API response structure.")
        return

    # Process files in parallel
    with Pool(processes=PROCESSES) as pool:
        pool.map(process_csv_reponse, csv_urls)

    logger.info("Completed execution.")


# Fix known threading issue in Python 3.13
def fix_known_threading_issue():
    """Gracefully handles Python 3.13 threading issue on exit."""
    try:
        for thread in threading.enumerate():
            if thread is threading.current_thread():
                continue
            thread.join(timeout=1)
        logging.info("Successfully handled known threading issue.")
    except Exception as e:
        logging.warning(f"Threading issue encountered: {e}")


# EXECUTION
if __name__ == "__main__":
    main()
    fix_known_threading_issue()  # Fix for ignored known threading exception in Python 3.13