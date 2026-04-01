import os
from pathlib import Path
import kagglehub
import pandas as pd
import requests
import json
import shutil
from utility import fetch_steam_data

# Set up the extract zone for raw datasets
extract_zone = Path(os.getenv("TEMP_EXTRACT_ZONE", "/data/tmpdata"))
extract_zone.mkdir(parents=True, exist_ok=True)

api_url = os.environ.get("API_URL")

def one_extract():
    empty_extract_zone()
    extract_csv_dataset_from_kaggle()
    extract_json_dataset_from_steam()
    print("All raw datasets have been extracted to the temporary extract zone.")

# Function to empty the extract zone before downloading new datasets
def empty_extract_zone():
    for item in extract_zone.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
            
    print(f"{extract_zone} has been emptied.")

# Function to extract datasets from Kaggle as CSV files
def extract_csv_dataset_from_kaggle():
    try:
        print("Kaggle csv dataset extraction started to:", extract_zone)
        dataset_path = kagglehub.dataset_download(
            "artyomkruglov/gaming-profiles-2025-steam-playstation-xbox",
            output_dir=extract_zone,
        )
        print("Kaggle csv dataset successfully extracted to:", dataset_path)
    except Exception as e:
        print("Failed to extract Kaggle CSV dataset:", e)

# Function to extract datasets from Steam as JSON files
# This function fetches the Steam data from a local REST API endpoint, which serves the combined real and fake data, and saves it to a JSON file in the landing zone.
def extract_json_dataset_from_steam():
    steam_json_file = extract_zone / "steam_data.json"
    print("Steam json dataset extraction started to:", steam_json_file)

    steam_json_file.parent.mkdir(parents=True, exist_ok=True)

    endpoint = f"{api_url.rstrip('/')}/steam_data"

    try:
        response = requests.get(endpoint)
        response.raise_for_status()

        data = response.json()

        with open(steam_json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("Steam json dataset successfully extracted to:", steam_json_file)

    except requests.RequestException as e:
        print("Failed to fetch Steam data:", e)