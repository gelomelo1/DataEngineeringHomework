import os
from pathlib import Path
import kagglehub
import pandas as pd
import requests
import json
import shutil
from utility import normalize_title, fetch_steam_data

# Set up the landing zone for raw datasets
landing_zone = Path(os.getenv("LANDING_ZONE", "/app/rawdata"))
landing_zone.mkdir(parents=True, exist_ok=True)

# Create a subdirectory for CSV datasets
csv_zone = landing_zone / "csv"
csv_zone.mkdir(parents=True, exist_ok=True)

# Create a subdirectory for JSON datasets
json_zone = landing_zone / "json"
json_zone.mkdir(parents=True, exist_ok=True)

api_url = os.environ.get("API_URL")

def download_raw_datasources():
    empty_landing_zone()
    download_csv_dataset_from_kaggle()
    download_json_dataset_from_steam()
    print("All raw datasets have been downloaded to the landing zone.")

# Function to empty the landing zone before downloading new datasets
def empty_landing_zone():
    for item in landing_zone.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
            
    print(f"{landing_zone} has been emptied.")

# Function to download datasets from Kaggle as CSV files
def download_csv_dataset_from_kaggle():
    try:
        csv_zone.mkdir(parents=True, exist_ok=True)
        print("Kaggle csv dataset download started to:", csv_zone)
        dataset_path = kagglehub.dataset_download(
            "artyomkruglov/gaming-profiles-2025-steam-playstation-xbox",
            output_dir=csv_zone,
        )
        print("Kaggle csv dataset successfully downloaded to:", dataset_path)
    except Exception as e:
        print("Failed to download Kaggle CSV dataset:", e)

# Function to download datasets from Steam as JSON files
# This function reads the game IDs from the previously downloaded CSV file and fetches detailed information for each game using the Steam API, saving the results in a JSON file.
def download_json_dataset_from_steam_real():
    steam_json_file = json_zone / "steam_data.json"
    print("Steam json dataset download started to:", steam_json_file)
    steam_df = pd.read_csv(csv_zone / "steam" / "games.csv")
    playstation_df = pd.read_csv(csv_zone / "playstation" / "games.csv")
    xbox_df = pd.read_csv(csv_zone / "xbox" / "games.csv")

    steam_df = steam_df.dropna(subset=["title"])
    playstation_df = playstation_df.dropna(subset=["title"])
    xbox_df = xbox_df.dropna(subset=["title"])

    steam_df["norm_title"] = steam_df["title"].apply(normalize_title)
    playstation_df["norm_title"] = playstation_df["title"].apply(normalize_title)
    xbox_df["norm_title"] = xbox_df["title"].apply(normalize_title)

    common_titles = set(steam_df["norm_title"]) & set(playstation_df["norm_title"]) & set(xbox_df["norm_title"])
    steam_common_df = steam_df[steam_df["norm_title"].isin(common_titles)]
    steam_common_df = steam_common_df.drop_duplicates(subset=["norm_title"])

    steam_common_ids = steam_common_df["gameid"].tolist()

    fetch_steam_data(steam_common_ids, steam_json_file)
    
    print("Steam json dataset successfully downloaded to:", steam_json_file)

# Function to download datasets from Steam as JSON files
# This function fetches the Steam data from a local REST API endpoint, which serves the combined real and fake data, and saves it to a JSON file in the landing zone.
def download_json_dataset_from_steam():
    steam_json_file = json_zone / "steam_data.json"
    print("Steam json dataset download started to:", steam_json_file)

    steam_json_file.parent.mkdir(parents=True, exist_ok=True)

    endpoint = f"{api_url.rstrip('/')}/steam_data"

    try:
        response = requests.get(endpoint)
        response.raise_for_status()

        data = response.json()

        with open(steam_json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("Steam json dataset successfully downloaded to:", steam_json_file)

    except requests.RequestException as e:
        print("Failed to fetch Steam data:", e)