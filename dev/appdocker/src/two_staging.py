import os
from pathlib import Path
import kagglehub
import pandas as pd
import requests
import json
import shutil
from utility import fetch_steam_data

# Set up the landing zone for raw datasets
landing_zone = Path(os.getenv("LANDING_ZONE", "/data/rawdata"))
landing_zone.mkdir(parents=True, exist_ok=True)

# Set up the extract zone for raw datasets
extract_zone = Path(os.getenv("TEMP_EXTRACT_ZONE", "/data/tmpdata"))
extract_zone.mkdir(parents=True, exist_ok=True)

api_url = os.environ.get("API_URL")

def two_staging():
    empty_landing_zone()
    copy_csv_data_from_extract_zone_to_landing_zone()
    copy_json_data_from_extract_zone_to_landing_zone()
    print("All raw datasets have been staged to the landing zone.")

# Function to copy csv datasets from the extract zone to the landing zone
def copy_csv_data_from_extract_zone_to_landing_zone():
    try:
        print("Csv datasets copy started to:", landing_zone)

        platforms = ["steam", "playstation", "xbox"]
        files = ["games.csv", "prices.csv", "purchased_games.csv"]

        for platform in platforms:
            src_base = extract_zone / platform
            dest_base = landing_zone / platform / "csv"

            dest_base.mkdir(parents=True, exist_ok=True)

            for file in files:
                src_file = src_base / file
                dest_file = dest_base / file

                if src_file.exists():
                    shutil.copy2(src_file, dest_file)
                    print(f"Copied: {src_file} → {dest_file}")
                else:
                    print(f"WARNING: Missing file {src_file}")

        print("Csv datasets successfully copied to:", landing_zone)

    except Exception as e:
        print("Failed to copy csv datasets:", e)

# Function to copy json datasets from the extract zone to the landing zone
def copy_json_data_from_extract_zone_to_landing_zone():
    try:
        print("Json datasets copy started to:", landing_zone)

        src_file = extract_zone / "steam_data.json"
        dest_dir = landing_zone / "steam" / "json"
        dest_file = dest_dir / "steam_data.json"

        dest_dir.mkdir(parents=True, exist_ok=True)

        if src_file.exists():
            shutil.copy2(src_file, dest_file)
            print(f"Copied: {src_file} → {dest_file}")
        else:
            print(f"WARNING: Missing file {src_file}")

        print("Json datasets successfully copied to:", landing_zone)

    except Exception as e:
        print("Failed to copy json datasets:", e)

# Function to empty the landing zone before downloading new datasets
def empty_landing_zone():
    for item in landing_zone.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
            
    print(f"{landing_zone} has been emptied.")