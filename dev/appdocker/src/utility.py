import re
import string
import json
import time
import requests
from pathlib import Path

# I used this function to fetch Steam data with retry logic and rate limit handling. I dont use this in the homework pipeline, becasue it takes a long time to run
def fetch_steam_data(game_ids, output_file: Path):
    all_data = {}
    failed_ids = []

    if output_file.exists():
        with open(output_file, "r") as f:
            all_data = json.load(f)

    remaining_ids = [gid for gid in game_ids if str(gid) not in all_data]

    print(f"Starting fetch for {len(remaining_ids)} Steam games...")

    for i, game_id in enumerate(remaining_ids, start=1):
        if i % 200 == 0:
            print("Reached 200 requests, pausing for 2 minutes...")
            time.sleep(120)

        str_game_id = str(game_id)
        url = f"https://store.steampowered.com/api/appdetails?appids={game_id}&filters=basic,price_overview,genres"

        success = False

        for attempt in range(1, 6):
            try:
                response = requests.get(url, timeout=10)

                if response.status_code == 429:
                    raise Exception("429 Too Many Requests")

                response.raise_for_status()
                data = response.json()
                all_data[str_game_id] = data[str_game_id]

                success = True
                break

            except Exception as e:
                print(f"[Attempt {attempt}/5] Failed for {game_id}: {e}")

                if attempt < 5:
                    wait_time = attempt * 60  # 1,2,3,4 minutes
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)

        if not success:
            failed_ids.append(game_id)
            print(f"Giving up on {game_id} after 5 attempts.")

        # Save progress every 50
        if i % 50 == 0 or i == len(remaining_ids):
            with open(output_file, "w") as f:
                json.dump(all_data, f, indent=2)
            print(f"Progress saved: {i}/{len(remaining_ids)}")

        time.sleep(0.1)

    success_count = len(all_data)
    failed_count = len(failed_ids)

    print("\n=== FINAL SUMMARY ===")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")

    return {
        "success_count": success_count,
        "failed_count": failed_count,
        "failed_ids": failed_ids
    }