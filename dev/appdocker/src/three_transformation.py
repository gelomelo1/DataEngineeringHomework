import os
import pandas as pd
import json
from pathlib import Path
import string

landing_zone = Path(os.getenv("LANDING_ZONE", "/app/rawdata"))
landing_zone.mkdir(parents=True, exist_ok=True)

def three_transformation():
    print("Starting transformation of raw datasets into a single clean dataset...")
    dfs = load_game_datas_into_dataframes()

    common_games = get_common_game_ids(dfs)

    print(f"Found {len(common_games)} common games across platforms")

    output_file = landing_zone / "common_games.json"

    with open(output_file, "w") as f:
        json.dump(common_games, f, indent=2)

    print(f"Saved common games to: {output_file}")
    print("Finished transformation of raw datasets into a single clean dataset...")

# Get the common game ids from the given dataframes
# Return type: [{gameName: "game1" (come from steam csv), steamId: 123, playstationIds: [1234, 1235], xboxIds: [12345]}, ...]
def get_common_game_ids(dfs):
    # Extract dataframes
    steam_csv_df = next(d["data"] for d in dfs if d["platform"] == "steam" and d["fromType"] == "csv")
    steam_json_df = next(d["data"] for d in dfs if d["platform"] == "steam" and d["fromType"] == "json")
    ps_df = next(d["data"] for d in dfs if d["platform"] == "playstation")
    xbox_df = next(d["data"] for d in dfs if d["platform"] == "xbox")

    # Drop missing titles
    steam_csv_df = steam_csv_df.dropna(subset=["title"])
    ps_df = ps_df.dropna(subset=["title"])
    xbox_df = xbox_df.dropna(subset=["title"])

    # Normalize titles
    steam_csv_df["norm_title"] = steam_csv_df["title"].apply(normalize_title)
    ps_df["norm_title"] = ps_df["title"].apply(normalize_title)
    xbox_df["norm_title"] = xbox_df["title"].apply(normalize_title)

    # Extract valid Steam IDs from JSON
    steam_json_ids = set(steam_json_df["gameid"].astype(str))

    # Group PS and Xbox (many IDs per title)
    ps_grouped = ps_df.groupby("norm_title")["gameid"].apply(list).to_dict()
    xbox_grouped = xbox_df.groupby("norm_title")["gameid"].apply(list).to_dict()

    # Sets of titles
    steam_titles = set(steam_csv_df["norm_title"])
    ps_titles = set(ps_grouped.keys())
    xbox_titles = set(xbox_grouped.keys())

    # Common titles across platforms
    common_titles = steam_titles & ps_titles & xbox_titles

    result = []

    for title in common_titles:
        steam_rows = steam_csv_df[steam_csv_df["norm_title"] == title]

        for _, row in steam_rows.iterrows():
            steam_id_str = str(row["gameid"])

            # Only include if exists in JSON
            if steam_id_str not in steam_json_ids:
                continue

            result.append({
                "gameName": row["title"],
                "steamId": row["gameid"],
                "playstationIds": ps_grouped.get(title, []),
                "xboxIds": xbox_grouped.get(title, [])
            })

    return result

def normalize_title(title: str) -> str:
    if pd.isna(title):
        return None
    title = str(title).lower().strip()
    title = title.translate(str.maketrans("", "", string.punctuation))
    return title

# Load the game csv's and the steam_data.json into dataframes
def load_game_datas_into_dataframes():
    steam_csv = pd.read_csv(landing_zone / "steam" / "csv" / "games.csv")
    playstation_csv = pd.read_csv(landing_zone / "playstation" / "csv" / "games.csv")
    xbox_csv = pd.read_csv(landing_zone / "xbox" / "csv" / "games.csv")

    # Load JSON
    with open(landing_zone / "steam" / "json" / "steam_data.json", "r") as f:
        steam_json = json.load(f)

    # Convert JSON → DataFrame
    steam_json_df = pd.DataFrame.from_dict(steam_json, orient="index")
    steam_json_df.reset_index(inplace=True)
    steam_json_df.rename(columns={"index": "gameid"}, inplace=True)

    return [
        {"platform": "steam", "fromType": "csv", "data": steam_csv},
        {"platform": "playstation", "fromType": "csv", "data": playstation_csv},
        {"platform": "xbox", "fromType": "csv", "data": xbox_csv},
        {"platform": "steam", "fromType": "json", "data": steam_json_df},
    ]