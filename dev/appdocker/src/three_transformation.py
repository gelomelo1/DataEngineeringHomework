import os
import pandas as pd
import json
from pathlib import Path
import string
import ast

landing_zone = Path(os.getenv("LANDING_ZONE", "/app/rawdata"))
landing_zone.mkdir(parents=True, exist_ok=True)

def three_transformation():
    print("Starting transformation of raw datasets into a single clean dataset...")
    dfs = load_game_datas_into_dataframes()

    common_games = get_common_game_ids(dfs)

    steam_json_df = next(d["data"] for d in dfs if d["platform"] == "steam" and d["fromType"] == "json")

    games_genres = get_games_genres(common_games, steam_json_df)

    price_dfs = load_price_datas_into_dataframes()

    game_prices = get_game_prices(common_games, price_dfs, steam_json_df)

    sales_dfs = load_sales_datas_into_dataframes()

    game_sales = get_game_sales(common_games, sales_dfs)

    print(f"Found {len(common_games)} common games across platforms")

    output_file = landing_zone / "common_games.json"

    with open(output_file, "w") as f:
        json.dump(common_games, f, indent=2)

    genres_output_file = landing_zone / "common_games_genres.json"

    with open(genres_output_file, "w") as f:
        json.dump(games_genres, f, indent=2)

    prices_output_file = landing_zone / "common_games_prices.json"

    with open(prices_output_file, "w") as f:
        json.dump(game_prices, f, indent=2)

    sales_output_file = landing_zone / "common_games_sales.json"

    with open(sales_output_file, "w") as f:
        json.dump(game_sales, f, indent=2)

    print(f"Saved common games to: {output_file}")
    print(f"Saved game genres to: {genres_output_file}")
    print(f"Saved game prices to: {prices_output_file}")
    print(f"Saved game sales to: {sales_output_file}")
    print("Finished transformation of raw datasets into a single clean dataset...")


# Get the aggregated sales per platform for the common games from the purchased_games_dfs
# Return type: [{steamId: 123, steamSales: 1000, playstationSales: 500, xboxSales: 200}, ...]
def get_game_sales(common_games, purchased_games_dfs):
    result = []

    # Extract dataframes
    steam_df = next(d["data"] for d in purchased_games_dfs if d["platform"] == "steam")
    ps_df = next(d["data"] for d in purchased_games_dfs if d["platform"] == "playstation")
    xbox_df = next(d["data"] for d in purchased_games_dfs if d["platform"] == "xbox")

    # Parse libraries into lists
    steam_df["library_list"] = steam_df["library"].apply(parse_library)
    ps_df["library_list"] = ps_df["library"].apply(parse_library)
    xbox_df["library_list"] = xbox_df["library"].apply(parse_library)

    # Flatten all purchases into one big list per platform
    steam_all = [gid for sublist in steam_df["library_list"] for gid in sublist]
    ps_all = [gid for sublist in ps_df["library_list"] for gid in sublist]
    xbox_all = [gid for sublist in xbox_df["library_list"] for gid in sublist]

    # Convert to Series for fast counting
    steam_counts = pd.Series(steam_all).value_counts()
    ps_counts = pd.Series(ps_all).value_counts()
    xbox_counts = pd.Series(xbox_all).value_counts()

    for game in common_games:
        steam_id = game["steamId"]
        ps_ids = game.get("playstationIds", [])
        xbox_ids = game.get("xboxIds", [])

        # Steam sales (single ID)
        steam_sales = int(steam_counts.get(steam_id, 0))

        # PlayStation sales (sum all IDs)
        playstation_sales = int(sum(ps_counts.get(pid, 0) for pid in ps_ids))

        # Xbox sales (sum all IDs)
        xbox_sales = int(sum(xbox_counts.get(xid, 0) for xid in xbox_ids))

        result.append({
            "steamId": steam_id,
            "steamSales": steam_sales,
            "playstationSales": playstation_sales,
            "xboxSales": xbox_sales
        })

    return result

# Get the gameids form the libary
def parse_library(lib_str):
    if pd.isna(lib_str) or lib_str == "":
        return []
    try:
        return ast.literal_eval(lib_str)
    except:
        return []

# Load the price csv's and the steam_data.json into dataframes
def load_sales_datas_into_dataframes():
    steam_csv = pd.read_csv(landing_zone / "steam" / "csv" / "purchased_games.csv")
    playstation_csv = pd.read_csv(landing_zone / "playstation" / "csv" / "purchased_games.csv")
    xbox_csv = pd.read_csv(landing_zone / "xbox" / "csv" / "purchased_games.csv")

    return [
        {"platform": "steam", "data": steam_csv},
        {"platform": "playstation",  "data": playstation_csv},
        {"platform": "xbox", "data": xbox_csv},
    ]

# Get the prices for the common games from the price dataframes and the steam json dataframe
#This function gets the prices for the most up-to-date platform on PlayStation and Xbox
# Return type: [{steamId: 123, steamPrice: 19.99, playstationPrice: 59.99, xboxPrice: 49.99}, ...]
# For missing prices the return value -1
def get_game_prices(common_games, price_dfs, steam_json_df):
    result = []

    # Extract price dataframes
    ps_prices_df = next(d["data"] for d in price_dfs if d["platform"] == "playstation")
    xbox_prices_df = next(d["data"] for d in price_dfs if d["platform"] == "xbox")
    steam_prices_df = next(d["data"] for d in price_dfs if d["platform"] == "steam")

    # Convert JSON to lookup dict
    if isinstance(steam_json_df, dict):
        steam_lookup = steam_json_df
    else:
        steam_lookup = steam_json_df.set_index("gameid").to_dict(orient="index")

    # Ensure numeric price columns
    for df in [ps_prices_df, xbox_prices_df, steam_prices_df]:
        df["eur"] = pd.to_numeric(df["eur"], errors="coerce")
        df["date_acquired"] = pd.to_datetime(df["date_acquired"], errors="coerce")

    for game in common_games:
        steam_id = str(game["steamId"])

        # Steam price
        steam_price = None

        if steam_id in steam_lookup:
            entry = steam_lookup[steam_id]
            data = entry.get("data") if isinstance(entry, dict) else None

            if isinstance(data, dict):
                price_info = data.get("price_overview")
                if isinstance(price_info, dict):
                    final_price = price_info.get("final")
                    if final_price is not None:
                        steam_price = final_price / 100  # cents → EUR

        # Fallback to steam prices.csv if JSON price is missing
        if steam_price is None:
            steam_rows = steam_prices_df[steam_prices_df["gameid"] == int(steam_id)]
            if not steam_rows.empty:
                steam_row = steam_rows.sort_values(by="date_acquired", ascending=False).iloc[0]
                steam_price = steam_row["eur"]

        # PlayStation price
        playstation_price = None
        ps_ids = game.get("playstationIds", [])

        if ps_ids:
            ps_id = ps_ids[0]
            ps_rows = ps_prices_df[ps_prices_df["gameid"] == ps_id]

            if not ps_rows.empty:
                ps_row = ps_rows.sort_values(by="date_acquired", ascending=False).iloc[0]
                playstation_price = ps_row["eur"]

        # Xbox price
        xbox_price = None
        xbox_ids = game.get("xboxIds", [])

        if xbox_ids:
            xbox_id = xbox_ids[0]
            xbox_rows = xbox_prices_df[xbox_prices_df["gameid"] == xbox_id]

            if not xbox_rows.empty:
                xbox_row = xbox_rows.sort_values(by="date_acquired", ascending=False).iloc[0]
                xbox_price = xbox_row["eur"]

        # replace None or NaN prices with -1 to indicate missing price
        def clean_price(price):
            if price is None or pd.isna(price):
                return -1
            return float(price)

        result.append({
            "steamId": game["steamId"],
            "steamPrice": clean_price(steam_price),
            "playstationPrice": clean_price(playstation_price),
            "xboxPrice": clean_price(xbox_price)
        })

    return result

# Load the price csv's and the steam_data.json into dataframes
def load_price_datas_into_dataframes():
    steam_csv = pd.read_csv(landing_zone / "steam" / "csv" / "prices.csv")
    playstation_csv = pd.read_csv(landing_zone / "playstation" / "csv" / "prices.csv")
    xbox_csv = pd.read_csv(landing_zone / "xbox" / "csv" / "prices.csv")

    return [
        {"platform": "steam", "data": steam_csv},
        {"platform": "playstation",  "data": playstation_csv},
        {"platform": "xbox", "data": xbox_csv},
    ]

# Get the genres for the common games from the steam json dataframe
# Return type: [{steamId: 123, genres: [{id: 1, name: "Action"}]}, ...]
def get_games_genres(common_games, steam_json_df):
    result = []

    steam_json_lookup = steam_json_df.set_index("gameid").to_dict(orient="index")

    for game in common_games:
        steam_id = str(game["steamId"])

        genres_list = []

        # Check if game exists in JSON
        if steam_id in steam_json_lookup:
            game_entry = steam_json_lookup[steam_id]

            # Handle both raw JSON and DataFrame-converted formats
            data = game_entry.get("data") if isinstance(game_entry, dict) else None

            if data and "genres" in data and data["genres"]:
                for genre in data["genres"]:
                    genres_list.append({
                        "id": int(genre["id"]),
                        "name": genre["description"]
                    })

        result.append({
            "steamId": game["steamId"],
            "genres": genres_list
        })

    return result

# Get the common game ids from the given dataframes
# Return type: [{gameName: "game1" (come from steam csv), steamId: 123, playstationIds: [1234, 1235], xboxIds: [12345]}, ...]
#playstationIds are ordered by platform priority (PS5 > PS4 > PS3), xboxIds are ordered by release_date (latest first)
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

    # PlayStation sorting (platform priority: PS5 > PS4 > PS3)
    platform_priority = {
        "PS5": 3,
        "PS4": 2,
        "PS3": 1
    }

    ps_df["platform_priority"] = ps_df["platform"].map(platform_priority).fillna(0)

    ps_grouped = (
        ps_df.sort_values(by="platform_priority", ascending=False)
        .groupby("norm_title")["gameid"]
        .apply(list)
        .to_dict()
    )

    # Xbox sorting (release_date, latest first)
    xbox_df["release_date"] = pd.to_datetime(xbox_df["release_date"], errors="coerce")

    xbox_grouped = (
        xbox_df.sort_values(by="release_date", ascending=False)
        .groupby("norm_title")["gameid"]
        .apply(list)
        .to_dict()
    )

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

# Normalize game titles by lowercasing, stripping whitespace, and removing punctuation
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
    # Filter out entries without valid "data" field
    steam_json_df = steam_json_df[steam_json_df["data"].notna()]
    steam_json_df.reset_index(inplace=True)
    steam_json_df.rename(columns={"index": "gameid"}, inplace=True)

    return [
        {"platform": "steam", "fromType": "csv", "data": steam_csv},
        {"platform": "playstation", "fromType": "csv", "data": playstation_csv},
        {"platform": "xbox", "fromType": "csv", "data": xbox_csv},
        {"platform": "steam", "fromType": "json", "data": steam_json_df},
    ]