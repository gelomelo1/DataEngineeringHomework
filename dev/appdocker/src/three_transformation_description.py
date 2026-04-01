#steam/cs/games.csv schema: gameid,title,developers,publishers,genres,supported_languages,release_date
#playstation/cs/games.csv schema: gameid,title,platform,developers,publishers,genres,supported_languages,release_date
#xbox/cs/games.csv schema: gameid,title,developers,publishers,genres,supported_languages,release_date
#steam/json/steam_data.json schema: "2270": {"success": true, "data": {"type": "game","name": "Wolfenstein 3D","steam_appid": 2270,"required_age": 0,"is_free": false,"detailed_description": "something","supported_languages": "English","header_image": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/2270/header.jpg?t=1750784646","capsule_image": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/2270/capsule_231x87.jpg?t=1750784646","capsule_imagev5": "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/2270/capsule_184x69.jpg?t=1750784646","website": null,"pc_requirements": {"minimum": "<strong>Minimum: </strong>A 100% Windows XP/Vista-compatible computer system"},"mac_requirements": [],"linux_requirements": [],"legal_notice": "something","price_overview": {"currency": "EUR","initial": 499,"final": 499,"discount_percent": 0,"initial_formatted": "","final_formatted": "4,99€"},"genres": [{"id": "1","description": "Action"}]}},

#playstation/cs/prices.csv schema: gameid,usd,eur,gbp,jpy,rub,date_acquired
#xbox/cs/prices.csv schema: gameid,usd,eur,gbp,jpy,rub,date_acquired

#steam/cs/purchased_games.csv scheam: playerid,library
#playstation/cs/purchased_games.csv schema: playerid,library
#xbox/cs/purchased_games.csv schema: playerid,library


# Set up the landing zone for raw datasets
landing_zone = Path(os.getenv("LANDING_ZONE", "/app/rawdata"))
landing_zone.mkdir(parents=True, exist_ok=True)

def three_transformation():
    # Load the game name csv's json to dataframes
    # Get the common game ids from the given dataframes, only pass the csv dataframes as parameters, the json can be used later, but they can be addressed by id, since for same platform the csv and jsons carry the same game ids
    # Print the length of the returned list
    # Save the result return type into a file in the landing_zone root

# Get the common game ids from the given dataframes
def get_common_game_ids(dfs):
    # The dfs are the dataframes like this: [{platform: "steam", fromType: "csv", data: steam_df}, {platform: "playstation", fromType: "csv", data: playstation_df}, {platform: "xbox", fromType: "csv", data: xbox_df}, {platform: "steam", fromType: "json", data: steam_json_df}]
    # Return type should be similar to this: [{gameName: "game1" (come from steam csv), steamId: 123, playstationIds: [1234, 1235], xboxIds: [12345]}, ...]
    # To get the common game ids, we have to compare the normalized titles of the games in each dataframe and find the common ones, then return the game ids of those common games from the steam dataframe
    # Watch out, the game title fields can be empty, so we have to drop the rows with empty titles before normalizing and comparing
    # Also, for xbox, and playstation, for a single game multple game ids can exist, as for playstation its because of different platforms, etc PS5, PS4, PS3, as for xbox i dont know, but multiple ids with same game name exists
    # The plan for playstation and xbox is for one name check all rows, if its the same, and if the same add its id to the list 

# This function normalizes game titles by converting them to lowercase, stripping whitespace, and removing punctuation.
def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = title.translate(str.maketrans("", "", string.punctuation))
    return title


# Load the game csv's and the steam_data.json into dataframes
def load_game_datas_into_dataframes():
    # return typpe should be like this: [{platform: "steam", fromType: "csv", data: steam_df}, {platform: "playstation", fromType: "csv", data: playstation_df}, {platform: "xbox", fromType: "csv", data: xbox_df}, {platform: "steam", fromType: "json", data: steam_json_df}]
    # Load landing_zone/playstation/csv/games.csv into a dataframe
    # Load landing_zone/xbox/csv/games.csv into a dataframe
    # Load landing_zone/steam/csv/games.csv into a dataframe
    # Load landing_zone/steam/json/steam_data.json into a dataframe

# Load the given paths raw csv, or json datasets from the landing zone into dataframes for transformation
def load_raw_datas_into_dataframes(paths):
