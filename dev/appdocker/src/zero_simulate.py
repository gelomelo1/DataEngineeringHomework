from faker import Faker
import random
import json
import os
from pathlib import Path

# Set up the landing zone for raw datasets
simdata_folder = Path(os.getenv("SIMDATA_FOLDER", "/data/simdata"))
simdata_folder.mkdir(parents=True, exist_ok=True)

def zero_simulate():
    generate_fake_steam_data_and_merge_with_real_data()
    print("All simulated datasets have been generated and merged with real data in the simulation data folder.")

# Function to generate fake Steam data
def generate_steam_data(n=200, seed=42, start_id=100_000_000):
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    data = {}

    for i in range(n):
        game_id = start_id + i 
        game_id_str = str(game_id)

        name = fake.word().capitalize()
        description = fake.text(max_nb_chars=300)

        genre_pool = ["Action", "Adventure", "RPG", "Indie", "Strategy", "Simulation"]
        genres = random.sample(genre_pool, k=random.randint(1, 3))

        data[game_id_str] = {
            "success": True,
            "data": {
                "type": "game",
                "name": name,
                "steam_appid": game_id,
                "required_age": random.choice([0, 12, 16, 18]),
                "is_free": random.choice([True, False]),
                "controller_support": random.choice(["full", "partial", None]),
                "detailed_description": description,
                "about_the_game": description,
                "short_description": fake.text(max_nb_chars=100),
                "supported_languages": "English",
                "header_image": fake.image_url(),
                "capsule_image": fake.image_url(),
                "website": fake.url(),
                "price_overview": {
                    "currency": "EUR",
                    "initial": random.randint(500, 6000),
                    "final": random.randint(500, 6000),
                    "discount_percent": random.choice([0, 10, 20, 30]),
                    "final_formatted": f"{random.randint(5, 60)},{random.randint(0,99):02d}€"
                },
                "genres": [
                    {
                        "id": str(j + 1),
                        "description": g
                    } for j, g in enumerate(genres)
                ]
            }
        }

    return data

# This function generates fake Steam data and merges it with real data from a base JSON file, then saves the combined dataset to a new JSON file
def generate_fake_steam_data_and_merge_with_real_data():
    output_file = simdata_folder / "steam_data.json"

    if output_file.exists():
        output_file.unlink()

    fake_data = generate_steam_data(n=200, seed=42)

    with open(simdata_folder / "steam_data_base.json", "r") as f:
        real_data = json.load(f)

    merged_data = {**real_data, **fake_data}

    keys = list(merged_data.keys())
    random.seed(42)
    random.shuffle(keys)

    shuffled_data = {k: merged_data[k] for k in keys}

    with open(output_file, "w") as f:
        json.dump(shuffled_data, f, indent=2)

    print("Steam fake json dataset generated to:", output_file)