from rawdatasource import download_raw_datasources
from simulatedata import generate_fake_steam_data_and_merge_with_real_data

# 1. Generate fake json data for steam (real data + fake data)
generate_fake_steam_data_and_merge_with_real_data()

#2 . Download raw datasets CSV from Kaggle and json from generated fake steam data trough local rest api, and save them to the landing zone
download_raw_datasources()

print("Pipeline finnished successfully.")