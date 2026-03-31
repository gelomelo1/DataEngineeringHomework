from one_extract import one_extract
from zero_simulate import zero_simulate
from two_staging import two_staging
from three_transformation import three_transformation

# 0. SOURCE - Generate fake json data for steam (real data + fake data)
zero_simulate()

#1 . EXTRACT - Download raw datasets CSV from Kaggle and json from generated fake steam data trough local rest api, this is unstructured data
one_extract()

#2. STAGE - Move the raw datasets to the landing zone, where the data will be stored in a structured way, ready for the next steps of the pipeline
two_staging()

#3. TRANSFORM - Transform the multiple raw datasets into a single clean csv dataset, using null check, normalization, aggregation
three_transformation()

print("Pipeline finnished successfully.")