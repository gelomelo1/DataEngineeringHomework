import os
import psycopg2
from pathlib import Path

clean_data_zone = Path(os.getenv("CLEAN_DATA_ZONE", "/data/cleandata"))
clean_data_zone.mkdir(parents=True, exist_ok=True)

def four_load():
    print("Loading clean datasets into PostgreSQL database...")

    # Open a connection to the PostgreSQL database
    conn = openDatabaseConnection()

    try:
        # Create necessary tables if they don't exist
        createTables(conn)

        # Load the clean datasets into the database
        loadDataToDatabase(conn)

        print("Clean datasets have been successfully loaded into the PostgreSQL database.")
    except Exception as e:
        print("Failed to load data into the database:", e)
    finally:
        conn.close()

# This function is responsible for opening a connection to posgreSQL databse
def openDatabaseConnection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "dataengineeringdb"),
        user=os.getenv("POSTGRES_USER", "dataengineeringuser"),
        password=os.getenv("POSTGRES_PASSWORD", "dataengineeringpwd")
    )

# This function creates the necessary tables in the PostgreSQL database if they do not already exist. It defines the schema for the dimension tables (dim_game and dim_genre), the switch table (switch_genre), and the fact table (fact_game_sales).
def createTables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_game (
            id BIGINT PRIMARY KEY,
            game TEXT
        );

        CREATE TABLE IF NOT EXISTS dim_genre (
            id INT PRIMARY KEY,
            name TEXT
        );

        CREATE TABLE IF NOT EXISTS switch_genre (
            gameId BIGINT,
            genreId INT
        );

        CREATE TABLE IF NOT EXISTS fact_game_sales (
            id BIGINT PRIMARY KEY,
            steamPrice FLOAT,
            playstationPrice FLOAT,
            xboxPrice FLOAT,
            steamSales INT,
            playstationSales INT,
            xboxSales INT
        );
        """)
    conn.commit()

# This function batch loads the csv clean datasets into the postgreSQL database
#It rerunable and idempotent, meaning that it can be run multiple times without causing duplicate entries or data corruption in the database.
def loadDataToDatabase(conn):
    with conn.cursor() as cur:

        # truncate tables (idempotens)
        cur.execute("""
        TRUNCATE TABLE
            switch_genre,
            fact_game_sales,
            dim_game,
            dim_genre
        RESTART IDENTITY;
        """)

        # COPY gyors betöltés
        def copy_csv(table_name, file_name):
            file_path = clean_data_zone / file_name
            with open(file_path, "r") as f:
                cur.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV HEADER",
                    f
                )
            print(f"Loaded {file_name} → {table_name}")

        copy_csv("dim_game", "dim_game.csv")
        copy_csv("dim_genre", "dim_genre.csv")
        copy_csv("switch_genre", "switch_genre.csv")
        copy_csv("fact_game_sales", "fact_game_sales.csv")

    conn.commit()