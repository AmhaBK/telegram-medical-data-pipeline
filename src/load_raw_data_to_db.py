import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")

RAW_DATA_PATH = "/app/data/raw/telegram_messages/"
TABLE_NAME = "raw_telegram_messages"
SCHEMA_NAME = "raw"

def create_table_if_not_exists(cursor):
    """
    Creates the raw schema and a table to store the raw Telegram messages if they don't exist.
    """
    try:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(SCHEMA_NAME)))
        print(f"Schema '{SCHEMA_NAME}' checked/created.")

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id INT PRIMARY KEY,
                channel_name TEXT,
                message_data JSONB NOT NULL,
                scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))
        cursor.execute(create_table_query)
        print(f"Table '{SCHEMA_NAME}.{TABLE_NAME}' checked/created.")

    except psycopg2.Error as e:
        print("Error creating schema or table:", e)
        raise

def load_data_from_json_files(cursor, conn):
    """
    Reads all JSON files from the raw data directory, extracts individual messages,
    and loads them into the database.
    """
    if not os.path.exists(RAW_DATA_PATH):
        print(f"Error: Directory '{RAW_DATA_PATH}' not found. Please check your volumes and scraping script.")
        return

    json_files = []
    for root, _, files in os.walk(RAW_DATA_PATH):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    if not json_files:
        print(f"No JSON files found in '{RAW_DATA_PATH}'. Nothing to load.")
        return

    print(f"Found {len(json_files)} JSON files to load.")

    insert_query = sql.SQL("""
        INSERT INTO {}.{} (id, channel_name, message_data)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET message_data = EXCLUDED.message_data, scraped_at = EXCLUDED.scraped_at;
    """).format(
        sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME)
    )
    
    total_messages_loaded = 0
    for i, file_path in enumerate(json_files):
        channel_name = os.path.basename(file_path).split('.')[0]
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                messages = json.load(f)
                if not isinstance(messages, list):
                    print(f"Warning: File {file_path} does not contain a list of messages. Skipping.")
                    continue

                for message in messages:
                    cursor.execute(insert_query, (message['id'], channel_name, json.dumps(message),))
                    total_messages_loaded += 1
            print(f"Processed {i+1}/{len(json_files)}: {file_path}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from file {file_path}: {e}")
        except psycopg2.Error as e:
            print(f"Error inserting data from file {file_path}: {e}")
            conn.rollback()
            raise

    conn.commit()
    print(f"All data loaded and committed. Total messages loaded: {total_messages_loaded}")

def main():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cursor = conn.cursor()
        print("Successfully connected to the database.")
        create_table_if_not_exists(cursor)
        load_data_from_json_files(cursor, conn)
    except psycopg2.Error as e:
        print("Database connection or operation failed:", e)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()