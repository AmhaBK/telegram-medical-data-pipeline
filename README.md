#  Telegram Medical Business Data Pipeline

This project builds a complete data pipeline that extracts raw Telegram messages from public channels related to Ethiopian medical businesses, loads them into a PostgreSQL database, transforms them using dbt, and prepares them for analytics and API consumption.

##  Features

-  Containerized using Docker + WSL  
-  Extract raw messages from scraped Telegram channels  
-  Store data in PostgreSQL as JSONB  
-  Transform and model data using dbt (staging + marts)  
-  Modular, rerunnable pipeline  
-  (Coming Soon) API to expose data via FastAPI  
-  (Coming Soon) Dagster orchestration and YOLOv8 enrichment  

##  Technologies Used

- **PostgreSQL**: Data storage  
- **Docker** + **WSL**: Containerization and cross-platform setup  
- **Python (psycopg2)**: Data ingestion  
- **dbt**: SQL-based data transformation  
- **FastAPI** *(planned)*: RESTful API for data access  
- **Dagster** *(optional)*: Workflow orchestration  

##  Data Flow

1. **Raw JSON Files**: Scraped Telegram messages saved under `data/raw/telegram_messages/`
2. **Ingestion Script**: `load_raw_data_to_db.py` inserts these messages into PostgreSQL under the `raw.raw_telegram_messages` table.
3. **Transformation with dbt**:
   - `staging.stg_telegram_messages`: Flattens raw JSON fields
   - `marts.telegram_message_summary`: Aggregates messages per channel or date
4. **API Layer** *(Coming Soon)*: Expose structured insights for downstream applications

##  Setup Instructions

### 1. Clone Repository

```bash
git clone https://github.com/AmhaBK/telegram-medical-data-pipeline.git
cd telegram-medical-data-pipeline


2. Configure Environment
Create a .env file at the root

3. Start Services with Docker
docker compose up --build

Make sure your Docker is running via WSL and that file paths are accessible.

4. Ingest Data
Place your JSON files in:

./data/raw/telegram_messages/

Then run the ingestion script inside the container:

docker exec -it data_pipeline_app python scripts/load_raw_data_to_db.py

5. Run dbt Transformations
docker exec -it data_pipeline_app dbt run --project-dir dbt

You should now have staging and mart models in your PostgreSQL DB.



📍 Next Steps
✅ Load raw Telegram messages into PostgreSQL

✅ Transform data with dbt models

⏭️ YOLOv8 integration for image-based enrichment

⏭️ Serve transformed data using FastAPI

⏭️ Add Dagster for scheduling 

