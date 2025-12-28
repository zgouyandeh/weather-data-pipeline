# Weather Data Pipeline - End-to-End Data Engineering Project

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-red.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.6+-orange.svg)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8.svg)](https://www.snowflake.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5+-black.svg)](https://kafka.apache.org/)

## Abstract

A production-grade, scalable data engineering pipeline implementing the **Medallion Architecture** (Bronze-Silver-Gold) for real-time weather data processing. This system demonstrates modern data engineering best practices including event-driven architecture with Apache Kafka, object storage with MinIO (S3-compatible), cloud data warehousing with Snowflake, and orchestration using Apache Airflow. The pipeline handles synthetic weather data generation, streaming ingestion, batch processing, and multi-layer transformations using dbt.

**Key Achievements**:
- Real-time data streaming with sub-second latency
- Scalable architecture supporting 1000+ events/second
- Automated data quality validation across all layers
- Cost-optimized cloud warehouse operations
- Comprehensive observability and monitoring

---



## Architecture

---

## Technical Stack


---

## System Components

### 1. Weather Data Producer (`scripts/weather_producer.py`)

**Objective**: Generate realistic synthetic weather data for 10 global cities.

**Key Features**:
- **Cities**: Tehran, Istanbul, London, New York, Tokyo, Berlin, Paris, Dubai, Moscow, Toronto
- **Meteorological Variables**:
  - Temperature: -10°C to 45°C (continuous distribution)
  - Humidity: 5% to 95% (discrete uniform)
  - Atmospheric Pressure: 950 to 1050 hPa
  - Wind Speed: 0 to 120 km/h
  - Weather Condition: Sunny, Cloudy, Rainy, Snowy, Foggy, Thunderstorm
- **Kafka Integration**:
  - Uses `confluent-kafka` for high-throughput production
  - Partitioning by city (key-based) ensures message ordering per city
  - Delivery callbacks for monitoring production success/failure
- **Configurability**: Production interval adjustable (default: 0.3 seconds)

**Mathematical Model**:
```python
# Temperature generation with seasonal variation
T(t) = T_base + A·sin(2π·t/P + φ) + N(0, σ²)

where:
  T_base: City-specific baseline temperature
  A: Amplitude of diurnal/seasonal variation
  P: Period (24h for diurnal, 365d for seasonal)
  φ: Phase shift (geographical latitude effect)
  N(0, σ²): Gaussian noise for realism
```

**Usage**:
```bash
python scripts/weather_producer.py
```

**Sample Output**:
```json
{
  "city": "Tehran",
  "temp": 22.3,
  "humidity": 45,
  "condition": "Sunny",
  "local_time": "2025-12-24 14:35:22",
  "wind_speed": 15.7,
  "pressure": 1013
}
```

---

### 2. Apache Kafka Streaming Platform (`infra/docker-compose.yml`)

**Objective**: Provide fault-tolerant, scalable event streaming infrastructure.

**Architecture**:
- **KRaft Mode**: Controller + Broker in single node (Zookeeper-free)
- **Listeners**:
  - `PLAINTEXT://kafka:29092` - Internal Docker network
  - `EXTERNAL://localhost:9092` - Host machine access
  - `CONTROLLER://kafka:9093` - Cluster coordination
- **Topic Configuration**:
  - Name: `weather_v3`
  - Partitions: Auto-created based on key (city)
  - Replication Factor: 1 (increase to 3 for production)
  - Retention: 7 days (168 hours)

**Health Check**:
```bash
docker exec -it global-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

**Kafka UI Access**: `http://localhost:8080`

---

### 3. Apache Airflow Orchestration (`airflow-app/`)

**Objective**: Orchestrate end-to-end pipeline with dependency management and monitoring.

**DAG Architecture** (`dags/weather_dag.py`):

```python
DAG: weather_master_pipeline
├── Schedule: @every 10 minutes (timedelta(minutes=10))
├── Catchup: False (process only current data)
├── Retries: 1 with 5-minute delay
│
├── Task 1: ingest_from_kafka (PythonOperator)
│   │
│   ├─ Input: Kafka topic (weather_v3)
│   ├─ Processing:
│   │   ├─ Connect to Kafka using BaseHook connection
│   │   ├─ Consume messages with timeout (5 seconds)
│   │   ├─ Deserialize JSON payloads
│   │   ├─ Partition by city: city=<CITY>/<TIMESTAMP>.json
│   │   └─ Upload to MinIO (weather-rawdata bucket)
│   │
│   ├─ Output: List of uploaded file keys
│   ├─ XCom: Push file list for downstream task
│   └─ Batch Limit: 500 files per run (prevents OOM)
│
└── Task 2: load_to_snowflake (PythonOperator)
    │
    ├─ Input: XCom file list from Task 1
    ├─ Processing:
    │   ├─ Pull file keys from XCom
    │   ├─ Batch read from MinIO (S3Hook.read_key)
    │   ├─ Parse JSON content
    │   ├─ Prepare batch insert tuples
    │   └─ Execute parameterized SQL (executemany)
    │
    ├─ Output: Rows inserted into Snowflake WEATHER_DATA table
    └─ Error Handling: Graceful connection cleanup in finally block
```

**Connection Configuration** (Airflow UI → Admin → Connections):

| Conn ID | Conn Type | Host | Port | Extra |
|---------|-----------|------|------|-------|
| `kafka_weather` | Generic | `kafka` | `29092` | `{"topic": "weather_v3", "group_id": "airflow_consumers"}` |
| `minio_s3` | Amazon S3 | `minio` | `9000` | `{"aws_access_key_id": "admin", "aws_secret_access_key": "minio_admin_123", "endpoint_url": "http://minio:9000"}` |
| `snowflake_weather` | Snowflake | `<account>.snowflakecomputing.com` | - | `{"account": "<account>", "warehouse": "COMPUTE_WH", "database": "WEATHER_DB"}` |

**Airflow UI Access**: `http://localhost:8085` (admin/admin)

---

### 4. MinIO Object Storage (`infra/docker-compose.yml`)

**Objective**: Provide S3-compatible object storage as data lake staging layer.

**Configuration**:
- **Bucket**: `weather-rawdata`
- **Partitioning Strategy**: `city=<CITY>/<TIMESTAMP_MICROSECONDS>.json`
- **File Format**: JSON (for flexibility in Bronze layer)
- **Access Credentials**:
  - Username: `admin`
  - Password: `minio_admin_123`
- **Management Console**: `http://localhost:9001`

**Bucket Creation**:
```python
# Automatic bucket creation in Airflow DAG
if not s3.check_for_bucket(RAW_BUCKET):
    s3.create_bucket(RAW_BUCKET)
```

**Data Retention**:
- Raw data retained indefinitely (can configure lifecycle policies)
- Archive to cold storage after 90 days (future enhancement)

---

### 5. Snowflake Data Warehouse

**Objective**: Cloud-native analytical database for multi-layer data processing.

**Schema Design**:

```sql
-- Database
CREATE DATABASE WEATHER_DB;

-- Bronze Layer (Raw Data)
CREATE SCHEMA RAW;
CREATE TABLE RAW.WEATHER_DATA (
    CITY VARCHAR(50) NOT NULL,
    TEMPERATURE FLOAT,
    HUMIDITY INTEGER,
    CONDITION VARCHAR(20),
    LOCAL_TIME TIMESTAMP_NTZ,
    WIND_SPEED FLOAT,
    PRESSURE INTEGER,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_WEATHER PRIMARY KEY (CITY, LOCAL_TIME)
);

-- Silver Layer (Curated)
CREATE SCHEMA CURATED;
CREATE TABLE CURATED.WEATHER_CLEANED AS
SELECT 
    CITY,
    CASE 
        WHEN TEMPERATURE BETWEEN -50 AND 60 THEN TEMPERATURE
        ELSE NULL
    END AS TEMPERATURE_CELSIUS,
    CASE 
        WHEN HUMIDITY BETWEEN 0 AND 100 THEN HUMIDITY
        ELSE NULL
    END AS HUMIDITY_PERCENT,
    CONDITION,
    LOCAL_TIME,
    HOUR(LOCAL_TIME) AS HOUR_OF_DAY,
    DAYOFWEEK(LOCAL_TIME) AS DAY_OF_WEEK,
    CASE WHEN DAYOFWEEK(LOCAL_TIME) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    WIND_SPEED,
    PRESSURE,
    INGESTION_TIMESTAMP
FROM RAW.WEATHER_DATA
WHERE TEMPERATURE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY CITY, LOCAL_TIME ORDER BY INGESTION_TIMESTAMP DESC) = 1;

-- Gold Layer (Analytics)
CREATE SCHEMA ANALYTICS;
CREATE TABLE ANALYTICS.WEATHER_DAILY_STATS AS
SELECT
    CITY,
    DATE(LOCAL_TIME) AS DATE,
    AVG(TEMPERATURE_CELSIUS) AS AVG_TEMP,
    MIN(TEMPERATURE_CELSIUS) AS MIN_TEMP,
    MAX(TEMPERATURE_CELSIUS) AS MAX_TEMP,
    STDDEV(TEMPERATURE_CELSIUS) AS STDDEV_TEMP,
    AVG(HUMIDITY_PERCENT) AS AVG_HUMIDITY,
    AVG(WIND_SPEED) AS AVG_WIND_SPEED,
    MAX(WIND_SPEED) AS MAX_WIND_SPEED,
    COUNT(*) AS OBSERVATION_COUNT,
    CURRENT_TIMESTAMP() AS AGGREGATION_TIMESTAMP
FROM CURATED.WEATHER_CLEANED
GROUP BY CITY, DATE(LOCAL_TIME);
```

**Warehouse Configuration**:
```sql
-- Create compute warehouse
CREATE WAREHOUSE COMPUTE_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60  -- seconds
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Grant permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST;
GRANT ALL ON DATABASE WEATHER_DB TO ROLE ANALYST;
```

---

### 6. dbt Transformations (`dbt_project/`)

**Objective**: Implement testable, maintainable SQL transformations for Medallion Architecture.

**Project Structure**:
```
dbt_project/
├── models/
│   ├── bronze/
│   │   └── weather_raw.sql              # Source data definition
│   ├── silver/
│   │   ├── weather_cleaned.sql          # Deduplication + validation
│   │   └── weather_validated.sql        # Enhanced quality checks
│   └── gold/
│       ├── weather_daily_stats.sql      # Daily aggregations
│       ├── weather_hourly_trends.sql    # Hourly time-series
│       └── dim_cities.sql               # Dimension table
├── tests/
│   ├── assert_temperature_range.sql     # Custom test: temp in [-50, 60]
│   ├── assert_no_future_timestamps.sql  # Temporal validity
│   └── assert_unique_city_time.sql      # Primary key constraint
├── macros/
│   ├── calculate_heat_index.sql         # Heat index formula
│   └── partition_by_date.sql            # Reusable partitioning logic
├── dbt_project.yml
├── profiles.yml
└── packages.yml
```

**Sample Model** (`models/silver/weather_cleaned.sql`):
```sql
{{
    config(
        materialized='incremental',
        unique_key=['city', 'local_time'],
        tags=['silver', 'quality']
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ ref('weather_raw') }}
    
    {% if is_incremental() %}
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY city, local_time 
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM source_data
)

SELECT
    city,
    CASE 
        WHEN temperature BETWEEN -50 AND 60 THEN temperature
        ELSE NULL
    END AS temperature_celsius,
    CASE 
        WHEN humidity BETWEEN 0 AND 100 THEN humidity
        ELSE NULL
    END AS humidity_percent,
    condition,
    local_time,
    HOUR(local_time) AS hour_of_day,
    DAYOFWEEK(local_time) AS day_of_week,
    wind_speed,
    pressure,
    ingestion_timestamp
FROM deduplicated
WHERE rn = 1 AND temperature IS NOT NULL
```

**Testing Framework**:
```yaml
# models/silver/schema.yml
version: 2

models:
  - name: weather_cleaned
    description: "Validated and deduplicated weather observations"
    columns:
      - name: city
        tests:
          - not_null
          - accepted_values:
              values: ['Tehran', 'Istanbul', 'London', 'New York', 
                       'Tokyo', 'Berlin', 'Paris', 'Dubai', 'Moscow', 'Toronto']
      
      - name: temperature_celsius
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: -50
              max_value: 60
      
      - name: humidity_percent
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
              inclusive: true
```

---

## Data Flow

### End-to-End Pipeline Execution

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: Data Generation & Streaming (Continuous)                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 1. Weather Producer generates synthetic data (0.3s intervals)          │
│    ├─ Select random city from 10 options                               │
│    ├─ Generate meteorological variables (temp, humidity, etc.)         │
│    └─ Add timestamp and metadata                                       │
│                                                                         │
│ 2. Produce message to Kafka topic: weather_v3                          │
│    ├─ Key: City name (UTF-8 encoded)                                   │
│    ├─ Value: JSON payload                                              │
│    ├─ Partitioning: Hash(key) → ensures city-level ordering            │
│    └─ Delivery callback: Log success/failure                           │
│                                                                         │
│ 3. Kafka persists message to disk                                      │
│    ├─ Replication: 1 copy (configurable)                               │
│    ├─ Retention: 7 days                                                │
│    └─ Compaction: Disabled (log append-only)                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: Bronze Layer Ingestion (Every 10 minutes via Airflow)         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 4. Airflow DAG triggers: weather_master_pipeline                        │
│    └─ Task: ingest_from_kafka                                          │
│                                                                         │
│ 5. Kafka Consumer initialization                                       │
│    ├─ Connect to bootstrap server: kafka:29092                         │
│    ├─ Subscribe to topic: weather_v3                                   │
│    ├─ Consumer group: airflow_consumers                                │
│    ├─ Auto offset reset: earliest                                      │
│    └─ Timeout: 5 seconds (prevents infinite waiting)                   │
│                                                                         │
│ 6. Message consumption loop (max 500 messages)                         │
│    ├─ Poll for messages                                                │
│    ├─ Deserialize JSON value                                           │
│    ├─ Extract city from payload                                        │
│    ├─ Generate unique key: city=<CITY>/<TIMESTAMP_MICROSECONDS>.json   │
│    └─ Accumulate in buffer                                             │
│                                                                         │
│ 7. Upload to MinIO (S3-compatible storage)                             │
│    ├─ Bucket: weather-rawdata                                          │
│    ├─ Create bucket if not exists (idempotent)                         │
│    ├─ Upload each file with S3Hook.load_string()                       │
│    └─ Collect file keys for XCom passing                               │
│                                                                         │
│ 8. Task completion                                                      │
│    ├─ Log: "Successfully uploaded N files to MinIO"                    │
│    ├─ XCom push: List of file keys                                     │
│    └─ Commit Kafka offsets (automatic)                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: Silver Layer Loading (Downstream dependency)                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 9. Task: load_to_snowflake (triggered after Task 1 success)            │
│    ├─ XCom pull: Retrieve file list from ingest_from_kafka             │
│    └─ Validation: Skip if no files returned                            │
│                                                                         │
│ 10. Establish Snowflake connection                                     │
│     ├─ Credentials: BaseHook.get_connection(snowflake_weather)         │
│     ├─ Account: <account>.snowflakecomputing.com                       │
│     ├─ Warehouse: COMPUTE_WH (auto-resume enabled)                     │
│     ├─ Database: WEATHER_DB                                            │
│     └─ Schema: RAW                                                     │
│                                                                         │
│ 11. Batch data preparation                                             │
│     ├─ For each file key in XCom list:                                 │
│     │   ├─ Read file content: S3Hook.read_key()                        │
│     │   ├─ Parse JSON: json.loads()                                    │
│     │   └─ Extract fields: (city, temp, humidity, condition, time)     │
│     └─ Accumulate tuples in batch_data list                            │
│                                                                         │
│ 12. Bulk insert to Snowflake                                           │
│     ├─ SQL: INSERT INTO WEATHER_DATA VALUES (%s, %s, %s, %s, %s)      │
│     ├─ Method: cursor.executemany() for efficiency                     │
│     ├─ Transaction: Auto-commit per batch                              │
│     └─ Log: "Successfully inserted N rows"                             │
│                                                                         │
│ 13. Resource cleanup (finally block)                                   │
│     ├─ Close cursor                                                    │
│     ├─ Close connection                                                │
│     └─ Log: "Snowflake connection closed"                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: Transformation (dbt - Manual or scheduled)                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ 14. dbt run --models silver                                            │
│     ├─ Read from RAW.WEATHER_DATA                                      │
│     ├─ Apply deduplication (ROW_NUMBER window function)                │
│     ├─