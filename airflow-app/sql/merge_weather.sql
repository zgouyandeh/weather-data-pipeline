-- Incremental Merge for Weather Data
MERGE INTO WEATHER_DB.BRONZE.BRONZE_WEATHER AS target
USING (
    SELECT 
        -- Handle dirty record_id or generate from timestamp
        COALESCE(TRY_TO_NUMBER(JSON_DATA:record_id::STRING), HASH(JSON_DATA:timestamp::STRING)) as record_id,
        JSON_DATA:city::STRING as city,
        TRY_TO_DOUBLE(JSON_DATA:temperature_c::STRING) as temperature_c,
        TRY_TO_NUMBER(JSON_DATA:humidity_pct::STRING) as humidity_pct,
        TRY_TO_TIMESTAMP_NTZ(JSON_DATA:timestamp::STRING) as dt,
        JSON_DATA:source_system::STRING as source_system,
        JSON_DATA:producer_id::STRING as producer_id
    FROM WEATHER_DB.RAW.RAW_WEATHER_STREAM -- Consume data from Stream
) AS source
ON target.RECORD_ID = source.record_id
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, CITY, TEMPERATURE_C, HUMIDITY_PCT, DATA_TIMESTAMP, SOURCE_SYSTEM, PRODUCER_ID)
    VALUES (source.record_id, source.city, source.temperature_c, source.humidity_pct, source.dt, source.source_system, source.producer_id);