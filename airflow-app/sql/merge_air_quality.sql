-- Incremental Merge for Air Quality Data
MERGE INTO WEATHER_DB.BRONZE.BRONZE_AIR_QUALITY AS target
USING (
    SELECT 
        COALESCE(TRY_TO_NUMBER(JSON_DATA:record_id::STRING), HASH(JSON_DATA:timestamp::STRING)) as record_id,
        JSON_DATA:city::STRING as city,
        TRY_TO_NUMBER(JSON_DATA:aqi_index::STRING) as aqi_index,
        TRY_TO_DOUBLE(JSON_DATA:pm25::STRING) as pm25,
        JSON_DATA:sensor_status::STRING as sensor_status,
        TRY_TO_TIMESTAMP_NTZ(JSON_DATA:timestamp::STRING) as dt,
        JSON_DATA:source_system::STRING as source_system,
        JSON_DATA:producer_id::STRING as producer_id
    FROM WEATHER_DB.RAW.RAW_AIR_QUALITY_STREAM -- Consume data from Stream
) AS source
ON target.RECORD_ID = source.record_id
WHEN NOT MATCHED THEN
    INSERT (RECORD_ID, CITY, AQI_INDEX, PM25, SENSOR_STATUS, DATA_TIMESTAMP, SOURCE_SYSTEM, PRODUCER_ID)
    VALUES (source.record_id, source.city, source.aqi_index, source.pm25, source.sensor_status, source.dt, source.source_system, source.producer_id);