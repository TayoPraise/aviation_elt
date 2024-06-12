WITH source as (
    SELECT 
        ROW_NUMBER() over(order by AIRCRAFT_CODE) AS AIRCRAFT_ID 
        , AIRCRAFT_CODE
        , PARSE_JSON(MODEL):en::STRING AS AIRCRAFT_MODEL --extracting the english translation from the json cell 
        , RANGE
    FROM {{ source('TY_AVIATION', 'SRC_AIRCRAFTS_DATA') }}
)

SELECT 
    *
    , CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source