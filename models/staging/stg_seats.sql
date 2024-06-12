WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEAT_NO) AS SEAT_ID
        , SEAT_NO
        , AIRCRAFT_CODE
        , FARE_CONDITIONS
    FROM {{ source('TY_AVIATION', 'SRC_SEATS')}}

)

SELECT *
       , CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source