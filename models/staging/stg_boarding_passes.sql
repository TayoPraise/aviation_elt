WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY BOARDING_NO) AS BOARDING_ID
        , BOARDING_NO
        , FLIGHT_ID
        , SEAT_NO
        , TICKET_NO
    FROM {{ source('TY_AVIATION', 'SRC_BOARDING_PASSES')}}

)

SELECT *
       , CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source