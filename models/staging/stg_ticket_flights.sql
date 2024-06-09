WITH source AS (
    SELECT
         ROW_NUMBER() OVER (ORDER BY TICKET_NO) AS TICKET_FLIGHT_ID
        , TICKET_NO
        , FLIGHT_ID
        , FARE_CONDITIONS
        , AMOUNT
    FROM {{ source('TY_AVIATION', 'SRC_TICKET_FLIGHTS')}}


)

SELECT *
      , CURRENT_TIMESTAMP() AS injestion_timestamp
FROM source