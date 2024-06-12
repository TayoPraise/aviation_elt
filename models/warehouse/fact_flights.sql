{{ config(
    materialized='table',
    cluster_by=['ACTUAL_DEPARTURE']
) }}

WITH source AS(

    SELECT 
        f.FLIGHT_ID
        , f.FLIGHT_NO           
        , a.AIRCRAFT_ID
        , ap1.AIRPORT_ID DEPARTURE_AIRPORT_ID
        , ap2.AIRPORT_ID ARRIVAL_AIRPORT_ID
        , f.ACTUAL_DEPARTURE    -- incase of null, the flight either didn't happened or it's been cancelled 
        , f.ACTUAL_ARRIVAL      -- incase of null, the flight either didn't happened or it's been cancelled 
        , f.SCHEDULED_ARRIVAL 
        , f.SCHEDULED_DEPARTURE
        , f.STATUS
    FROM {{ ref('stg_flights')}} f
    LEFT JOIN {{ ref('stg_aircrafts')}} a
    USING(AIRCRAFT_CODE)
    LEFT JOIN {{ ref('stg_airports')}} ap1
    ON f.departure_airport = ap1.airport_code
    LEFT JOIN {{ ref('stg_airports')}} ap2
    ON f.arrival_airport = ap2.airport_code

)
, unique_source AS (

    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY FLIGHT_ID, FLIGHT_NO, AIRCRAFT_ID, DEPARTURE_AIRPORT_ID, ARRIVAL_AIRPORT_ID, SCHEDULED_DEPARTURE, SCHEDULED_ARRIVAL ORDER BY ACTUAL_DEPARTURE) as row_number
    FROM source
)

SELECT
    FLIGHT_ID
    , FLIGHT_NO
    , AIRCRAFT_ID
    , DEPARTURE_AIRPORT_ID
    , ARRIVAL_AIRPORT_ID
    , ACTUAL_DEPARTURE
    , ACTUAL_ARRIVAL
    , SCHEDULED_ARRIVAL
    , SCHEDULED_DEPARTURE
    , STATUS
    , CURRENT_TIMESTAMP() INGESTION_TIMESTAMP
FROM unique_source
WHERE row_number = 1
ORDER BY 1