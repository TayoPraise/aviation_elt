WITH source AS (

        WITH d_count AS (

        SELECT 
            departure_airport airport_code
            , COUNT(*) D_COUNT
            , MAX(actual_departure)::DATE LAST_DEPARTURE_DATE
        FROM {{ref('stg_flights')}}
        WHERE ACTUAL_DEPARTURE IS NOT NULL
        GROUP BY 1
)

, a_count AS (

        SELECT 
            arrival_airport airport_code
            , COUNT(*) A_COUNT
            , MAX(actual_arrival)::DATE LAST_arrival_DATE
        FROM {{ref('stg_flights')}}
        WHERE ACTUAL_ARRIVAL IS NOT NULL
        GROUP BY 1
)

SELECT ai.AIRPORT_ID, ai.AIRPORT_CODE, AIRPORT_NAME, CITY, TIMEZONE, COORDINATES
        , LAST_DEPARTURE_DATE, LAST_ARRIVAL_DATE
        , COALESCE(a.A_COUNT, 0) +  COALESCE(d.D_COUNT, 0) FLIGHT_OPERATIONS_FREQUENCY
FROM {{ref('stg_airports')}} ai
LEFT JOIN d_count d
ON d.airport_code = ai.airport_code
LEFT JOIN a_count a
ON a.airport_code = ai.airport_code
GROUP BY 1,2,3,4,5,6,7,8,9


)

 , unique_source AS (
    SELECT  
        *,
        ROW_NUMBER() OVER (PARTITION BY airport_code ORDER BY AIRPORT_ID ASC) AS row_number
    FROM source

 )

SELECT AIRPORT_ID
       , AIRPORT_CODE
       , AIRPORT_NAME
       , CITY
       , TIMEZONE
       , COORDINATES
       , LAST_DEPARTURE_DATE
       , LAST_ARRIVAL_DATE
       , FLIGHT_OPERATIONS_FREQUENCY
       , CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
FROM unique_source
ORDER BY AIRPORT_ID