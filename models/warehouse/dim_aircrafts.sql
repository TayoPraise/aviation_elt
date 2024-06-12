WITH source AS (


        SELECT 
            a.AIRCRAFT_ID
            , a.AIRCRAFT_CODE
            , a.AIRCRAFT_MODEL
            , a.RANGE
            , MIN(f.ACTUAL_DEPARTURE)::DATE FIRST_TRIP_DATE
            , DATEDIFF(YEAR, MIN(f.ACTUAL_DEPARTURE), MAX(f.ACTUAL_DEPARTURE) ) || ' year(s), ' || 
                DATEDIFF(MONTH,MIN(f.ACTUAL_DEPARTURE), MAX(f.ACTUAL_DEPARTURE) ) || ' month(s)' AGE_IN_FLEET
            , COUNT(f.ACTUAL_ARRIVAL) NUMBER_OF_TRIPS
            , (COUNT(f.ACTUAL_ARRIVAL) / 300)::NUMERIC NUMBER_OF_MAINTENANCE     -- 300 flights was used based on the Federal Aviation Admin directives
            , 300 - (COUNT(f.ACTUAL_ARRIVAL) % 300)::NUMERIC FLIGHTS_TO_MAINTENANCE
        FROM {{ ref('stg_aircrafts') }} a
        LEFT JOIN {{ ref('stg_flights') }} f
        USING(AIRCRAFT_CODE)
        GROUP BY 1,2,3,4
    )

 , unique_source AS (
    SELECT  
        *,
        ROW_NUMBER() OVER (PARTITION BY AIRCRAFT_CODE ORDER BY FIRST_TRIP_DATE ASC) AS row_number
    FROM source

 )


        SELECT 
            AIRCRAFT_ID
            , AIRCRAFT_CODE
            , AIRCRAFT_MODEL
            , RANGE
            , FIRST_TRIP_DATE
            , AGE_IN_FLEET
            , NUMBER_OF_TRIPS
            , NUMBER_OF_MAINTENANCE
            , FLIGHTS_TO_MAINTENANCE
            , CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
        FROM unique_source
        WHERE row_number = 1
        ORDER BY 1