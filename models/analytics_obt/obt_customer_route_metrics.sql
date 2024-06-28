WITH routes AS (

    SELECT FLIGHT_ID
           , FLIGHT_NO
           , a1.AIRPORT_CODE DEPARTURE_AIRPORT_CODE
           , a2.AIRPORT_CODE ARRIVAL_AIRPORT_CODE
           , a1.AIRPORT_NAME DEPARTURE_AIRPORT
           , a2.AIRPORT_NAME ARRIVAL_AIRPORT
           , SCHEDULED_DEPARTURE FLIGHT_DATETIME
           , STATUS
    FROM {{ ref('fact_flights') }} f
    LEFT JOIN {{ ref('dim_airports') }} a1
    ON f.DEPARTURE_AIRPORT_id = a1.airport_id
    LEFT JOIN {{ ref('dim_airports') }} a2
    ON f.ARRIVAL_AIRPORT_id = a2.airport_id
    ORDER BY 1
    
) 

      
, unique_trips AS (

    SELECT ti.CUSTOMER_ID
           , FLIGHT_ID
           , FLIGHT_NO
           , DEPARTURE_AIRPORT_CODE|| ' - ' || ARRIVAL_AIRPORT_CODE AS ROUTE_ID
           , DEPARTURE_AIRPORT
           , ARRIVAL_AIRPORT
           , FLIGHT_DATETIME
           , STATUS
           , ti.TICKET_NO
           , ti.FARE_CONDITIONS
           , AMOUNT
           , ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID, TICKET_NO, FLIGHT_ID, FLIGHT_NO ORDER BY FLIGHT_DATETIME) rn
    FROM routes
    LEFT JOIN {{ ref('fact_tickets') }} ti
    USING(FLIGHT_ID)
    ORDER BY 1

)

SELECT CUSTOMER_ID
       , PASSENGER_NAME
       , SIGNUP_DATE
       , NUMBER_OF_TRIPS
       , CUMULATIVE_PASSENGER_REVENUE
       , MOST_RECENT_TRIP
       , FLIGHT_ID
       , ROUTE_ID
       , COUNT(ROUTE_ID) FREQUENCY_OF_TRIPS_BY_ROUTE_ID
       , DEPARTURE_AIRPORT
       , ARRIVAL_AIRPORT
       , FLIGHT_DATETIME
       , STATUS
       , FARE_CONDITIONS
       , AMOUNT
       , CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
FROM unique_trips
LEFT JOIN {{ ref('dim_customers') }} c
USING(CUSTOMER_ID)
WHERE rn = 1
GROUP BY 1,2,3,4,5,6,7,8,10,11,12,13,14,15
ORDER BY 1