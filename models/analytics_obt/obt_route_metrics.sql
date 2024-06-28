WITH airport_metric AS (
   
    WITH flight_routes AS (
        SELECT FLIGHT_ID
               , FLIGHT_NO
               , a1.AIRPORT_CODE DEPARTURE_AIRPORT_CODE
               , a2.AIRPORT_CODE ARRIVAL_AIRPORT_CODE
               , a1.AIRPORT_NAME DEPARTURE_AIRPORT
               , a2.AIRPORT_NAME ARRIVAL_AIRPORT
               , STATUS
        FROM {{ ref('fact_flights') }} f
        LEFT JOIN {{ ref('dim_airports') }} a1
        ON f.DEPARTURE_AIRPORT_ID = a1.AIRPORT_ID
        LEFT JOIN {{ ref('dim_airports') }} a2
        ON f.ARRIVAL_AIRPORT_ID = a2.AIRPORT_ID
        ORDER BY 1
    )

    , flight_revenue AS (
        SELECT FLIGHT_ID, sum(AMOUNT) REVENUE
        FROM {{ ref('fact_tickets') }} t
        GROUP BY 1
        ORDER BY 1
    )

SELECT DEPARTURE_AIRPORT_CODE
       , ARRIVAL_AIRPORT_CODE
       , DEPARTURE_AIRPORT
       , ARRIVAL_AIRPORT
       , STATUS
       , count(FLIGHT_ID) NUMBER_OF_FLIGHTS
       , SUM(REVENUE)::NUMERIC(15,2) CUMULATIVE_ROUTE_REVENUE
FROM flight_routes
LEFT JOIN flight_revenue
USING(FLIGHT_ID)
GROUP BY 1,2,3,4,5
ORDER BY 4 desc


)

, row_id AS ( 

     SELECT
     ROW_NUMBER() OVER(ORDER BY DEPARTURE_AIRPORT, ARRIVAL_AIRPORT, NUMBER_OF_FLIGHTS DESC) id
       , DEPARTURE_AIRPORT_CODE
       , ARRIVAL_AIRPORT_CODE
       , DEPARTURE_AIRPORT
       , ARRIVAL_AIRPORT
       , STATUS
       , NUMBER_OF_FLIGHTS
       , CUMULATIVE_ROUTE_REVENUE
    FROM airport_metric
)

SELECT 
    ID
    , DEPARTURE_AIRPORT_CODE|| ' - ' || ARRIVAL_AIRPORT_CODE AS ROUTE_ID
    , DEPARTURE_AIRPORT
    , ARRIVAL_AIRPORT
    , STATUS
    , NUMBER_OF_FLIGHTS
    , CUMULATIVE_ROUTE_REVENUE
    , CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
FROM row_id