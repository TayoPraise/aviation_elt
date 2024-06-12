{{ config(
    materialized='table',
    cluster_by=['book_date']
) }}


WITH source AS (

SELECT 
    t.TICKET_ID
    , t.TICKET_NO
    , tf.FLIGHT_ID
    , c.CUSTOMER_ID
    , t.BOOK_REF
    , bp.SEAT_NO         --In a cases where the seat no is null, the ticket_number didn't board meaning the passenger missed the flight
    , b.BOOK_DATE::DATE book_date
    , tf.FARE_CONDITIONS
    , tf.AMOUNT
FROM {{ ref('stg_tickets') }} t
LEFT JOIN {{ ref('stg_ticket_flights') }} tf
USING(TICKET_NO)
LEFT JOIN {{ ref('stg_customers') }} c
USING(PASSENGER_ID)
LEFT JOIN {{ ref('stg_bookings') }} b
USING(BOOK_REF)
LEFT JOIN {{ ref('stg_boarding_passes') }} bp
USING(TICKET_NO)
)

, unique_source AS (

    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY TICKET_ID, TICKET_NO, FLIGHT_ID, CUSTOMER_ID, BOOK_REF, SEAT_NO ORDER BY BOOK_DATE) as row_number
    FROM source
)

SELECT
    TICKET_ID
    , TICKET_NO
    , FLIGHT_ID
    , CUSTOMER_ID
    , BOOK_REF
    , SEAT_NO
    , BOOK_DATE
    , FARE_CONDITIONS
    , AMOUNT
    , CURRENT_TIMESTAMP() INGESTION_TIMESTAMP
FROM unique_source
WHERE row_number = 1
ORDER BY 1


