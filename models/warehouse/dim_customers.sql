WITH source AS (

    SELECT 
        c.CUSTOMER_ID
        , c.PASSENGER_ID
        , c.PASSENGER_NAME
        , c.CONTACT_NUMBER
        , c.CONTACT_EMAIL
        , MIN(b.BOOK_DATE)::DATE SIGNUP_DATE
        , SUM(tf.amount)::NUMERIC(15,2) CUMULATIVE_PASSENGER_REVENUE
        , COUNT(t.PASSENGER_ID) NUMBER_OF_TRIPS
        , MAX(b.BOOK_DATE)::DATE MOST_RECENT_TRIP
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_tickets') }} t
    USING(PASSENGER_ID)
    LEFT JOIN {{ ref('stg_bookings') }} b
    USING(BOOK_REF)
    LEFT JOIN {{ ref('stg_ticket_flights') }} tf
   USING(TICKET_NO)
    GROUP BY 1,2,3,4,5
    ORDER BY 1
)

, unique_source AS (
    SELECT  
        *,
        ROW_NUMBER() OVER (PARTITION BY PASSENGER_ID ORDER BY SIGNUP_DATE ASC) AS row_number
    FROM source
)

SELECT 
    CUSTOMER_ID
    , PASSENGER_ID
    , PASSENGER_NAME
    , CONTACT_NUMBER
    , CONTACT_EMAIL
    , SIGNUP_DATE
    , CUMULATIVE_PASSENGER_REVENUE
    , NUMBER_OF_TRIPS
    , MOST_RECENT_TRIP
    , CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
FROM unique_source
WHERE row_number = 1
ORDER BY 1