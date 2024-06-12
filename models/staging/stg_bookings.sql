WITH source AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY BOOK_REF) AS BOOKING_ID
        , BOOK_REF
        , BOOK_DATE
        , TOTAL_AMOUNT
    FROM {{ source('TY_AVIATION', 'SRC_BOOKINGS')}}

)

SELECT *
       , CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source