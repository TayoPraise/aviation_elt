WITH source as (
    SELECT 
        ROW_NUMBER() OVER (order by TICKET_NO) AS TICKET_ID
        , TICKET_NO
        , BOOK_REF
        , PASSENGER_ID
    FROM {{ source('TY_AVIATION', 'SRC_TICKETS') }}
)

SELECT 
    *
    , CURRENT_TIMESTAMP() INGESTION_TIMESTAMP
FROM source