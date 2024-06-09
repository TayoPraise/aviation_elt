WITH source as (
    SELECT 
        ROW_NUMBER() OVER (order by TICKET_NO) AS CUSTOMER_ID
        , PASSENGER_ID
        , PASSENGER_NAME
        , PARSE_JSON(CONTACT_DATA):phone::STRING AS CONTACT_NUMBER      --extracting the phone number value from the contact_data json cell 
        , PARSE_JSON(CONTACT_DATA):email::STRING AS CONTACT_EMAIL       --extracting the email address value from the contact_data json cell
    FROM {{ source('TY_AVIATION', 'SRC_TICKETS') }}
)

SELECT 
    *
    , CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source