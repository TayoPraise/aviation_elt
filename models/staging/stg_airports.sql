WITH source AS (
    SELECT
         ROW_NUMBER() OVER (ORDER BY AIRPORT_CODE) AS AIRPORT_ID
        , AIRPORT_CODE
        , PARSE_JSON(AIRPORT_NAME):en::STRING AS AIRPORT_NAME   --extracting the english translation from the json cell 
        , PARSE_JSON(CITY):en::STRING AS CITY                   --extracting the english translation from the json cell 
        , TIMEZONE
        , COORDINATES
    FROM {{ source('TY_AVIATION', 'SRC_AIRPORTS_DATA')}}


)

SELECT *
      , CURRENT_TIMESTAMP() AS injestion_timestamp
FROM source