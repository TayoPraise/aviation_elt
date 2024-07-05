SELECT ID
       , FULL_DATE
       , YEAR
       , YEAR_WEEK
       , YEAR_DAY
       , FISCAL_YEAR
       , FISCAL_QTR
       , MONTH
       , MONTH_NAME
       , WEEK_DAY
       , DAY_NAME
       , DAY_IS_WEEKDAY
FROM {{ ref('dim_date')}} d