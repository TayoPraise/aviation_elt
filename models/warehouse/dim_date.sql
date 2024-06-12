WITH CTE_MY_DATE AS (
    SELECT DATEADD(HOUR, SEQ4(), '2016-01-01 00:00:00') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 20000))
)
SELECT
    TO_CHAR(d, 'YYYY-MM-DD') AS id,
    d AS full_date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(WEEK FROM d) AS year_week,
    EXTRACT(DAY FROM d) AS year_day,
    EXTRACT(YEAR FROM d) AS fiscal_year,
    EXTRACT(QUARTER FROM d) AS fiscal_qtr,
    EXTRACT(MONTH FROM d) AS month,
    MONTHNAME(d) AS month_name,
    DAYOFWEEK(d) AS week_day,
    DAYNAME(d) AS day_name,
    (CASE WHEN DAYNAME(d) IN ('Sun', 'Sat') THEN 0 ELSE 1 END) AS day_is_weekday
FROM CTE_MY_DATE
