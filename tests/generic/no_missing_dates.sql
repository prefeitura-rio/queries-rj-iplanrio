{% test no_missing_dates (model, column_name) %}
WITH
    dates AS (
        SELECT DISTINCT
            {{column_name}} AS date_columns
        FROM
            {{model}}
        WHERE
            {{column_name}} IS NOT NULL
    ),
    windowed AS (
        SELECT
            date_columns,
            LAG (date_columns) OVER (
                ORDER BY
                    date_columns
            ) AS previous_date_columns
        FROM
            dates
    )
SELECT
    date_columns,
    previous_date_columns,
    DATE_DIFF(date_columns, previous_date_columns, DAY) AS days_diff
FROM
    windowed
WHERE
    previous_date_columns IS NOT NULL
    AND DATE_DIFF(date_columns, previous_date_columns, DAY) > 1 {% endtest %}