SELECT
    rank() OVER (ORDER BY t.open_time::date, t.login_hash, t.server_hash, t.symbol) AS row_number, -- Assigns a unique row number to each row based on the specified order
    t.login_hash,
    t.server_hash,
    t.open_time::date AS dt_report, -- Retrieves the date portion from the open_time column and assigns it to dt_report (Based On Assumtion as the related Data Was not Given on This column)
    t.symbol,
    u.currency,
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-06-01' AND '2020-09-30') OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_prev_all, -- Calculates the sum of volume for each combination of login_hash, server_hash, and symbol Of all The Privious Days including current
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-06-01' AND t.open_time::date) OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_volume_prev_7d, -- Calculates the sum of volume for each combination of login_hash, server_hash, and symbol within the previous 7 days
    dense_rank() OVER (PARTITION BY t.login_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rank_volume_symbol_prev_7d, -- Assigns a dense rank to each combination of login_hash and symbol based on the sum of volume within the previous 7 days
    dense_rank() OVER (PARTITION BY t.login_hash ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rank_count_prev_7d, -- Assigns a dense rank to each login_hash based on the count of trades within the previous 7 days
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-08-01' AND '2020-08-31') OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_2020_08, -- Calculates the sum of volume for each combination of login_hash, server_hash, and symbol within August 2020
    MIN(Date(t.open_time)) OVER (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS date_first_trade -- Retrieves the earliest date of the trade for each combination of login_hash, server_hash, and symbol
FROM
    trades AS t -- Specifies the trades table
    JOIN users AS u ON t.login_hash = u.login_hash AND t.server_hash = u.server_hash -- Joins the trades table with the users table based on login_hash and server_hash
WHERE
    t.open_time::date BETWEEN '2020-06-01' AND '2020-09-30' -- Filters trades based on the open_time falling within the specified date range
    AND u.enable = 1 -- Filters only users with active accounts
--These Conditions are derived From The Data Quality Checks Performed By Python Script
    AND u.login_hash IS NOT NULL -- Filters out rows where login_hash is NULL
    AND u.server_hash IS NOT NULL -- Filters out rows where server_hash is NULL
    AND t.contractsize IS NOT NULL -- Filters out rows where contractsize is NULL
ORDER BY
    row_number DESC -- Orders the result set by row_number in descending order
