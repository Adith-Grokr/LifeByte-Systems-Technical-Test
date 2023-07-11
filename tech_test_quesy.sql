SELECT
    rank() OVER (ORDER BY t.open_time::date, t.login_hash, t.server_hash, t.symbol) AS row_number,
    t.login_hash,
    t.server_hash,
	t.open_time::date AS dt_report,
    t.symbol,
    u.currency,
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-06-01' AND '2020-09-30') OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_prev_all,
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-06-01' AND t.open_time::date) OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_volume_prev_7d,
    dense_rank() OVER (PARTITION BY t.login_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rank_volume_symbol_prev_7d,
    dense_rank() OVER (PARTITION BY t.login_hash ORDER BY t.open_time::date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rank_count_prev_7d,
    sum(t.volume) FILTER (WHERE t.open_time::date BETWEEN '2020-08-01' AND '2020-08-31') OVER
        (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_2020_08,
    MIN(Date(t.open_time)) OVER (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.open_time::date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS date_first_trade
FROM
    trades AS t
    JOIN users AS u ON t.login_hash = u.login_hash AND t.server_hash=u.server_hash
WHERE
    t.open_time::date BETWEEN '2020-06-01' AND '2020-09-30'
    AND u.enable = 1
	AND u.login_hash is not NULL
	AND u.server_hash is not NULL
	AND t.contractsize is not NULL
ORDER BY
    row_number DESC