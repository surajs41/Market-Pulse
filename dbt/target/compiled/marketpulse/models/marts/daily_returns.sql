-- Mart: daily returns — primary feature table for ML forecasting

select
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    lag(close) over (
        partition by ticker
        order by date
    ) as prev_close,
    (close - lag(close) over (
        partition by ticker
        order by date
    )) / nullif(lag(close) over (
        partition by ticker
        order by date
    ), 0) as daily_return,
    case
        when (close - lag(close) over (
            partition by ticker
            order by date
        )) / nullif(lag(close) over (
            partition by ticker
            order by date
        ), 0) > 0 then true
        else false
    end as is_positive
from "marketpulse"."analytics"."stg_daily_prices"