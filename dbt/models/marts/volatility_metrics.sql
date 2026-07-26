-- Mart: volatility and volume metrics (depends on daily_returns)

select
    dr.ticker,
    dr.date,
    dr.daily_return,
    stddev(dr.daily_return) over (
        partition by dr.ticker
        order by dr.date
        rows between 19 preceding and current row
    ) as rolling_std_20,
    stddev(dr.daily_return) over (
        partition by dr.ticker
        order by dr.date
        rows between 4 preceding and current row
    ) as rolling_std_5,
    (sdp.high - sdp.low) / nullif(sdp.close, 0) as high_low_range,
    avg(sdp.volume) over (
        partition by dr.ticker
        order by dr.date
        rows between 19 preceding and current row
    ) as avg_volume_20
from {{ ref('daily_returns') }} as dr
inner join {{ ref('stg_daily_prices') }} as sdp
    on dr.ticker = sdp.ticker
    and dr.date = sdp.date
