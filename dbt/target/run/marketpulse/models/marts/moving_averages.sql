
  
    

  create  table "marketpulse"."analytics"."moving_averages__dbt_tmp"
  
  
    as
  
  (
    -- Mart: moving averages and price-vs-MA ratio

select
    ticker,
    date,
    close,
    avg(close) over (
        partition by ticker
        order by date
        rows between 6 preceding and current row
    ) as ma_7,
    avg(close) over (
        partition by ticker
        order by date
        rows between 19 preceding and current row
    ) as ma_20,
    avg(close) over (
        partition by ticker
        order by date
        rows between 49 preceding and current row
    ) as ma_50,
    close / nullif(avg(close) over (
        partition by ticker
        order by date
        rows between 19 preceding and current row
    ), 0) as price_vs_ma20
from "marketpulse"."analytics"."stg_daily_prices"
  );
  