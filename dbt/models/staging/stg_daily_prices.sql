-- Staging layer: cleaned daily prices from raw batch data

select
    ticker || '_' || date::text as price_id,
    ticker,
    date::date as date,
    open,
    high,
    low,
    close,
    adj_close as adjusted_close,
    volume,
    ingested_at
from {{ source('raw', 'daily_prices') }}
where close is not null
  and close > 0
