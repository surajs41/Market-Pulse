-- Staging layer: cleaned intraday ticks from streaming pipeline

select
    id,
    ticker,
    ts::timestamptz as ts,
    date(ts) as tick_date,
    extract(hour from ts)::int as tick_hour,
    open,
    high,
    low,
    close,
    volume,
    produced_at,
    consumed_at
from "marketpulse"."streaming"."market_ticks"
where close is not null
  and close > 0