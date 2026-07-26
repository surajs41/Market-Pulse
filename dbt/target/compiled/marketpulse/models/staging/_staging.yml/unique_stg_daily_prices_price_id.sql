
    
    

select
    price_id as unique_field,
    count(*) as n_records

from "marketpulse"."analytics"."stg_daily_prices"
where price_id is not null
group by price_id
having count(*) > 1


