
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    price_id as unique_field,
    count(*) as n_records

from "marketpulse"."analytics"."stg_daily_prices"
where price_id is not null
group by price_id
having count(*) > 1



  
  
      
    ) dbt_internal_test