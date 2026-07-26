
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close
from "marketpulse"."analytics"."stg_market_ticks"
where close is null



  
  
      
    ) dbt_internal_test