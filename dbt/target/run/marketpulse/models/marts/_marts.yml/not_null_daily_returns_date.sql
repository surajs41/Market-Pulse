
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "marketpulse"."analytics"."daily_returns"
where date is null



  
  
      
    ) dbt_internal_test