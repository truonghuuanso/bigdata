select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select encounter_id
from "hospital_dw"."public"."stg_fact_admission"
where encounter_id is null



      
    ) dbt_internal_test