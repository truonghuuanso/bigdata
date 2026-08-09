select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select patient_key
from "hospital_dw"."public"."stg_dim_patient"
where patient_key is null



      
    ) dbt_internal_test