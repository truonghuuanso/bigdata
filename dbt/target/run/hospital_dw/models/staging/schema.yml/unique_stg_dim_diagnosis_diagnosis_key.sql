select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    diagnosis_key as unique_field,
    count(*) as n_records

from "hospital_dw"."public"."stg_dim_diagnosis"
where diagnosis_key is not null
group by diagnosis_key
having count(*) > 1



      
    ) dbt_internal_test