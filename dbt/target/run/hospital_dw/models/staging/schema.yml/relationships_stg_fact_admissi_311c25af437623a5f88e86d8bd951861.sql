select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select diagnosis_key as from_field
    from "hospital_dw"."public"."stg_fact_admission"
    where diagnosis_key is not null
),

parent as (
    select diagnosis_key as to_field
    from "hospital_dw"."public"."stg_dim_diagnosis"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test