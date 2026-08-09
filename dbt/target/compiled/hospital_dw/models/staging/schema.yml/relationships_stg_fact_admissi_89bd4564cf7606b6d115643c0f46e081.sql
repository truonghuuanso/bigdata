
    
    

with child as (
    select patient_key as from_field
    from "hospital_dw"."public"."stg_fact_admission"
    where patient_key is not null
),

parent as (
    select patient_key as to_field
    from "hospital_dw"."public"."stg_dim_patient"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


