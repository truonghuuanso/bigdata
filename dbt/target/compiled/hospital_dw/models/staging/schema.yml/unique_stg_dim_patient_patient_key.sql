
    
    

select
    patient_key as unique_field,
    count(*) as n_records

from "hospital_dw"."public"."stg_dim_patient"
where patient_key is not null
group by patient_key
having count(*) > 1


