
    
    

select
    encounter_id as unique_field,
    count(*) as n_records

from "hospital_dw"."public"."stg_fact_admission"
where encounter_id is not null
group by encounter_id
having count(*) > 1


