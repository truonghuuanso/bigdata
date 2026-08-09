
      
  
    

  create  table "hospital_dw"."public"."snapshot_dim_patient"
  
  
    as
  
  (
    

    select *,
        md5(coalesce(cast(patient_key as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        nullif(now()::timestamp without time zone, now()::timestamp without time zone) as dbt_valid_to
    from (
        



select * from "hospital_dw"."public"."dim_patient"

    ) sbq



  );
  
  