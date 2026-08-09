
  create view "hospital_dw"."public"."stg_fact_admission__dbt_tmp"
    
    
  as (
    select * from "hospital_dw"."public"."fact_admission"
  );