
  create view "hospital_dw"."public"."stg_dim_date__dbt_tmp"
    
    
  as (
    select * from "hospital_dw"."public"."dim_date"
  );