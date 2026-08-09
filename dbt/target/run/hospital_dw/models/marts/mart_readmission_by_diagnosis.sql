
  
    

  create  table "hospital_dw"."public"."mart_readmission_by_diagnosis__dbt_tmp"
  
  
    as
  
  (
    -- Mart: ty le tai nhap vien 30 ngay, LOS trung binh, theo tung nhom benh (ICD-9)

select
    di.diag_1_group,
    count(*) as so_luot_kham,
    sum(f.readmitted_30d) as so_ca_tai_nhap_30_ngay,
    round(
        100.0 * sum(f.readmitted_30d) / nullif(count(*), 0), 2
    ) as ty_le_tai_nhap_pct,
    round(avg(f.los_days), 2) as los_trung_binh_ngay,
    round(avg(f.prior_visits_total), 2) as so_lan_kham_truoc_trung_binh
from "hospital_dw"."public"."stg_fact_admission" f
join "hospital_dw"."public"."stg_dim_diagnosis" di
    on f.diagnosis_key = di.diagnosis_key
group by di.diag_1_group
order by so_luot_kham desc
  );
  