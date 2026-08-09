-- Mart: ty le tai nhap vien 30 ngay, LOS trung binh, theo tung khoa/phong

select
    dd.medical_specialty,
    count(*) as so_luot_kham,
    sum(f.readmitted_30d) as so_ca_tai_nhap_30_ngay,
    round(
        100.0 * sum(f.readmitted_30d) / nullif(count(*), 0), 2
    ) as ty_le_tai_nhap_pct,
    round(avg(f.los_days), 2) as los_trung_binh_ngay,
    round(avg(f.num_medications), 2) as so_thuoc_trung_binh
from {{ ref('stg_fact_admission') }} f
join {{ ref('stg_dim_department') }} dd
    on f.department_key = dd.department_key
group by dd.medical_specialty
order by so_luot_kham desc