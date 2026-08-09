"""
Tang 3 - Xu ly PySpark: chuan hoa ma chan doan, tinh LOS, tao dac trung
nguy co tai nhap, va xay dung star schema (Fact_Admission + 4 Dim).

Chay bang spark-submit ben trong container spark-master (xem huong dan
chay o cuoi file / trong tin nhan).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------------------------------------------
# 1. Khoi tao Spark session, cau hinh ket noi MinIO (S3-compatible)
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("HospitalDW-Layer3-Processing")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

RAW_BUCKET = "s3a://hospital-raw"
CURATED_BUCKET = "s3a://hospital-curated"

# ----------------------------------------------------------------------
# 2. Doc du lieu tho
# ----------------------------------------------------------------------
raw = spark.read.csv(f"{RAW_BUCKET}/diabetic_data.csv", header=True, inferSchema=True)
mapping_raw = spark.read.csv(f"{RAW_BUCKET}/IDS_mapping.csv", header=True, inferSchema=True)

print(f"Da doc {raw.count()} dong tu diabetic_data.csv")

# ----------------------------------------------------------------------
# 3. Lam sach du lieu
# ----------------------------------------------------------------------
# 3a. Bo cot weight (thieu 97%, khong dung duoc)
df = raw.drop("weight")

# 3b. Thay '?' bang null, roi dien 'Unknown' cho vai cot quan trong
cols_to_clean = ["race", "payer_code", "medical_specialty", "diag_1", "diag_2", "diag_3"]
for c in cols_to_clean:
    df = df.withColumn(c, F.when(F.col(c) == "?", None).otherwise(F.col(c)))

df = df.fillna({"medical_specialty": "Unknown", "payer_code": "Unknown", "race": "Unknown"})

# 3c. Loai bo ban ghi ma benh nhan da tu vong hoac chuyen hospice
#     (discharge_disposition_id 11,19,20,21 = expired/hospice -> khong the "tai nhap")
df = df.filter(~F.col("discharge_disposition_id").isin([11, 19, 20, 21]))

# ----------------------------------------------------------------------
# 4. Mo phong ngay nhap vien (dataset goc khong co timestamp that)
#    Gan ngay tang dan theo encounter_id trong khoang 1999-2008
# ----------------------------------------------------------------------
w = Window.orderBy("encounter_id")
df = df.withColumn("row_num", F.row_number().over(w))
total_rows = df.count()

df = df.withColumn(
    "admission_date",
    F.expr(
        f"date_add(to_date('1999-01-01'), "
        f"cast((row_num / {total_rows}) * 3650 as int))"
    ),
)
df = df.withColumn(
    "discharge_date", F.expr("date_add(admission_date, time_in_hospital)")
)
df = df.drop("row_num")

# LOS = time_in_hospital co san trong dataset (don vi: ngay) -> giu nguyen lam LOS
df = df.withColumnRenamed("time_in_hospital", "los_days")

# ----------------------------------------------------------------------
# 5. Nhom ma ICD-9 (diag_1) thanh nhom benh lon (chuan hoa chan doan)
# ----------------------------------------------------------------------
def icd9_group_expr(col_name):
    c = F.col(col_name)
    # Ma dang V/E la nhom rieng (yeu to ben ngoai / phan loai bo sung)
    return (
        F.when(c.isNull(), "Unknown")
        .when(c.startswith("V"), "Supplemental (V-code)")
        .when(c.startswith("E"), "External cause (E-code)")
        .when((c.cast("double") >= 250) & (c.cast("double") < 251), "Diabetes")
        .when((c.cast("double") >= 390) & (c.cast("double") <= 459), "Circulatory")
        .when(c.cast("double") == 785, "Circulatory")
        .when((c.cast("double") >= 460) & (c.cast("double") <= 519), "Respiratory")
        .when(c.cast("double") == 786, "Respiratory")
        .when((c.cast("double") >= 520) & (c.cast("double") <= 579), "Digestive")
        .when(c.cast("double") == 787, "Digestive")
        .when((c.cast("double") >= 580) & (c.cast("double") <= 629), "Genitourinary")
        .when((c.cast("double") >= 800) & (c.cast("double") <= 999), "Injury")
        .when((c.cast("double") >= 710) & (c.cast("double") <= 739), "Musculoskeletal")
        .when((c.cast("double") >= 140) & (c.cast("double") <= 239), "Neoplasms")
        .otherwise("Other")
    )

df = df.withColumn("diag_1_group", icd9_group_expr("diag_1"))

# ----------------------------------------------------------------------
# 6. Tao dac trung nguy co tai nhap (feature engineering)
# ----------------------------------------------------------------------
df = df.withColumn(
    "prior_visits_total",
    F.col("number_outpatient") + F.col("number_emergency") + F.col("number_inpatient"),
)
df = df.withColumn(
    "readmit_risk_flag",
    F.when(
        (F.col("prior_visits_total") >= 3) | (F.col("number_inpatient") >= 2), "High"
    )
    .when((F.col("prior_visits_total") >= 1), "Medium")
    .otherwise("Low"),
)
# Bien muc tieu nhi phan cho model ML sau nay: tai nhap trong 30 ngay hay khong
df = df.withColumn(
    "readmitted_30d", F.when(F.col("readmitted") == "<30", 1).otherwise(0)
)

# ----------------------------------------------------------------------
# 7. Giai ma cac cot *_id qua IDS_mapping
#    File nay la 3 bang con (admission_type_id, discharge_disposition_id,
#    admission_source_id) bi dong chung vao 2 cot trong cung 1 file CSV,
#    voi dong "header" cua tung bang con nam LAN giua nhu du lieu.
#    Phai tach rieng tung bang con truoc khi join, neu khong 1 ma so
#    (vd id=1) se bi trung giua 3 bang -> join se nhan ban dong len 3 lan.
# ----------------------------------------------------------------------
import csv

raw_lines = [row.value for row in spark.read.text(f"{RAW_BUCKET}/IDS_mapping.csv").collect()]

blocks = {}
current_block = None
for row in csv.reader(raw_lines):
    if not row or all(cell.strip() == "" for cell in row):
        continue
    first_col, second_col = row[0].strip(), (row[1].strip() if len(row) > 1 else "")
    if second_col.lower() == "description":
        # Day la dong "header" danh dau bat dau 1 bang con moi
        current_block = first_col
        blocks[current_block] = []
        continue
    if current_block:
        blocks[current_block].append((first_col, second_col))


def build_lookup_df(block_name, id_col_name, desc_col_name):
    rows = [(int(i), d) for i, d in blocks.get(block_name, []) if i.isdigit()]
    return spark.createDataFrame(rows, [id_col_name, desc_col_name])


admission_type_map = build_lookup_df(
    "admission_type_id", "admission_type_id", "admission_type_desc"
)
discharge_disposition_map = build_lookup_df(
    "discharge_disposition_id", "discharge_disposition_id", "discharge_disposition_desc"
)
admission_source_map = build_lookup_df(
    "admission_source_id", "admission_source_id", "admission_source_desc"
)

df = df.join(admission_type_map, on="admission_type_id", how="left")
df = df.join(discharge_disposition_map, on="discharge_disposition_id", how="left")
df = df.join(admission_source_map, on="admission_source_id", how="left")
df = df.fillna(
    {
        "admission_type_desc": "Unknown",
        "discharge_disposition_desc": "Unknown",
        "admission_source_desc": "Unknown",
    }
)

# ----------------------------------------------------------------------
# 8. Xay dung cac bang DIMENSION (surrogate key tu monotonically_increasing_id)
# ----------------------------------------------------------------------
def add_surrogate_key(dim_df, key_name):
    return dim_df.withColumn(key_name, F.monotonically_increasing_id())

# --- Dim_Patient ---
dim_patient = df.select("patient_nbr", "race", "gender", "age").dropDuplicates(
    ["patient_nbr"]
)
dim_patient = add_surrogate_key(dim_patient, "patient_key")

# --- Dim_Department ---
dim_department = df.select("medical_specialty").dropDuplicates()
dim_department = add_surrogate_key(dim_department, "department_key")

# --- Dim_Diagnosis ---
dim_diagnosis = df.select(
    "diag_1_group", "admission_type_desc", "discharge_disposition_desc", "admission_source_desc"
).dropDuplicates()
dim_diagnosis = add_surrogate_key(dim_diagnosis, "diagnosis_key")

# --- Dim_Date (tu ngay nho nhat den lon nhat trong du lieu) ---
date_bounds = df.select(
    F.min("admission_date").alias("min_d"), F.max("discharge_date").alias("max_d")
).collect()[0]

dim_date = spark.sql(
    f"""
    SELECT explode(sequence(
        to_date('{date_bounds['min_d']}'),
        to_date('{date_bounds['max_d']}'),
        interval 1 day
    )) as full_date
    """
)
dim_date = (
    dim_date.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
)

# ----------------------------------------------------------------------
# 9. Xay dung FACT_ADMISSION - join voi cac dim de lay surrogate key
# ----------------------------------------------------------------------
fact = df.join(
    dim_patient.select("patient_nbr", "patient_key"), on="patient_nbr", how="left"
)
fact = fact.join(dim_department, on="medical_specialty", how="left")
fact = fact.join(
    dim_diagnosis,
    on=["diag_1_group", "admission_type_desc", "discharge_disposition_desc", "admission_source_desc"],
    how="left",
)
fact = fact.withColumn(
    "date_key", F.date_format("admission_date", "yyyyMMdd").cast("int")
)
# Cot dung de partition khi ghi Parquet - theo nam/thang cua admission_date
# (partition theo tung ngay le se tao qua nhieu folder nho, khong thuc te)
fact = fact.withColumn("admission_year", F.year("admission_date"))
fact = fact.withColumn("admission_month", F.month("admission_date"))

fact_admission = fact.select(
    "encounter_id",
    "patient_key",
    "department_key",
    "diagnosis_key",
    "date_key",
    "los_days",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_diagnoses",
    "prior_visits_total",
    "readmit_risk_flag",
    "readmitted",
    "readmitted_30d",
    "admission_year",
    "admission_month",
)

# ----------------------------------------------------------------------
# 10. Ghi ket qua ra Parquet vao curated zone
#     fact_admission duoc partition theo admission_date (nam/thang) theo
#     dung yeu cau de bai "S3 Parquet partition theo admission_date"
# ----------------------------------------------------------------------
(
    fact_admission.write.mode("overwrite")
    .partitionBy("admission_year", "admission_month")
    .parquet(f"{CURATED_BUCKET}/fact_admission")
)
dim_patient.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/dim_patient")
# (fact_admission da duoc ghi o buoc 10 ben tren, co partitionBy)
dim_department.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/dim_department")
dim_diagnosis.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/dim_diagnosis")
dim_date.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/dim_date")

expected_rows = df.count()
actual_rows = fact_admission.count()
print(f"KIEM TRA: du lieu sau lam sach co {expected_rows} dong, fact_admission co {actual_rows} dong")
if actual_rows != expected_rows:
    print("CANH BAO: so dong lech nhau -> join dang bi fan-out (trung lap khoa), can kiem tra lai!")

print("HOAN TAT tang 3. Da ghi 1 fact + 4 dimension vao hospital-curated/")
print(f"  fact_admission: {fact_admission.count()} dong")
print(f"  dim_patient   : {dim_patient.count()} dong")
print(f"  dim_department: {dim_department.count()} dong")
print(f"  dim_diagnosis : {dim_diagnosis.count()} dong")
print(f"  dim_date      : {dim_date.count()} dong")

spark.stop()