from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

raw = spark.read.csv(f"{RAW_BUCKET}/diabetic_data.csv", header=True, inferSchema=True)
mapping_raw = spark.read.csv(f"{RAW_BUCKET}/IDS_mapping.csv", header=True, inferSchema=True)

print(f"Da doc {raw.count()} dong tu diabetic_data.csv")
df = raw.drop("weight")

cols_to_clean = ["race", "payer_code", "medical_specialty", "diag_1", "diag_2", "diag_3"]
for c in cols_to_clean:
    df = df.withColumn(c, F.when(F.col(c) == "?", None).otherwise(F.col(c)))

df = df.fillna({"medical_specialty": "Unknown", "payer_code": "Unknown", "race": "Unknown"})

df = df.filter(~F.col("discharge_disposition_id").isin([11, 19, 20, 21]))

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

df = df.withColumnRenamed("time_in_hospital", "los_days")


def icd9_group_expr(col_name):
    c = F.col(col_name)
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
df = df.withColumn(
    "readmitted_30d", F.when(F.col("readmitted") == "<30", 1).otherwise(0)
)

import csv

raw_lines = [row.value for row in spark.read.text(f"{RAW_BUCKET}/IDS_mapping.csv").collect()]

blocks = {}
current_block = None
for row in csv.reader(raw_lines):
    if not row or all(cell.strip() == "" for cell in row):
        continue
    first_col, second_col = row[0].strip(), (row[1].strip() if len(row) > 1 else "")
    if second_col.lower() == "description":
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

def add_surrogate_key(dim_df, key_name):
    return dim_df.withColumn(key_name, F.monotonically_increasing_id())

dim_patient = df.select("patient_nbr", "race", "gender", "age").dropDuplicates(
    ["patient_nbr"]
)
dim_patient = add_surrogate_key(dim_patient, "patient_key")

dim_department = df.select("medical_specialty").dropDuplicates()
dim_department = add_surrogate_key(dim_department, "department_key")

dim_diagnosis = df.select(
    "diag_1_group", "admission_type_desc", "discharge_disposition_desc", "admission_source_desc"
).dropDuplicates()
dim_diagnosis = add_surrogate_key(dim_diagnosis, "diagnosis_key")

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

(
    fact_admission.write.mode("overwrite")
    .partitionBy("admission_year", "admission_month")
    .parquet(f"{CURATED_BUCKET}/fact_admission")
)
dim_patient.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/dim_patient")
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