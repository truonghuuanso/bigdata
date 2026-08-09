"""
Tang 5 - Nap star schema (1 fact + 4 dimension) tu Parquet trong MinIO
vao Postgres (thay the cho Amazon Redshift trong ban open-source).

Chay bang spark-submit ben trong container spark-master.
"""

from pyspark.sql import SparkSession

# ----------------------------------------------------------------------
# 1. Khoi tao Spark session - can 2 bo package: hadoop-aws (doc MinIO)
#    va postgresql JDBC driver (ghi vao Postgres)
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("HospitalDW-Layer5-LoadToPostgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CURATED_BUCKET = "s3a://hospital-curated"

# ----------------------------------------------------------------------
# 2. Thong tin ket noi Postgres (dung dung ten service + credential
#    da khai bao trong docker-compose.yml)
# ----------------------------------------------------------------------
PG_URL = "jdbc:postgresql://postgres:5432/hospital_dw"
PG_PROPERTIES = {
    "user": "dwuser",
    "password": "dwpassword",
    "driver": "org.postgresql.Driver",
}

# ----------------------------------------------------------------------
# 3. Doc tung bang Parquet tu MinIO, ghi vao Postgres
#    mode "overwrite": moi lan chay se tao lai bang tu dau (phu hop khi
#    dang phat trien/test - sau nay chuyen sang Airflow co the doi
#    thanh incremental neu can)
# ----------------------------------------------------------------------
tables = ["fact_admission", "dim_patient", "dim_department", "dim_diagnosis", "dim_date"]

for table_name in tables:
    print(f"Dang doc {table_name} tu MinIO...")
    df = spark.read.parquet(f"{CURATED_BUCKET}/{table_name}")
    row_count = df.count()

    print(f"Dang ghi {row_count} dong vao Postgres bang '{table_name}'...")
    (
        df.write.mode("overwrite")
        .option("truncate", "true")  # xoa sach du lieu cu, GIU NGUYEN bang (khong drop)
        .jdbc(url=PG_URL, table=table_name, properties=PG_PROPERTIES)
    )
    print(f"  -> Xong: {table_name} ({row_count} dong)\n")

print("HOAN TAT tang 5. Da nap 1 fact + 4 dimension vao Postgres (hospital_dw).")

spark.stop()