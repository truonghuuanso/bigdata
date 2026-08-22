from pyspark.sql import SparkSession


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
PG_URL = "jdbc:postgresql://postgres:5432/hospital_dw"
PG_PROPERTIES = {
    "user": "dwuser",
    "password": "dwpassword",
    "driver": "org.postgresql.Driver",
}

tables = ["fact_admission", "dim_patient", "dim_department", "dim_diagnosis", "dim_date"]

for table_name in tables:
    print(f"Dang doc {table_name} tu MinIO...")
    df = spark.read.parquet(f"{CURATED_BUCKET}/{table_name}")
    row_count = df.count()

    print(f"Dang ghi {row_count} dong vao Postgres bang '{table_name}'...")
    (
        df.write.mode("overwrite")
        .option("truncate", "true")  
        .jdbc(url=PG_URL, table=table_name, properties=PG_PROPERTIES)
    )
    print(f"  -> Xong: {table_name} ({row_count} dong)\n")

print("HOAN TAT tang 5. Da nap 1 fact + 4 dimension vao Postgres (hospital_dw).")

spark.stop()