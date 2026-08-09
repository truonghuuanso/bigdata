"""
Tang 2 (hoan thien) - Spark Structured Streaming doc truc tiep tu Kafka,
phat hien bat thuong sinh hieu theo thoi gian thuc (canh bao qua tai
cap cuu - thu thach nang cao / diem cong), ghi ket qua ra MinIO.

Chay bang spark-submit ben trong container spark-master, CHAY SONG SONG
voi kafka_producer.py (kafka_producer.py chay tren Windows, script nay
chay trong Docker - ca 2 phai cung chay 1 luc thi moi co du lieu).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

# ----------------------------------------------------------------------
# 1. Khoi tao Spark session - can them package spark-sql-kafka
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("HospitalDW-Layer2-StreamingVitals")
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
KAFKA_BOOTSTRAP = "kafka:29092"   # listener noi bo Docker, khac voi localhost:9092 producer dung
TOPIC_NAME = "hospital_admissions_stream"

# ----------------------------------------------------------------------
# 2. Schema - chi khai bao cac field can dung, cac field con lai trong
#    JSON (50 cot dataset goc) se tu dong bi bo qua khi parse
# ----------------------------------------------------------------------
vitals_schema = StructType(
    [
        StructField("encounter_id", LongType()),
        StructField("patient_nbr", LongType()),
        StructField("timestamp", StringType()),
        StructField("heart_rate", IntegerType()),
        StructField("blood_pressure_sys", IntegerType()),
        StructField("spo2", IntegerType()),
    ]
)

# ----------------------------------------------------------------------
# 3. Doc stream tho tu Kafka
# ----------------------------------------------------------------------
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

# ----------------------------------------------------------------------
# 4. Parse JSON, chuan hoa kieu du lieu, gan co canh bao bat thuong
# ----------------------------------------------------------------------
parsed = raw_stream.select(
    F.from_json(F.col("value").cast("string"), vitals_schema).alias("data")
).select("data.*")

parsed = parsed.withColumn("event_time", F.to_timestamp("timestamp"))

# Nguong canh bao: nhip tim bat thuong hoac SpO2 thap (thieu oxy)
parsed = parsed.withColumn(
    "alert_flag",
    (F.col("heart_rate") > 120)
    | (F.col("heart_rate") < 50)
    | (F.col("spo2") < 90),
)
parsed = parsed.withColumn(
    "alert_reason",
    F.when(F.col("heart_rate") > 120, "Nhip tim qua nhanh")
    .when(F.col("heart_rate") < 50, "Nhip tim qua cham")
    .when(F.col("spo2") < 90, "Thieu oxy mau (SpO2 thap)")
    .otherwise(F.lit(None)),
)

# ----------------------------------------------------------------------
# 5a. Ghi TOAN BO du lieu sinh hieu ra MinIO (streaming_vitals zone)
#     de sau nay co the phan tich lich su / dua vao dashboard
# ----------------------------------------------------------------------
query_all = (
    parsed.writeStream.format("parquet")
    .option("path", f"{CURATED_BUCKET}/streaming_vitals")
    .option("checkpointLocation", "/tmp/checkpoints/streaming_vitals")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# ----------------------------------------------------------------------
# 5b. Rieng cac ca CANH BAO (bat thuong) - in ra console de demo truc
#     quan realtime khi bao ve do an
# ----------------------------------------------------------------------
alerts_only = parsed.filter(F.col("alert_flag") == True)  # noqa: E712

query_alerts = (
    alerts_only.select(
        "event_time", "encounter_id", "patient_nbr", "heart_rate", "spo2", "alert_reason"
    )
    .writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start()
)

print(">>> Dang lang nghe stream tu Kafka... Nhan Ctrl+C de dung.")
print(">>> Nho chay kafka_producer.py o mot terminal khac (Windows) de co du lieu chay vao.")

spark.streams.awaitAnyTermination()