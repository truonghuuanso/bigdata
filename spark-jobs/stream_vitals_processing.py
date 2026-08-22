from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType


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
KAFKA_BOOTSTRAP = "kafka:29092"   
TOPIC_NAME = "hospital_admissions_stream"

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

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw_stream.select(
    F.from_json(F.col("value").cast("string"), vitals_schema).alias("data")
).select("data.*")

parsed = parsed.withColumn("event_time", F.to_timestamp("timestamp"))

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

query_all = (
    parsed.writeStream.format("parquet")
    .option("path", f"{CURATED_BUCKET}/streaming_vitals")
    .option("checkpointLocation", "/tmp/checkpoints/streaming_vitals")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

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