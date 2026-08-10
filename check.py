"""
Script kiem tra suc khoe toan bo do an (chay sau khi sua code de biet
cho nao con loi truoc khi chay lai tung tang).

Cai thu vien can thiet (chay 1 lan, neu chua co):
    pip install docker sqlalchemy psycopg2-binary boto3

Chay: python check_pipeline_health.py
"""

import subprocess
import sys

RESULTS = []  # list of (ten_kiem_tra, True/False, ghi_chu)


def log(name, ok, note=""):
    RESULTS.append((name, ok, note))
    mark = "✅" if ok else "❌"
    print(f"{mark} {name}" + (f" — {note}" if note else ""))


def section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


# ----------------------------------------------------------------------
# 1. Kiem tra Docker containers dang chay
# ----------------------------------------------------------------------
section("1. DOCKER CONTAINERS")

EXPECTED_CONTAINERS = [
    "minio", "kafka", "kafka-ui", "spark-master", "spark-worker",
    "postgres", "airflow", "dbt", "adminer", "metabase",
]

try:
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
        capture_output=True, text=True, timeout=15,
    )
    running = {}
    for line in result.stdout.strip().splitlines():
        if "\t" in line:
            name, status = line.split("\t", 1)
            running[name] = status

    for c in EXPECTED_CONTAINERS:
        if c in running:
            log(f"Container '{c}'", "Up" in running[c], running[c])
        else:
            log(f"Container '{c}'", False, "KHONG chay - thu 'docker compose up -d'")
except Exception as e:
    log("Ket noi Docker", False, str(e))
    print("\nKhong ket noi duoc Docker. Kiem tra Docker Desktop co dang chay khong.")
    sys.exit(1)


# ----------------------------------------------------------------------
# 2. Kiem tra MinIO (bucket + file)
# ----------------------------------------------------------------------
section("2. MINIO (S3)")

try:
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    log("Bucket 'hospital-raw' ton tai", "hospital-raw" in buckets)
    log("Bucket 'hospital-curated' ton tai", "hospital-curated" in buckets)

    if "hospital-raw" in buckets:
        objs = s3.list_objects_v2(Bucket="hospital-raw").get("Contents", [])
        names = [o["Key"] for o in objs]
        log("File 'diabetic_data.csv' trong hospital-raw", "diabetic_data.csv" in names)
        log("File 'IDS_mapping.csv' trong hospital-raw", "IDS_mapping.csv" in names)

    if "hospital-curated" in buckets:
        for prefix in ["fact_admission/", "dim_patient/", "dim_department/", "dim_diagnosis/", "dim_date/"]:
            objs = s3.list_objects_v2(Bucket="hospital-curated", Prefix=prefix, MaxKeys=1)
            has_data = objs.get("KeyCount", 0) > 0
            log(f"Du lieu Parquet '{prefix}' trong hospital-curated", has_data)

except ImportError:
    log("Thu vien boto3", False, "chua cai - chay: pip install boto3")
except Exception as e:
    log("Ket noi MinIO", False, str(e))


# ----------------------------------------------------------------------
# 3. Kiem tra Postgres - 5 bang goc + dbt marts + snapshot
# ----------------------------------------------------------------------
section("3. POSTGRES - DATA WAREHOUSE")

try:
    from sqlalchemy import create_engine, text

    engine = create_engine("postgresql+psycopg2://dwuser:dwpassword@localhost:5432/hospital_dw")

    EXPECTED_TABLES = {
        "fact_admission": 90000,       # toi thieu mong doi (~100k, tru mot so ca loc bo)
        "dim_patient": 60000,
        "dim_department": 50,
        "dim_diagnosis": 1000,
        "dim_date": 1000,
        "stg_fact_admission": 90000,
        "mart_readmission_by_department": 10,
        "mart_readmission_by_diagnosis": 5,
        "snapshot_dim_patient": 60000,
    }

    with engine.connect() as conn:
        for table, min_rows in EXPECTED_TABLES.items():
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                ok = count >= min_rows
                log(f"Bang '{table}'", ok, f"{count} dong (mong doi >= {min_rows})")
            except Exception as e:
                log(f"Bang '{table}'", False, "khong ton tai hoac loi truy van")

        # Kiem tra logic: khong co dong nao patient_key bi NULL (bug da tung gap)
        try:
            null_count = conn.execute(
                text("SELECT COUNT(*) FROM fact_admission WHERE patient_key IS NULL")
            ).scalar()
            log(
                "Khong co patient_key bi NULL trong fact_admission",
                null_count == 0,
                f"{null_count} dong bi NULL" if null_count else "",
            )
        except Exception:
            log("Kiem tra NULL patient_key", False, "khong chay duoc (bang co ton tai khong?)")

        # Kiem tra ty le tai nhap hop ly (~10-13%, dung voi phan bo dataset goc)
        try:
            pct = conn.execute(
                text("SELECT AVG(readmitted_30d::float) * 100 FROM fact_admission")
            ).scalar()
            ok = pct is not None and 5 <= pct <= 20
            log("Ty le readmitted_30d hop ly (5-20%)", ok, f"{pct:.2f}%" if pct else "N/A")
        except Exception:
            log("Kiem tra ty le readmitted_30d", False, "khong chay duoc")

        # Model AI/ML da chay chua (bang optional)
        try:
            pred_count = conn.execute(
                text("SELECT COUNT(*) FROM predictions_readmission_risk")
            ).scalar()
            log("Bang 'predictions_readmission_risk' (tang AI/ML)", pred_count > 0, f"{pred_count} dong")
        except Exception:
            log("Bang 'predictions_readmission_risk' (tang AI/ML)", False, "chua chay train_readmission_model.py")

except ImportError:
    log("Thu vien sqlalchemy/psycopg2", False, "chua cai - chay: pip install sqlalchemy psycopg2-binary")
except Exception as e:
    log("Ket noi Postgres", False, str(e))


# ----------------------------------------------------------------------
# TONG KET
# ----------------------------------------------------------------------
section("TONG KET")

total = len(RESULTS)
passed = sum(1 for _, ok, _ in RESULTS if ok)
failed = [name for name, ok, _ in RESULTS if not ok]

print(f"\nKet qua: {passed}/{total} kiem tra PASS\n")

if failed:
    print("Cac muc dang LOI, can kiem tra lai:")
    for name in failed:
        print(f"  - {name}")
else:
    print("Tat ca deu OK! He thong hoat dong binh thuong.")