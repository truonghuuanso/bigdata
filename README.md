
# Đồ án 5 — Bệnh viện: Kho dữ liệu vận hành & phân tích tái nhập viện

Kiến trúc Big Data / Data Warehouse chạy 100% local bằng Docker Compose
(phương án open-source thay AWS): MinIO → Kafka → Spark → Postgres → dbt → Airflow → Metabase.

**Phạm vi README này: Tầng 1 → Tầng 7 (đã hoàn thành và kiểm chứng).**
Tầng 8 ( RAG tra cứu phác đồ) **không nằm trong phạm vi này**, nhưng phần AI/ML dự đoán tái nhập đã làm có thể xem

RAG tra cứu phác đồ cũng đã xong nhưng chưa hoàn thiện, model từ phát đồ chưa nói chính xác tự bịa nhiều
---

## 1. Kiến trúc tổng thể

Ánh xạ theo mô hình Bronze → Silver → Gold:

```
CSV (Kaggle)
   │
   ▼
[Tầng 1] MinIO bucket "hospital-raw"           ← BRONZE (dữ liệu thô)
   │
   ▼
[Tầng 2] Kafka "hospital_admissions_stream"    ← streaming song song, độc lập
   │       (Spark Structured Streaming đọc + cảnh báo realtime)
   │
[Tầng 3] Spark xử lý (làm sạch, tính LOS,
          nhóm ICD-9, feature nguy cơ)
   │
   ▼
[Tầng 4] Parquet partition theo admission_date
          trong MinIO bucket "hospital-curated"
   │
   ▼
[Tầng 5] Nạp vào Postgres "hospital_dw"        ← SILVER (star schema sạch)
          (1 fact + 4 dimension)
   │
   ▼
[Tầng 7] dbt: staging views + 2 mart +          ← GOLD (sẵn sàng cho BI)
          test toàn vẹn + snapshot lịch sử
   │
   ▼
[Tầng 8 - KHÔNG THUỘC PHẠM VI README NÀY]
Metabase dashboard (đã setup, 3 chart) + AI/ML (nhóm khác làm)

[Tầng 6] Airflow điều phối toàn bộ chuỗi:
Spark job (tầng 3) → load Postgres (tầng 5) → dbt run → dbt test
```

---

## 2. Yêu cầu trước khi chạy

- Docker Desktop đã cài, đang chạy (Windows cần WSL2 bật).
- Máy còn trống tối thiểu ~6-8GB RAM cho Docker.
- Python 3.10+ với venv (dùng cho các script chạy ngoài Docker: `kafka_producer.py`, kiểm tra dữ liệu...).
- Tài khoản Kaggle (miễn phí) để tải dataset.

## 3. Cấu trúc thư mục

```
doan5-hospital-dw/
├── docker-compose.yml
├── .gitignore
├── data/
│   └── raw/
│       ├── diabetic_data.csv       # tai tu Kaggle, KHONG commit len git
│       └── IDS_mapping.csv
├── spark-jobs/
│   ├── process_hospital_dw.py      # Tang 3: xu ly PySpark
│   ├── load_to_postgres.py         # Tang 5: nap vao Postgres
│   └── stream_vitals_processing.py # Tang 2: Spark Structured Streaming
├── airflow/
│   └── dags/
│       └── hospital_dw_pipeline.py # Tang 6: DAG dieu phoi
├── dbt/                             # Tang 7
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                # 5 view stg_* + sources.yml + schema.yml (test)
│   │   └── marts/                  # 2 mart: theo khoa, theo chan doan
│   └── snapshots/
│       └── snapshot_dim_patient.sql
├── postgres-init/
│   └── 01-create-airflow-db.sql    # tu tao database airflow_meta
├── kafka_producer.py                # Tang 2: gia lap thiet bi IoT
├── check.py                         # script kiem tra suc khoe toan he thong
└── README.md
```

---

## 4. Setup lần đầu (làm theo đúng thứ tự)

### Bước 1 — Tải dataset
Tải **"Diabetes 130-US hospitals for years 1999-2008"** từ Kaggle hoặc UCI ML Repository.
Đặt 2 file vào `data/raw/diabetic_data.csv` và `data/raw/IDS_mapping.csv`.

### Bước 2 — Khởi động hạ tầng
```bash
docker compose up -d
```
Lần đầu mất vài phút để tải hết image. Kiểm tra tất cả container đã "Up":
```bash
docker ps
```
Danh sách mong đợi: `minio`, `kafka`, `kafka-ui`, `spark-master`, `spark-worker`,
`postgres`, `airflow`, `dbt`, `adminer`, `metabase` (và `ollama` nếu có, không bắt buộc cho tầng 1-7).

### Bước 3 — Tạo database `airflow_meta`
Airflow **bắt buộc** phải dùng database riêng, tách khỏi `hospital_dw` (data warehouse thật) —
nếu dùng chung, các bảng nội bộ của Airflow (`dag`, `job`, `log`...) sẽ trộn lẫn vào chung schema `public`
với 5 bảng dữ liệu thật, rất khó phân biệt.

Vào Adminer (http://localhost:8083), đăng nhập vào database hệ thống `postgres`
(Server: `postgres`, User: `dwuser`, Password: `dwpassword`, Database: `postgres`), chạy:
```sql
CREATE DATABASE airflow_meta;
```
Sau đó:
```bash
docker compose restart airflow
```

### Bước 4 — Tạo bucket MinIO
Mở http://localhost:9001 (user/pass: `minioadmin`/`minioadmin`), tạo 2 bucket:
- `hospital-raw`
- `hospital-curated`

Upload 2 file CSV ở Bước 1 vào bucket `hospital-raw`.

### Bước 5 — Chạy pipeline qua Airflow
Mở http://localhost:8081 (user/pass: `admin`/`admin`), tìm DAG `hospital_dw_batch_pipeline`,
bật toggle, bấm **Trigger DAG**.

DAG chạy tuần tự 4 bước: `process_pyspark_layer3` → `load_postgres_layer5` →
`dbt_run_layer7` → `dbt_test_layer7`. Theo dõi tab **Graph**, tất cả phải xanh (success).

Nếu `dbt_test_layer7` đỏ, xem log — có thể có bug dữ liệu cần sửa (xem mục 6 bên dưới,
đã từng gặp và biết cách sửa).

### Bước 6 — Chạy snapshot dbt (không nằm trong DAG, chạy tay 1 lần)
```bash
docker exec -it dbt dbt snapshot --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt
```

### Bước 7 — Streaming (tầng 2, chạy riêng, không tự động qua Airflow)
Mở 2 terminal song song:
```bash
# Terminal 1 - container Spark, lắng nghe Kafka + cảnh báo realtime
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-jobs/stream_vitals_processing.py
```
```bash
# Terminal 2 - máy Windows (venv, pip install kafka-python pandas)
python kafka_producer.py
```

### Bước 8 — Kết nối Metabase (đã làm sẵn, không cần lặp lại)
http://localhost:3000 → PostgreSQL: host `postgres`, port `5432`, database `hospital_dw`,
user `dwuser`, password `dwpassword`. Dashboard `Hospital Operations Dashboard` đã có 3 chart:
tỷ lệ tái nhập theo khoa, theo chẩn đoán, LOS trung bình theo khoa.

---

## 5. Giải thích chi tiết từng tầng

### Tầng 1 — Batch Ingestion
CSV → upload thủ công vào MinIO bucket `hospital-raw` (không tự động hoá, chấp nhận được
vì chỉ 2 file tĩnh, không phải nguồn dữ liệu liên tục).

### Tầng 2 — Streaming Ingestion
- `kafka_producer.py`: đọc `diabetic_data.csv`, mô phỏng thiết bị IoT gửi sinh hiệu
  (nhịp tim, SpO2, huyết áp) vào Kafka topic `hospital_admissions_stream` mỗi giây,
  ~5% ca được gán giá trị bất thường có chủ đích để test cảnh báo.
- `spark-jobs/stream_vitals_processing.py`: Spark Structured Streaming đọc trực tiếp
  từ Kafka, gắn cờ cảnh báo (nhịp tim >120 hoặc <50, SpO2 <90), in cảnh báo ra console
  theo thời gian thực, đồng thời ghi toàn bộ dữ liệu ra `hospital-curated/streaming_vitals`.
- **Độc lập với tầng 3** — đây là 2 nhánh song song (batch và streaming), không phải
  tầng 2 chảy vào tầng 3.

### Tầng 3 — Xử lý PySpark
`spark-jobs/process_hospital_dw.py`: đọc CSV từ MinIO, làm sạch, tạo star schema
(1 Fact_Admission + 4 Dimension), ghi Parquet có partition theo `admission_year`/`admission_month`.

Các bước xử lý chính: bỏ cột `weight` (thiếu 97%), điền "Unknown" cho cột thiếu,
loại bỏ ca tử vong/hospice, mô phỏng `admission_date` (dataset gốc không có ngày thật),
nhóm mã ICD-9 thành nhóm bệnh lớn, tạo đặc trưng `prior_visits_total`/`readmit_risk_flag`.

### Tầng 4 — Lưu trữ Parquet
Gộp chung trong `process_hospital_dw.py` (bước ghi cuối) — `fact_admission` được
partition theo năm/tháng của `admission_date`, 4 dimension ghi phẳng (không cần partition
vì bảng nhỏ).

### Tầng 5 — Data Warehouse (star schema)
`spark-jobs/load_to_postgres.py`: đọc 5 bảng Parquet từ MinIO, ghi vào Postgres
database `hospital_dw`, schema `public`. Dùng `.option("truncate", "true")` thay vì
overwrite mặc định (xem mục 6 — lý do quan trọng).

### Tầng 6 — Orchestration (Airflow)
`airflow/dags/hospital_dw_pipeline.py`: DAG 4 task, gọi `docker exec` vào container
`spark-master` và `dbt` qua thư viện `docker-py` (Airflow "bấm nút" giùm, không có logic
xử lý riêng). Task sau chỉ chạy khi task trước `success`.

### Tầng 7 — Transformation (dbt)
- **Staging** (`dbt/models/staging/`): 5 view `stg_*`, chỉ là `SELECT * FROM source`,
  không biến đổi gì — lớp trung gian chuẩn theo convention dbt.
- **Marts** (`dbt/models/marts/`): 2 bảng tổng hợp sẵn — `mart_readmission_by_department`
  (73 dòng) và `mart_readmission_by_diagnosis` (12 dòng) — tỷ lệ tái nhập, LOS trung bình.
- **Test toàn vẹn** (`dbt/models/staging/schema.yml`): 15 test — `unique`/`not_null` cho
  mọi khoá chính, `relationships` cho khoá ngoại fact↔dim, `accepted_values` cho
  `readmitted_30d`. Chạy `dbt test`, tất cả phải PASS.
- **Snapshot** (`dbt/snapshots/snapshot_dim_patient.sql`): tự động lưu lịch sử thay đổi
  của `dim_patient` (chiến lược `check`, theo dõi cột race/gender/age).

---

## 6. Các lỗi/quyết định kỹ thuật quan trọng đã xử lý

Ghi lại để hiểu *tại sao* code viết như hiện tại, tránh sửa nhầm về lại lỗi cũ.

1. **Image `bitnami/kafka` và `bitnami/spark` không còn dùng được miễn phí**
   (Bitnami đổi chính sách 8/2025) → đổi sang image chính thức `apache/kafka:3.7.0`
   và `apache/spark:3.5.1`.

2. **File `IDS_mapping.csv` là 3 bảng tra cứu bị dồn chung vào 2 cột trong 1 file** —
   đọc thẳng bằng `spark.read.csv` khiến 1 mã số (vd id=1) trùng giữa 3 bảng, gây
   **join fan-out 3x** (`fact_admission` bị nhân từ ~100k lên 300k dòng). Đã sửa bằng
   cách tự parse file theo block (dò dòng "header" lặp giữa file) trong
   `process_hospital_dw.py`, bước 7.

3. **`dim_patient` dedup chỉ theo `patient_nbr` nhưng join lại với 4 cột**
   (`patient_nbr`+`race`+`gender`+`age`) — 1 bệnh nhân khám nhiều lần ở độ tuổi khác
   nhau sẽ không khớp được → ~2,909 dòng `patient_key` bị NULL. Đã sửa: join
   `fact` với `dim_patient` **chỉ theo `patient_nbr`**.

4. **Spark JDBC `mode("overwrite")` mặc định = DROP + CREATE TABLE** — sau khi dbt
   tạo view `stg_fact_admission` phụ thuộc vào bảng `fact_admission`, Postgres từ chối
   cho Spark xoá bảng (`cannot drop table ... other objects depend on it`). Đã sửa:
   dùng `.option("truncate", "true")` — xoá sạch dữ liệu nhưng **giữ nguyên bảng**,
   không phá vỡ view.

5. **Airflow dùng chung database với data warehouse** — các bảng nội bộ Airflow
   (`dag`, `job`, `ab_user`...) trộn lẫn với 5 bảng thật trong `hospital_dw`. Đã sửa:
   tách riêng database `airflow_meta` (xem Bước 3 ở mục Setup).

6. **Kafka cần 2 listener** (`INTERNAL` cho container-to-container như Kafka UI,
   `EXTERNAL` cho Windows-to-container như `kafka_producer.py`) — 1 listener duy nhất
   quảng bá `localhost` sẽ làm container khác không kết nối được.

---

## 7. Các URL và tài khoản quan trọng

| Dịch vụ | URL | Tài khoản |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8082 | - |
| Spark Master UI | http://localhost:8080 | - |
| Airflow | http://localhost:8081 | admin / admin |
| Adminer (xem Postgres) | http://localhost:8083 | dwuser / dwpassword (DB: hospital_dw) |
| Metabase | http://localhost:3000 | tự tạo lúc setup |

## 8. Kiểm tra sức khoẻ hệ thống

Sau khi setup hoặc mỗi lần chỉnh sửa code, chạy script kiểm tra tổng thể:
```bash
pip install docker boto3 sqlalchemy psycopg2-binary
python check.py
```
Script kiểm tra: container Docker đang chạy, bucket/file MinIO, 9 bảng Postgres
(5 gốc + staging + 2 mart + snapshot) đủ số dòng mong đợi, và logic dữ liệu
(không có `patient_key` NULL, tỷ lệ `readmitted_30d` hợp lý 5-20%).













### Đồ án 5 — Bệnh viện — Kho dữ liệu vận hành & phân tích tái nhập viện

**Cloud bắt buộc:** `AWS`

- **Bối cảnh nghiệp vụ:** Data warehouse cho bệnh viện để phân tích luồng bệnh nhân, thời gian nằm viện và nguy cơ tái nhập viện.
- **Nguồn dữ liệu & dataset gợi ý:** Diabetes 130-US hospitals / Hospital readmissions (Kaggle); dữ liệu khoa/phòng mô phỏng.
- **📦 Kích thước dataset (xấp xỉ):** Diabetes 130-US hospitals ~19 MB (~101k lượt khám, 50 cột). Nhỏ — bù bằng luồng streaming mô phỏng.
- **Ingestion — batch:** Hồ sơ khám/nhập-xuất viện vào Amazon S3; Glue catalog + Glue Job nạp cleansed zone (ẩn danh dữ liệu nhạy cảm).
- **Ingestion — streaming (Kafka):** Sự kiện theo dõi sinh hiệu/hàng chờ cấp cứu qua Amazon MSK (Kafka); producer mô phỏng thiết bị.
- **Xử lý PySpark:** EMR PySpark: chuẩn hoá mã chẩn đoán, tính LOS (length of stay), tạo đặc trưng nguy cơ tái nhập.
- **Lưu trữ (Data lake / Parquet):** S3 Parquet partition theo admission_date; kiểm soát truy cập bằng Lake Formation.
- **Mô hình Data Warehouse (star schema):** Fact_Admission (admission_id, patient_key, department_key, date_key, los_days, readmitted). Dimension: Dim_Patient, Dim_Department, Dim_Diagnosis, Dim_Date. Nạp vào Amazon Redshift.
- **Orchestration (Airflow):** MWAA: DAG ingest → Spark job → load Redshift → dbt → dashboard chất lượng vận hành.
- **Transformation (dbt / ELT):** dbt-redshift: marts theo khoa/chẩn đoán, test toàn vẹn, snapshot lịch sử bệnh nhân.
- **Lớp Analytics cuối cùng (BI + AI/ML):** QuickSight: công suất giường, thời gian chờ, tỷ lệ tái nhập theo khoa. AI: dự đoán tái nhập viện 30 ngày bằng SageMaker; trợ lý tra cứu phác đồ bằng RAG trên Amazon Bedrock + OpenSearch (vector).
- **Thử thách nâng cao (điểm cộng):** Cảnh báo quá tải cấp cứu realtime; phân tích công bằng mô hình (bias) theo nhóm bệnh nhân.
- **🟢 Phương án open-source (chạy local, không cần cloud):** Toàn bộ đồ án chạy được 100% trên máy cá nhân bằng Docker Compose, không cần tài khoản cloud. Kiến trúc và code PySpark/dbt/Airflow giữ nguyên — chỉ thay điểm kết nối. Ánh xạ dịch vụ AWS → open-source: Amazon S3 ⇒ MinIO; Amazon MSK (Kafka) ⇒ Apache Kafka (Docker); AWS Glue / EMR (Spark) ⇒ Apache Spark tự host; Amazon Redshift / Athena ⇒ DuckDB hoặc PostgreSQL; Amazon MWAA ⇒ Apache Airflow tự host; dbt-redshift / dbt-athena ⇒ dbt-duckdb / dbt-postgres; Amazon QuickSight ⇒ Metabase / Superset; Amazon Bedrock + OpenSearch ⇒ Ollama + ChromaDB/Qdrant; Amazon SageMaker / Redshift ML ⇒ scikit-learn / XGBoost / Prophet. Data lake & lưu trữ: MinIO (S3 API) làm data lake; Ghi Parquet + partition lên MinIO/đĩa; Nessie/Iceberg nếu cần quản lý bảng. Streaming: Apache Kafka + Kafka UI (Docker) — giữ nguyên code producer/consumer; Spark Structured Streaming (bản open-source) đọc trực tiếp từ Kafka. Kho dữ liệu & biến đổi: DuckDB (nhúng, cực nhanh) hoặc PostgreSQL cho data warehouse; dbt-core + dbt-duckdb (hoặc dbt-postgres) — cùng model, chỉ đổi adapter. Điều phối: Apache Airflow tự host (Docker) — chính là bản gốc của MWAA/Composer. Lớp analytics: Metabase (dễ dùng) hoặc Apache Superset (nhiều biểu đồ) làm dashboard cho dashboard; scikit-learn / XGBoost trong notebook thay cho SageMaker/BigQuery ML (dự đoán tái nhập viện; RAG tra cứu phác đồ); RAG bằng Ollama (LLM local) + ChromaDB/Qdrant (vector) + LlamaIndex cho RAG.
