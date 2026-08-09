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
