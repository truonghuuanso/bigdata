import pandas as pd

DATA_PATH = "data/raw/diabetic_data.csv"
MAPPING_PATH = "data/raw/IDS_mapping.csv"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

# 1. Đọc dữ liệu
df = pd.read_csv(DATA_PATH)

print("=" * 60)
print(f"Số dòng: {df.shape[0]:,} | Số cột: {df.shape[1]}")
print("=" * 60)

print("\n--- Kiểu dữ liệu từng cột ---")
print(df.dtypes)

print("\n--- 5 dòng mẫu ---")
print(df.head())

print("\n--- Số giá trị '?' (missing) theo từng cột (top 15) ---")
missing_counts = (df == "?").sum().sort_values(ascending=False)
print(missing_counts[missing_counts > 0].head(15))

print("\n--- Phân bố cột 'readmitted' (biến mục tiêu cho AI/ML) ---")
print(df["readmitted"].value_counts())

key_cols = [
    "encounter_id", "patient_nbr", "admission_type_id",
    "discharge_disposition_id", "admission_source_id",
    "time_in_hospital", "diag_1", "diag_2", "diag_3", "readmitted",
]
print("\n--- Các cột then chốt cho Fact_Admission / Dim_* ---")
print(df[key_cols].head(10))

print("\n--- Nội dung IDS_mapping.csv (giải mã các cột _id) ---")
mapping = pd.read_csv(MAPPING_PATH)
print(mapping.head(30))