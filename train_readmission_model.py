"""
Tang 8 (AI/ML) - Du doan tai nhap vien 30 ngay bang scikit-learn
(thay the cho Amazon SageMaker trong ban open-source)

Ban hoan chinh, viet lai tu dau - co xu ly ca 2 van de:
  1. Du lieu mat can bang (~11% tai nhap) -> dung class_weight='balanced'
     de model khong "lam bieng" doan toan lop da so.
  2. Xac suất dau ra bi lech cao do class_weight -> dung
     CalibratedClassifierCV de hieu chinh lai xac suat cho sat thuc te,
     van giu duoc kha nang phat hien ca tai nhap tot.

Cai thu vien can thiet (chay 1 lan):
    pip install pandas scikit-learn sqlalchemy psycopg2-binary joblib
"""

import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.base import clone
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix
import joblib


PG_CONN = "postgresql+psycopg2://dwuser:dwpassword@localhost:5432/hospital_dw"
engine = create_engine(PG_CONN)

query = """
    SELECT
        f.encounter_id,
        f.los_days,
        f.num_lab_procedures,
        f.num_procedures,
        f.num_medications,
        f.number_diagnoses,
        f.prior_visits_total,
        f.readmit_risk_flag,
        f.readmitted_30d,
        dp.race,
        dp.gender,
        dp.age,
        dd.medical_specialty,
        di.diag_1_group,
        di.admission_type_desc
    FROM fact_admission f
    JOIN dim_patient dp     ON f.patient_key = dp.patient_key
    JOIN dim_department dd  ON f.department_key = dd.department_key
    JOIN dim_diagnosis di   ON f.diagnosis_key = di.diagnosis_key
"""

print("Dang doc du lieu tu Postgres...")
df = pd.read_sql(query, engine)
base_rate = df["readmitted_30d"].mean()
print(f"Da doc {len(df)} dong.")
print(f"Ty le tai nhap 30 ngay trong du lieu (base rate): {base_rate * 100:.2f}%")


NUMERIC_FEATURES = [
    "los_days", "num_lab_procedures", "num_procedures",
    "num_medications", "number_diagnoses", "prior_visits_total",
]
CATEGORICAL_FEATURES = [
    "readmit_risk_flag", "race", "gender", "age",
    "medical_specialty", "diag_1_group", "admission_type_desc",
]

X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
y = df["readmitted_30d"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train: {len(X_train)} dong | Test: {len(X_test)} dong")


preprocessor = ColumnTransformer(transformers=[
    ("num", StandardScaler(), NUMERIC_FEATURES),
    ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
])

base_pipeline = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("classifier", RandomForestClassifier(
        n_estimators=200, max_depth=10,
        class_weight="balanced", random_state=42, n_jobs=-1,
    )),
])


print("Dang train model (ban goc, de xem feature importance)...")
model_raw = clone(base_pipeline)
model_raw.fit(X_train, y_train)

print("Dang train model (ban hieu chinh xac suat - CalibratedClassifierCV)...")
model_calibrated = CalibratedClassifierCV(
    estimator=clone(base_pipeline), method="sigmoid", cv=5
)
model_calibrated.fit(X_train, y_train)


from sklearn.metrics import precision_recall_curve

y_proba = model_calibrated.predict_proba(X_test)[:, 1]

precisions, recalls, thresholds = precision_recall_curve(y_test, y_proba)
f1_scores = 2 * precisions * recalls / (precisions + recalls + 1e-9)
best_idx = f1_scores[:-1].argmax() 
best_threshold = thresholds[best_idx]

print(f"\nNguong phan loai toi uu tim duoc: {best_threshold:.4f} (thay vi mac dinh 0.5)")

y_pred = (y_proba >= best_threshold).astype(int)


print("KET QUA DANH GIA MODEL (sau khi hieu chinh xac suat)")
print("=" * 60)
print(classification_report(y_test, y_pred, target_names=["Khong tai nhap", "Tai nhap 30 ngay"]))
print(f"ROC-AUC: {roc_auc_score(y_test, y_proba):.4f}")
print("\nConfusion Matrix (hang=thuc te, cot=du doan):")
print(confusion_matrix(y_test, y_pred))

print(f"\nXac suat trung binh model du doan tren tap test: {y_proba.mean() * 100:.2f}%")
print(f"So voi ty le that trong du lieu goc: {base_rate * 100:.2f}%")
print("(2 con so nay cang gan nhau, model cang duoc hieu chinh tot)")


feature_names = model_raw.named_steps["preprocess"].get_feature_names_out()
importances = model_raw.named_steps["classifier"].feature_importances_
top_features = (
    pd.DataFrame({"feature": feature_names, "importance": importances})
    .sort_values("importance", ascending=False)
    .head(10)
)
print("\nTop 10 dac trung quan trong nhat:")
print(top_features.to_string(index=False))


joblib.dump(model_calibrated, "readmission_model_calibrated.joblib")
joblib.dump(model_raw, "readmission_model_raw.joblib")
print("\nDa luu model vao readmission_model_calibrated.joblib va readmission_model_raw.joblib")


df["predicted_readmit_probability"] = model_calibrated.predict_proba(X)[:, 1]
df["predicted_readmit_flag"] = (df["predicted_readmit_probability"] >= best_threshold).astype(int)

predictions_out = df[[
    "encounter_id", "readmitted_30d",
    "predicted_readmit_flag", "predicted_readmit_probability",
]]
predictions_out.to_sql(
    "predictions_readmission_risk", engine, if_exists="replace", index=False
)
print(
    f"Da ghi {len(predictions_out)} dong du doan vao bang "
    f"'predictions_readmission_risk' trong Postgres."
)
print(f"Xac suat trung binh trong bang du doan: {df['predicted_readmit_probability'].mean() * 100:.2f}%")