"""
Tang 6 - Airflow DAG dong goi pipeline batch:
  Tang 3 (xu ly PySpark, tao star schema Parquet) -> Tang 5 (nap vao Postgres)

Chay bang cach dieu khien container spark-master co san qua Docker socket
(dung thu vien docker-py), giong het lenh spark-submit ban da go tay truoc do.
"""

from datetime import datetime

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# Ham dung chung: goi docker exec vao container spark-master de chay
# 1 file spark-submit, tuong tu lenh ban da go tay tren terminal
# ----------------------------------------------------------------------
def run_spark_submit(script_path: str, extra_packages: str = ""):
    client = docker.from_env()
    container = client.containers.get("spark-master")

    base_packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    packages = base_packages + (f",{extra_packages}" if extra_packages else "")

    cmd = (
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--conf spark.jars.ivy=/tmp/.ivy2 "
        f"--packages {packages} "
        f"{script_path}"
    )

    print(f"Dang chay lenh: {cmd}")
    exit_code, output = container.exec_run(cmd, stream=False, demux=False)
    log_text = output.decode("utf-8", errors="replace") if isinstance(output, bytes) else str(output)
    print(log_text)

    if exit_code != 0:
        raise RuntimeError(
            f"Spark job that bai (exit code {exit_code}) khi chay {script_path}.\n"
            f"Xem log ben tren de biet chi tiet loi."
        )
    print(f"Hoan tat: {script_path}")


def task_process_layer3():
    run_spark_submit("/opt/spark-jobs/process_hospital_dw.py")


def task_load_layer5():
    run_spark_submit(
        "/opt/spark-jobs/load_to_postgres.py",
        extra_packages="org.postgresql:postgresql:42.7.3",
    )


# ----------------------------------------------------------------------
# Ham dung chung: goi docker exec vao container dbt de chay lenh dbt,
# giong het lenh ban da go tay tren terminal
# ----------------------------------------------------------------------
def run_dbt_command(dbt_subcommand: str):
    client = docker.from_env()
    container = client.containers.get("dbt")

    cmd = f"dbt {dbt_subcommand} --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt"

    print(f"Dang chay lenh: {cmd}")
    exit_code, output = container.exec_run(cmd, stream=False, demux=False)
    log_text = output.decode("utf-8", errors="replace") if isinstance(output, bytes) else str(output)
    print(log_text)

    if exit_code != 0:
        raise RuntimeError(
            f"Lenh 'dbt {dbt_subcommand}' that bai (exit code {exit_code}).\n"
            f"Xem log ben tren de biet chi tiet loi."
        )
    print(f"Hoan tat: dbt {dbt_subcommand}")


def task_dbt_run():
    run_dbt_command("run")


def task_dbt_test():
    run_dbt_command("test")


# ----------------------------------------------------------------------
# Dinh nghia DAG
# ----------------------------------------------------------------------
with DAG(
    dag_id="hospital_dw_batch_pipeline",
    description="Do an 5 - Pipeline batch: xu ly PySpark (tang 3) -> nap Postgres (tang 5) -> dbt (tang 7)",
    start_date=datetime(2026, 1, 1),
    schedule=None,       # chi chay khi bam nut Trigger thu cong (demo/do an)
    catchup=False,
    tags=["doan5", "hospital-dw"],
) as dag:

    process_layer3 = PythonOperator(
        task_id="process_pyspark_layer3",
        python_callable=task_process_layer3,
    )

    load_layer5 = PythonOperator(
        task_id="load_postgres_layer5",
        python_callable=task_load_layer5,
    )

    dbt_run_layer7 = PythonOperator(
        task_id="dbt_run_layer7",
        python_callable=task_dbt_run,
    )

    dbt_test_layer7 = PythonOperator(
        task_id="dbt_test_layer7",
        python_callable=task_dbt_test,
    )

    process_layer3 >> load_layer5 >> dbt_run_layer7 >> dbt_test_layer7