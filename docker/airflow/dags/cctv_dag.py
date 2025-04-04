from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

dag = DAG(
    "cctv_batch_ingestion",
    start_date=datetime(2025, 3, 31),
    schedule_interval="0 23 * * *",  # 23h00 chaque jour
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)

def ingest_videos(periods, execution_date):
    base_dir = "/opt/airflow/videos/cameras"
    date_str = execution_date.strftime("%d-%m-%Y")  # Date actuelle
    for camera in os.listdir(base_dir):
        camera_dir = os.path.join(base_dir, camera)
        if not os.path.isdir(camera_dir):
            continue
        topic = f"{camera}_data"
        date_dir = f"Video_{date_str}"
        video_dir = os.path.join(camera_dir, date_dir)
        if not os.path.exists(video_dir):
            print(f"Dossier {video_dir} non trouvé")
            continue
        for period in periods:
            video_file = f"vid_{period}.mp4"
            video_path = os.path.join(video_dir, video_file)
            if os.path.exists(video_path):
                cmd = f"python /opt/airflow/scripts/ingestion.py {video_path} {topic} {period} {date_str}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
                print(f"Ingestion de {video_path}: {result.stdout}")
            else:
                print(f"Fichier {video_path} non trouvé")

# Périodes spécifiques à traiter
periods = ["09:00_10:00", "10:00_11:00"]

task1 = PythonOperator(
    task_id="ingest_night",
    python_callable=lambda **context: ingest_videos(periods, context["execution_date"]),
    provide_context=True,
    dag=dag
)