from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lag, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.sql.window import Window
from ultralytics import YOLO
import pandas as pd
import os
import time
import datetime
import json
import logging
import cv2  # Ajout explicite de cv2

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def preprocess_videos():
    logger.info("Chargement manuel de /opt/airflow/scripts/yolo11n.pt")
    yolo_model = YOLO("yolo11n.pt")  # Chemin dans Airflow
    class_names = yolo_model.names

    # Initialisation Spark
    spark = SparkSession.builder \
        .appName("TrafficDataProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-2.8.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/opt/spark/jars/kafka-clients-2.8.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar:/opt/bitnami/spark/jars/kafka-clients-2.8.0.jar") \
        .config("spark.submit.deployMode", "client") \
        .getOrCreate()
    try:
        spark._jvm.org.apache.spark.sql.kafka010.KafkaSourceProvider
        logger.info("KafkaSourceProvider chargé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement de KafkaSourceProvider : {str(e)}")
        raise

# Test KafkaConfigUpdater
    try:
        spark._jvm.org.apache.spark.sql.kafka010.KafkaConfigUpdater
        logger.info("KafkaConfigUpdater chargé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement de KafkaConfigUpdater : {str(e)}")
        raise

    # Schéma Kafka (métadonnées des vidéos)
    kafka_schema = StructType([
        StructField("video_path", StringType(), True),
        StructField("period", StringType(), True),
        StructField("camera", StringType(), True),
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True)
    ])

    # Schéma des détections
    detection_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("vehicles", IntegerType(), True),
        StructField("frame_id", IntegerType(), True),
        StructField("elapsed_time_sec", FloatType(), True),
        StructField("period", StringType(), True),
        StructField("camera", StringType(), True),
        StructField("date", StringType(), True),
        StructField("detections", ArrayType(StructType([
            StructField("x_min", FloatType(), True),
            StructField("y_min", FloatType(), True),
            StructField("x_max", FloatType(), True),
            StructField("y_max", FloatType(), True),
            StructField("track_id", IntegerType(), True),
            StructField("conf", FloatType(), True),
            StructField("class_name", StringType(), True)
        ])), True)
    ])

    def process_video_to_df(video_path, period, camera, date):
        """Traite une vidéo et renvoie un DataFrame pandas avec les détections"""
        logger.info(f"Début du traitement de {video_path}")
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logger.error(f"Impossible d'ouvrir {video_path}")
            return None

        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        frame_count = 0
        all_frames = []
        start_time = time.time()

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            elapsed_time = time.time() - start_time
            timestamp = datetime.datetime.now().isoformat()

            frame_res = cv2.resize(frame, (640, 640))
            results = yolo_model.track(frame_res, persist=True)

            detection = results[0].boxes.data
            if detection.numel() == 0:
                frame_count += 1
                continue

            frame_data = detection.cpu().numpy()
            frame_df = pd.DataFrame(
                frame_data,
                columns=["x_min", "y_min", "x_max", "y_max", "track_id", "conf", "class"]
            )
            frame_df["frame_id"] = frame_count
            frame_df["elapsed_time_sec"] = elapsed_time
            frame_df["class_name"] = frame_df["class"].apply(lambda x: class_names[int(x)])

            num_vehicles = frame_df["track_id"].nunique()
            all_frames.append({
                "timestamp": timestamp,
                "vehicles": int(num_vehicles),
                "frame_id": frame_count,
                "elapsed_time_sec": float(elapsed_time),
                "period": period,
                "camera": camera,
                "date": date,
                "detections": frame_df.to_dict(orient="records")
            })

            frame_count += 1

        cap.release()
        logger.info(f"Traitement terminé pour {video_path} - {frame_count} frames")
        return pd.DataFrame(all_frames)

    # Lecture batch depuis Kafka
    data_batch = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "camera1_data,camera2_data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse les métadonnées
    parsed_data = data_batch.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select(
        "data.video_path", "data.period", "data.camera", "data.date"
    ).filter(col("data.status") == "ready_for_processing").distinct()

    # Traitement des vidéos
    output_dir = "/opt/airflow/output/preprocessed"  # Chemin dans Airflow
    os.makedirs(output_dir, exist_ok=True)

    for row in parsed_data.collect():
        video_path = row["video_path"]
        period = row["period"]
        camera = row["camera"]
        date = row["date"]
        logger.info(f"Traitement de {video_path}")

        # Traitement vidéo avec YOLO
        pandas_df = process_video_to_df(video_path, period, camera, date)
        if pandas_df is None or pandas_df.empty:
            logger.warning(f"Aucune donnée pour {video_path}")
            continue

        # Convertir en Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df, schema=detection_schema)

        # Exploser les détections pour analyse
        detection_df = spark_df.select(
            "timestamp", "frame_id", "vehicles", "elapsed_time_sec", "period", "camera", "date",
            explode(col("detections")).alias("detection")
        ).select(
            "timestamp", "frame_id", "vehicles", "elapsed_time_sec", "period", "camera", "date",
            col("detection.x_min").alias("x_min"),
            col("detection.y_min").alias("y_min"),
            col("detection.x_max").alias("x_max"),
            col("detection.y_max").alias("y_max"),
            col("detection.track_id").alias("track_id"),
            col("detection.conf").alias("confiance"),
            col("detection.class_name").alias("class_name")
        )

        # Calcul des centroïdes et vitesses
        detection_df = detection_df.withColumn("x_centre", (col("x_max") + col("x_min")) / 2) \
                                   .withColumn("y_centre", (col("y_max") + col("y_min")) / 2)

        window_spec = Window.partitionBy("track_id").orderBy("frame_id")
        detection_df = detection_df.withColumn("x_delta", col("x_centre") - lag("x_centre", 1, 0).over(window_spec)) \
                                   .withColumn("y_delta", col("y_centre") - lag("y_centre", 1, 0).over(window_spec)) \
                                   .withColumn("time_delta", col("elapsed_time_sec") - lag("elapsed_time_sec", 1, 0).over(window_spec)) \
                                   .withColumn("x_velocity", when(col("time_delta") > 0, col("x_delta") / col("time_delta")).otherwise(0)) \
                                   .withColumn("y_velocity", when(col("time_delta") > 0, col("y_delta") / col("time_delta")).otherwise(0)) \
                                   .withColumn("speed", (col("x_velocity")**2 + col("y_velocity")**2)**0.5) \
                                   .drop("x_centre", "y_centre", "time_delta")

        # Sauvegarde des résultats
        video_name = os.path.basename(video_path).replace(".mp4", "")
        output_path = f"{output_dir}/{date}_{camera}_{period}_detections"
        detection_df.write.csv(output_path, mode="overwrite")
        logger.info(f"Résultats sauvegardés dans {output_path}")

    spark.stop()

if __name__ == "__main__":
    preprocess_videos()