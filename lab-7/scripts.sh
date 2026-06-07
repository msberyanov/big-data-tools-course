#!/bin/bash

# Копирование скриптов.
docker cp scripts/kafka_loader.py airflow-webserver:/opt/airflow/scripts
docker cp scripts/spark_processor.py airflow-webserver:/opt/airflow/scripts
docker cp dags/spark_kafka_pipeline.py airflow-webserver:/opt/airflow/dags
docker cp config/pipeline.cfg airflow-webserver:/opt/airflow/config

docker exec -it airflow-webserver airflow dags trigger spark_kafka_pipeline
docker exec -it airflow-webserver airflow dags list-import-errors
