# Отчёт по лабораторной работе 7.

---

## Задание.

**Цель**: Реализовать ETL-пайплайн с использованием Apache Airflow для оркестрации, Apache Kafka для потоковой передачи данных и Apache Spark для обработки данных, с хранением в HDFS.

### Компоненты решения

#### 1. Оркестрация через Apache Airflow.

Airflow управляет пайплайном через DAG [`spark_kafka_pipeline.py`](./dags/spark_kafka_pipeline.py), который содержит следующие задачи:
- `check_hdfs_input` — ожидание появления входных данных в HDFS.
- `load_from_kafka` — загрузка данных из Kafka в HDFS.
- `process_with_spark` — обработка данных с помощью Spark.
- `check_hdfs_output` — проверка наличия выходных данных в HDFS.

Конфигурация подключения:
- `hdfs_default` — подключение к HDFS (namenode:8020).
- `spark_default` — подключение к Spark (spark-master:7077).
- `kafka_broker` — подключение к Kafka (kafka:9092).

#### 2. Загрузка данных из Kafka.

Скрипт [`kafka_loader.py`](./scripts/kafka_loader.py) подключается к Kafka-топику `input_topic` и сохраняет данные в HDFS:

```python
from kafka import KafkaConsumer
import hdfs

def load_data():
    consumer = KafkaConsumer(
        'input_topic',
        bootstrap_servers='kafka-broker:9092',
        auto_offset_reset='earliest'
    )
    
    client = hdfs.InsecureClient("http://namenode:50070")
    
    with client.write('/data/input/raw_data.csv') as writer:
        for message in consumer:
            writer.write(message.value + b'\n')

if __name__ == "__main__":
    load_data()
```

#### 3. Обработка данных через Spark.

Скрипт [`spark_processor.py`](./scripts/spark_processor.py) читает сырые данные из HDFS, фильтрует их и агрегирует:

```python
from pyspark.sql import SparkSession

def process_data():
    spark = SparkSession.builder \
        .appName("AirflowSparkProcessor") \
        .getOrCreate()
    
    # Чтение данных из HDFS
    df = spark.read.csv("hdfs://namenode:8020/data/input/raw_data.csv", header=True)
    
    # Пример обработки: фильтрация значений > 100 по группам category
    processed_df = df.filter(df["value"] > 100).groupBy("category").count()
    
    # Сохранение результатов в Parquet
    processed_df.write.parquet("hdfs://namenode:8020/data/output/processed_data.parquet")
    
    spark.stop()

if __name__ == "__main__":
    process_data()
```

#### 4. Конфигурация

Файл [`pipeline.cfg`](./config/pipeline.cfg) содержит параметры подключения:

```ini
[kafka]
bootstrap_servers = kafka:9092
topic = input_topic

[hdfs]
namenode = hdfs://namenode:8020
input_path = /data/input/raw_data.csv
output_path = /data/output/processed_data.parquet
```

### Запуск пайплайна

Для автоматизации всех операций используется скрипт [`scripts.sh`](./scripts.sh):

```bash
#!/bin/bash

# Копирование скриптов в контейнер Airflow.
docker cp scripts/kafka_loader.py airflow-webserver:/opt/airflow/scripts
docker cp scripts/spark_processor.py airflow-webserver:/opt/airflow/scripts
docker cp dags/spark_kafka_pipeline.py airflow-webserver:/opt/airflow/dags
docker cp config/pipeline.cfg airflow-webserver:/opt/airflow/config

# Триггер запуска DAG.
docker exec -it airflow-webserver airflow dags trigger spark_kafka_pipeline

# Логи.
docker logs airflow-webserver
docker logs airflow-scheduler
```

После успешного выполнения пайплайна в HDFS создаются:
- Входные данные: `/data/input/raw_data.csv`
- Выходные данные: `/data/output/processed_data.parquet`

Пример результатов обработки:

```
+--------+-----+
|category|count|
+--------+-----+
|       A|  150|
|       B|  230|
|       C|   95|
+--------+-----+
```

Пайплайн успешно загрузил данные из Kafka, обработал их с помощью Spark и сохранил агрегированные результаты в HDFS в формате Parquet.
