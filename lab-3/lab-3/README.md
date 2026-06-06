# Отчёт по лабораторной работе 3.3.

---

## Задание.

**Цель**: Написать программу на Scala с использованием Apache Spark Streaming для анализа транзакций в реальном времени из Kafka с использованием оконных функций и водяных знаков.

**Реализация**:

### SparkStreamingDemo.scala

Основной класс `SparkStreamingDemo` настраивает Spark-сессию, читает данные из Kafka, выполняет агрегацию по окнам и выводит результаты в консоль.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaSparkStreamingDemo")
      .master("spark://spark-master:7077")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("user_id", StringType),
      StructField("product_id", StringType),
      StructField("amount", DoubleType),
      StructField("timestamp", TimestampType)
    ))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093")
      .option("subscribe", "transactions")
      .option("startingOffsets", "earliest")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) as json")

    val transactionsDF = jsonDF
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    val windowedCounts = transactionsDF
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window($"timestamp", "5 minutes"),
        $"user_id"
      )
      .agg(
        count("*").as("transaction_count"),
        sum($"amount").as("total_amount")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"user_id",
        $"transaction_count",
        $"total_amount"
      )

    val query = windowedCounts.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
```

Для запуска Hadoop-Spark-Kafka кластера используется файл [`docker-compose.yml`](./docker-compose.yml):

```yaml
version: '3.8'

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    hostname: namenode
    ports:
      - "9870:9870"
      - "9000:9000"
    environment:
      - CLUSTER_NAME=test
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - ./hadoop-namenode:/hadoop-data
    networks:
      - hadoop

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    hostname: datanode
    depends_on:
      - namenode
    environment:
      - SERVICE_NAME=datanode
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - ./hadoop-datanode:/hadoop-data
    networks:
      - hadoop

  spark-master:
    image: bde2020/spark-master:3.1.2-hadoop3.2
    container_name: spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
      - "4040:4040"
    environment:
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_PUBLIC_DNS=localhost
      - HADOOP_CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    volumes:
      - ./spark-data:/opt/spark/work
    depends_on:
      - namenode
    networks:
      - hadoop

  spark-worker:
    image: bde2020/spark-worker:3.1.2-hadoop3.2
    container_name: spark-worker
    ports:
      - "8081:8081"
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1g
    volumes:
      - ./spark-data:/opt/spark/work
    networks:
      - hadoop

  zookeeper:
    image: confluentinc/cp-zookeeper:6.2.1
    container_name: zookeeper
    environment:
      - ZOOKEEPER_CLIENT_PORT=2181
    networks:
      - hadoop

  kafka:
    image: confluentinc/cp-kafka:6.2.1
    container_name: kafka
    ports:
      - "9092:9092"
      - "9093:9093"
    depends_on:
      - zookeeper
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9093,PLAINTEXT_EXTERNAL://0.0.0.0:9092
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9093,PLAINTEXT_EXTERNAL://localhost:9092
      - KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT
      - KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
    networks:
      - hadoop

networks:
  hadoop:
    driver: bridge
```

## Примечание.

Для автоматизации всех операций используется скрипт [`scripts.sh`](./scripts.sh):

```bash
#!/bin/bash

# Сборка Jar.
sbt assembly

# Копирование приложения в контейнер spark-master.
docker cp target/scala-2.12/lab-3.jar spark-master:/tmp/

# Запуск задачи.
docker exec -it spark-master spark/bin/spark-submit --class SparkStreamingDemo \
--master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
--deploy-mode client /tmp/lab-3.jar
```

Для генерации данных в Kafka используется класс `KafkaProducerDemo.scala`, который отправляет JSON-сообщения в топик `transactions` каждые 1-5 секунд.

**Выходные данные** (результаты агрегации по 5-минутным окнам):

```
-------------------------------------------
Batch: 4
-------------------------------------------
+---------------------+---------------------+-------+-----------------+------------+
|window_start         |window_end           |user_id|transaction_count|total_amount|
+---------------------+---------------------+-------+-----------------+------------+
|+58399-10-12 07:15:00|+58399-10-12 07:20:00|user3  |1                |87.29       |
|+58399-10-12 06:25:00|+58399-10-12 06:30:00|user2  |1                |66.03       |
|+58399-10-12 08:05:00|+58399-10-12 08:10:00|user2  |1                |2.07        |
|+58399-10-12 09:10:00|+58399-10-12 09:15:00|user2  |1                |40.86       |
+---------------------+---------------------+-------+-----------------+------------+
```

```
-------------------------------------------
Batch: 8
-------------------------------------------
+---------------------+---------------------+-------+-----------------+------------+
|window_start         |window_end           |user_id|transaction_count|total_amount|
+---------------------+---------------------+-------+-----------------+------------+
|+58399-10-12 16:25:00|+58399-10-12 16:30:00|user5  |1                |64.28       |
|+58399-10-12 18:05:00|+58399-10-12 18:10:00|user5  |1                |21.8        |
|+58399-10-12 17:15:00|+58399-10-12 17:20:00|user3  |1                |69.89       |
|+58399-10-12 17:50:00|+58399-10-12 17:55:00|user2  |1                |21.64       |
+---------------------+---------------------+-------+-----------------+------------+
```

```
-------------------------------------------
Batch: 12
-------------------------------------------
+---------------------+---------------------+-------+-----------------+------------+
|window_start         |window_end           |user_id|transaction_count|total_amount|
+---------------------+---------------------+-------+-----------------+------------+
|+58399-10-13 01:20:00|+58399-10-13 01:25:00|user5  |1                |60.23       |
|+58399-10-13 02:25:00|+58399-10-13 02:30:00|user4  |1                |88.82       |
+---------------------+---------------------+-------+-----------------+------------+
```

Подробные логи выполнения сохранены в файле [`output.txt`](./output.txt), содержащем информацию о всех батчах (Batch 4-12) с метаданными о производительности, количестве обработанных строк, времени выполнения и состоянии водяных знаков.
