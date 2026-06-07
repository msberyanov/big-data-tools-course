# Отчёт по лабораторной работе 5.

---

## Задание.

**Цель**: Написать программу на Scala с использованием Apache Flink для потоковой обработки данных из Kafka.

**Реализация**:

### KafkaFlinkStreamingApp.scala

Основной класс `KafkaFlinkStreamingApp` настраивает Flink-сессию, подключается к Kafka-топику, обрабатывает поток сообщений и выводит результаты.

```scala
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy

object KafkaFlinkStreamingApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9093")
      .setTopics("input-topic")
      .setGroupId("flink-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka Source"
    )

    stream.map { msg =>
      println(s"Processing: $msg")
      s"Processed: $msg (length: ${msg.length})"
    }.print()

    env.execute("Kafka to Flink Streaming Job")
  }
}
```

Для запуска кластера Hadoop-Spark-Kafka-Flink используется файл [`docker-compose.yml`](./docker-compose.yml):

```yaml
version: '3.8'

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    hostname: namenode
    ports:
      - "9870:9870"  # Web UI Hadoop (NameNode)
      - "9000:9000"  # HDFS RPC endpoint (fs.defaultFS = hdfs://namenode:9000)
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
      - "8080:8080"  # MasterWebUI
      - "7077:7077"  # Spark worker communication
      - "4040:4040" # SparkUI
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
      - "8081:8081"  # WorkerUI
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
      - "9092:9092"  # Внешнее подключение (с хоста)
      - "9093:9093"  # Внутреннее подключение (межконтейнерное)
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

  flink-jobmanager:
    image: flink:1.17.1-scala_2.12-java11
    container_name: flink-jobmanager
    ports:
      - "8082:8082"  # Flink Web UI
      - "6123:6123"  # RPC
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=1
      - STATE_BACKEND=filesystem
      - STATE_CHECKPOINTS_DIR=hdfs://namenode:9000/flink/checkpoints
      - STATE_SAVEPOINTS_DIR=hdfs://namenode:9000/flink/savepoints
      - FS_DEFAULT_SCHEME=hdfs://namenode:9000
    volumes:
      - ./flink-data:/opt/flink/data
    depends_on:
      - kafka
    networks:
      - hadoop

  flink-taskmanager-1:
    image: flink:1.17.1-scala_2.12-java11
    container_name: flink-taskmanager-1
    command: taskmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=1
      - STATE_BACKEND=filesystem
      - STATE_CHECKPOINTS_DIR=hdfs://namenode:9000/flink/checkpoints
      - STATE_SAVEPOINTS_DIR=hdfs://namenode:9000/flink/savepoints
      - FS_DEFAULT_SCHEME=hdfs://namenode:9000
    volumes:
      - ./flink-data:/opt/flink/data
    depends_on:
      - flink-jobmanager
    networks:
      - hadoop

  flink-taskmanager-2:
    image: flink:1.17.1-scala_2.12-java11
    container_name: flink-taskmanager-2
    command: taskmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=1
      - STATE_BACKEND=filesystem
      - STATE_CHECKPOINTS_DIR=hdfs://namenode:9000/flink/checkpoints
      - STATE_SAVEPOINTS_DIR=hdfs://namenode:9000/flink/savepoints
      - FS_DEFAULT_SCHEME=hdfs://namenode:9000
    volumes:
      - ./flink-data:/opt/flink/data
    depends_on:
      - flink-jobmanager
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

# Создание топика input-topic.
docker exec -it kafka kafka-topics --create --topic input-topic --partitions 4 --replication-factor 1 --bootstrap-server kafka:9093

# Проверка наличия топика.
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9093

# Копирование JAR в контейнер flink-jobmanager.
docker cp target/scala-2.12/lab-5.jar flink-jobmanager:/opt/flink/

# Запуск приложения.
docker exec -it flink-jobmanager ./bin/flink run -c KafkaFlinkStreamingApp /opt/flink/lab-5.jar

# Проверка логов (в отдельном терминале).
docker logs flink-taskmanager-1 | grep "Processing:"

# Ввод сообщений в топик (в отдельном терминале):
docker exec -it kafka kafka-console-producer --topic input-topic --bootstrap-server kafka:9092

# Поиск ID задания.
docker exec -it flink-jobmanager ./bin/flink list

# Завершение задания.
docker exec -it flink-jobmanager ./bin/flink cancel 5a33d8027156e521c95d44894fb7aa25
```

**Выходные данные** (результаты обработки сообщений):

```
$ docker logs flink-taskmanager-1 | grep "Processing:"
Processing: test message 1
Processing: test message 2
```

Программа успешно обработала два тестовых сообщения:
- `test message 1`
- `test message 2`

Оба сообщения были получены из Kafka-топика, обработаны и выведены в консоль Flink TaskManager с префиксом "Processing:".
