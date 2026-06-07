# Отчёт по лабораторной работе 6.

---

## Задание.

**Цель**: Написать программу на Scala с использованием Apache Spark для интеграции с Hive и выполнения SQL-запросов к таблицам Hive.

**Реализация**:

### SparkHiveDemo.scala

Основной класс `SparkHiveDemo` настраивает SparkSession с поддержкой Hive, создает базу данных, таблицу, вставляет данные и выполняет SQL-запросы.

```scala
import org.apache.spark.sql.SparkSession

object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Hive Integration")
      .master("spark://spark-master:7077")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
      .config("spark.hadoop.hive.metastore.schema.verification", "false")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
    spark.sql("USE demo_db")
    spark.sql(
      """
CREATE TABLE IF NOT EXISTS users (
id INT,
name STRING,
age INT
)
STORED AS PARQUET
""")
    spark.sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")

    println("Available users:")
    spark.sql("SELECT * FROM users").show()

    println("Users aged more than 26:")
    spark.table("demo_db.users")
      .filter($"age" > 26)
      .show()

    spark.stop()
  }
}
```

Для запуска кластера Hadoop-Spark-Hive используется файл [`docker-compose.yml`](./docker-compose.yml):

```yaml
version: "3"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
    volumes:
      - namenode:/hadoop/dfs/name
    environment:
      - CLUSTER_NAME=test
    env_file:
      - ./hadoop-hive.env
    ports:
      - "50070:50070"
    networks:
      - hadoop

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8
    volumes:
      - datanode:/hadoop/dfs/data
    env_file:
      - ./hadoop-hive.env
    environment:
      SERVICE_PRECONDITION: "namenode:50070"
    ports:
      - "50075:50075"
    networks:
      - hadoop

  spark-master:
    image: bde2020/spark-master:3.1.2-hadoop3.2
    container_name: spark-master
    ports:
      - "7077:7077"  # Spark worker communication
      - "4040:4040" # SparkUI
    environment:
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_PUBLIC_DNS=localhost  # можно поменять, если нужен внешний доступ
      - HADOOP_CORE_CONF_fs_defaultFS=hdfs://namenode:8020
    volumes:
      - ./spark-data:/opt/spark/work  # рабочая директория для Spark
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

  hive-server:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    environment:
      HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://hive-metastore/metastore"
      SERVICE_PRECONDITION: "hive-metastore:9083"
    ports:
      - "10000:10000"
    networks:
      - hadoop

  hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    env_file:
      - ./hadoop-hive.env
    command: /opt/hive/bin/hive --service metastore
    environment:
      SERVICE_PRECONDITION: "namenode:50070 datanode:50075 hive-metastore-postgresql:5432"
    ports:
      - "9083:9083"
    networks:
      - hadoop

  hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0
    container_name: hive-metastore-postgresql
    ports:
      - "5432:5432"
    networks:
      - hadoop

  presto-coordinator:
    image: shawnzhu/prestodb:0.181
    ports:
      - "8080:8080"
    networks:
      - hadoop

volumes:
  namenode:
  datanode:

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

# Копирование JAR в контейнер flink-jobmanager.
docker cp target/scala-2.12/lab-6.jar spark-master:/opt/

# Запуск приложения.
docker exec -it spark-master /spark/bin/spark-submit --class SparkHiveDemo \
--master spark://spark-master:7077 /opt/lab-6.jar
```

**Выходные данные** (результаты выполнения Spark-задания):

```
Available users:
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|Alice| 25|
|  2|  Bob| 30|
+---+-----+---+

Users aged more than 26:
+---+----+---+
| id|name|age|
+---+----+---+
|  2| Bob| 30|
+---+----+---+
```

Программа успешно выполнила следующие операции:

1. Создала базу данных `demo_db` в Hive
2. Создала таблицу `users` со схемой (id INT, name STRING, age INT) в формате Parquet
3. Вставила две записи: (1, 'Alice', 25) и (2, 'Bob', 30)
4. Выполнила SELECT запрос для отображения всех пользователей
5. Выполнила фильтрацию пользователей старше 26 лет (только Bob с возрастом 30)

Spark корректно интегрировался с Hive через Thrift-метастор и выполнил запросы к Parquet-таблицам, хранящимся в HDFS.
