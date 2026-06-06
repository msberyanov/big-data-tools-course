#!/bin/bash

# Сборка Jar.
sbt compile
sbt assembly

# Перенос файлов lab-2.jar и sales_data.csv в файловую систему контейнера namenode.
docker cp target/scala-2.12/lab-2.jar namenode:/tmp
docker cp sales_data.csv namenode:/tmp/

# Создание в hdfs директории для набора данных.
docker exec -it namenode hadoop fs -mkdir -p /data

# Загрузка набора данных в HDFS.
docker exec -it namenode hdfs dfs -put -f /tmp/sales_data.csv /data/

# Создание директории для приложения в HDFS.
docker exec -it namenode hdfs dfs -mkdir -p /jobs

# Загрузка приложения в HDFS.
docker cp target/scala-2.12/lab-2.jar namenode:/tmp/lab-2.jar
docker exec -it namenode hdfs dfs -put -f /tmp/lab-2.jar /jobs/

# Запуск Spark приложения с выводом логов.
docker exec -it spark-master /spark/bin/spark-submit --class Main \
--master spark://spark-master:7077 --deploy-mode client --executor-memory 512M --executor-cores 1 --num-executors 1 \
--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 --conf spark.sql.debugger.enabled=true hdfs:///jobs/lab-2.jar

# Чтение результата.
docker exec -it namenode hdfs dfs -cat hdfs:///results/top_5_products/part-00000-2bbb9a88-8ca8-47e2-adc4-8fcf6ce83ec1-c000.csv