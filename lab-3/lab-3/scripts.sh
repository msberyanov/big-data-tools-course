#!/bin/bash

# Сборка Jar.
sbt assembly

# Копирование приложения в контейнер spark-master.
docker cp target/scala-2.12/lab-3.jar spark-master:/tmp/

# Запуск задачи.
docker exec -it spark-master spark/bin/spark-submit --class SparkStreamingDemo \
--master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
--deploy-mode client /tmp/lab-3.jar