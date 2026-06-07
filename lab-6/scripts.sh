#!/bin/bash

# Сборка Jar.
sbt assembly

# Копирование JAR в контейнер flink-jobmanager.
docker cp target/scala-2.12/lab-6.jar spark-master:/opt/

# Запуск приложения.
docker exec -it spark-master /spark/bin/spark-submit --class SparkHiveDemo \
--master spark://spark-master:7077 /opt/lab-6.jar