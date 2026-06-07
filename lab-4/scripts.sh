#!/bin/bash

# Сборка Jar.
sbt assembly

# Копирование приложения в контейнер spark-master.
docker cp target/scala-2.12/lab-4.jar spark-master:/tmp/

# Запуск задачи.
docker exec -it spark-master spark/bin/spark-submit --class TextClassificationDemo \
--deploy-mode client --executor-memory 512M --executor-cores 1 --num-executors 1 \
--master spark://spark-master:7077 /tmp/lab-4.jar