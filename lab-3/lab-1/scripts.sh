#!/bin/zsh
# Скрипт для создания директории в HDFS и проверки её наличия.
# Предполагается, что Hadoop-кластер запущен через docker-compose (с именем контейнера 'namenode').

# 1. Создаёт директорию /tmp в HDFS (рекурсивно, если нужны родительские папки).
# -p: аналог "mkdir -p" — не выдаёт ошибку, если директория уже существует.
# /tmp — путь в HDFS (не путать с /tmp на хост-машине!).
docker exec -it namenode hdfs dfs -mkdir -p /tmp

# 2. Выводит список корневого каталога HDFS (аналог `ls /` в Linux).
# Позволяет убедиться, что директория /tmp действительно была создана.
docker exec -it namenode hdfs dfs -ls /

# Сборка Jar.
sbt assembly

# Перенос файлы hadoop-demo.jar и input.txt в файловую систему контейнера namenode.
docker cp target/scala-2.13/lab-1.jar namenode:/tmp/
docker cp input.txt namenode:/tmp/

# Создание в hdfs директории для входных данных.
docker exec -it namenode hadoop fs -mkdir -p /input

# Перемещение файла input.txt из fs контейнера в hdfs.
docker exec -it namenode hadoop fs -put /tmp/input.txt /input/input.txt

# Запуск задания.
docker exec -it namenode hadoop jar /tmp/lab-1.jar /input/input.txt /output/

# Просмотр результата работы задания.
docker exec -it namenode hadoop fs -cat /output/part-r-00000

