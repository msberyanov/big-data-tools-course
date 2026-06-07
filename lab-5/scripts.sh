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
docker exec -it kafka kafka-console-producer --topic input-topic --bootstrap-server kafka:9093

# Поиск ID задания.
docker exec -it flink-jobmanager ./bin/flink list

# Завершение задания.
docker exec -it flink-jobmanager ./bin/flink cancel 5a33d8027156e521c95d44894fb7aa25