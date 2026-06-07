# Лабораторная работа 7: Интеграция Airflow, Spark и Kafka

## Описание

В данной лабораторной работе реализована пайплайн обработки данных с использованием следующих технологий:
- **Apache Airflow** - оркестрация пайплайнов
- **Apache Spark** - распределенная обработка данных
- **Apache Kafka** - потоковая передача данных
- **Hadoop HDFS** - распределенное хранилище данных

## Структура проекта

```
lab-7/
├── docker-compose.yml          # Конфигурация Docker Compose
├── scripts/                    # Скрипты обработки данных
│   ├── spark_processor.py      # Spark-скрипт для обработки данных
│   └── kafka_loader.py         # Скрипт для загрузки данных из Kafka в HDFS
├── dags/                       # DAG-файлы Airflow
│   └── spark_kafka_pipeline.py # Основной DAG для пайплайна
├── config/                     # Конфигурационные файлы
│   └── pipeline.cfg            # Конфигурация пайплайна
├── scripts.sh                  # Скрипт для запуска всего проекта
└── README.md                   # Документация
```

## Технологический стек

### Apache Airflow
- Оркестрация задач пайплайна
- Планирование и мониторинг
- Web UI для управления пайплайнами

### Apache Spark
- Распределенная обработка больших данных
- Spark SQL для анализа данных
- Интеграция с HDFS

### Apache Kafka
- Потоковая передача данных в реальном времени
- Темы для публикации/подписки
- Связь между источниками данных и обработчиками

### Hadoop HDFS
- Распределенное хранилище данных
- Высокая доступность и отказоустойчивость

## Запуск проекта

### Вариант 1: Использование скрипта запуска

```bash
chmod +x scripts.sh
./scripts.sh
```

### Вариант 2: Ручной запуск

```bash
# 1. Запуск контейнеров
docker-compose up -d

# 2. Ожидание инициализации (30-60 секунд)
sleep 60

# 3. Включение DAG в Airflow
docker exec airflow-airflow-webserver-1 airflow dags unpause spark_kafka_pipeline

# 4. Проверка статуса
docker-compose ps
```

## Доступные сервисы

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| Airflow WebUI | http://localhost:8080 | admin | admin |
| Spark Master UI | http://localhost:4040 | - | - |
| Hadoop NameNode | http://localhost:50070 | - | - |
| Kafka (внешний) | localhost:9092 | - | - |
| Kafka (внутренний) | kafka:9093 | - | - |

## Описание пайплайна

### DAG: spark_kafka_pipeline

1. **generate_test_data** - Генерация тестовых данных
2. **copy_to_hdfs** - Копирование данных в HDFS
3. **process_with_spark** - Обработка данных с помощью Spark
4. **check_output** - Проверка результатов обработки
5. **send_notification** - Уведомление об успешном завершении

## Скрипты

### spark_processor.py

Spark-приложение, которое:
- Читает данные из HDFS (CSV)
- Фильтрует данные (value > 100)
- Группирует по категории и считает количество
- Сохраняет результат в формате Parquet

### kafka_loader.py

Скрипт для загрузки данных из Kafka в HDFS:
- Подписывается на Kafka-тему
- Считывает сообщения
- Записывает в HDFS в формате CSV

## Переменные окружения

### spark_processor.py
- `INPUT_PATH` - путь к входным данным в HDFS (по умолчанию: hdfs://hadoop:8020/data/input/raw_data.csv)
- `OUTPUT_PATH` - путь к выходным данным в HDFS (по умолчанию: hdfs://hadoop:8020/data/output/processed_data.parquet)

### kafka_loader.py
- `BOOTSTRAP_SERVERS` - адреса Kafka брокеров (по умолчанию: kafka:9092)
- `KAFKA_TOPIC` - имя Kafka-темы (по умолчанию: input_topic)
- `HDFS_URL` - URL HDFS NameNode (по умолчанию: http://hadoop:50070)
- `HDFS_PATH` - путь в HDFS (по умолчанию: /data/input/raw_data.csv)

## Управление Docker контейнерами

```bash
# Остановка всех контейнеров
docker-compose down

# Остановка с удалением томов
docker-compose down -v

# Просмотр логов
docker-compose logs -f

# Подключение к контейнеру
docker exec -it <container_name> bash
```

## Тестирование

```bash
# Проверка данных в HDFS
docker exec hadoop hdfs dfs -ls /data/input
docker exec hadoop hdfs dfs -cat /data/input/raw_data.csv | head -20

# Проверка результатов обработки
docker exec hadoop hdfs dfs -ls /data/output
docker exec hadoop hdfs dfs -cat /data/output/processed_data.parquet/part-*.parquet | head -10
```

## Решение проблем

### Модуль pyspark не найден
```bash
# Установка в локальное окружение
pip install pyspark

# Или запуск внутри Docker контейнера
docker exec airflow-airflow-webserver-1 pip install pyspark
```

### Airflow не находит DAG
```bash
docker exec airflow-airflow-webserver-1 airflow dags list
docker exec airflow-airflow-webserver-1 airflow dags unpause spark_kafka_pipeline
```

### Spark не подключается к HDFS
Проверьте переменную окружения `HADOOP_CORE_CONF_fs_defaultFS` в docker-compose.yml

## Требования

- Docker 20+
- Docker Compose 1.29+
- Python 3.8+ (для разработки скриптов)

## Автор

Белянов М.С.
Курс: "Инструменты работы с большими данными"
