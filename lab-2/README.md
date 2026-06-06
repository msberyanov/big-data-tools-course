# Отчёт по лабораторной работе 2.

---

## Задание 1.

**Цель**: Написать программу на Scala, которая подключается к HDFS (Hadoop Distributed File System) и выводит содержимое корневого каталога.

**Реализация**:
```scala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object Main {
  def main(args: Array[String]): Unit = {
    val hdfsUri = "hdfs://localhost:9000"

    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    conf.set("dfs.replication", "1")

    val fs = FileSystem.get(new URI(hdfsUri), conf)

    val path = new Path("/")
    val status = fs.listStatus(path)

    println("Содержимое корневого каталога HDFS:")

    status.foreach(x => println(x.getPath))

    fs.close()
  }
}
```

**Результат выполнения** (при запущенных Docker-контейнерах Hadoop):
```
Содержимое корневого каталога HDFS:
hdfs://localhost:9000/user
```

Для запуска Hadoop-кластера используется файл [`docker-compose.yml`](./docker-compose.yml):

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

networks:
  hadoop:
    driver: bridge
```

Для создания директории в HDFS используется скрипт [`scripts.sh`](./scripts.sh):

```bash
#!/bin/zsh
# Скрипт для создания директории в HDFS и проверки её наличия.
# Предполагается, что Hadoop-кластер запущен через docker-compose (с именем контейнера 'namenode').

# Команда 1: Создаёт директорию /tmp в HDFS (рекурсивно, если нужны родительские папки).
# -p: аналог "mkdir -p" — не выдаёт ошибку, если директория уже существует.
# /tmp — путь в HDFS (не путать с /tmp на хост-машине!).
docker exec -it namenode hdfs dfs -mkdir -p /tmp

# Команда 2: Выводит список корневого каталога HDFS (аналог `ls /` в Linux).
# Позволяет убедиться, что директория /tmp действительно была создана.
docker exec -it namenode hdfs dfs -ls /
```

## Примечание.

Для успешного выполнения программы необходимо запустить Docker-контейнеры с Hadoop (NameNode и DataNode) с помощью команды:

```bash
docker-compose up -d
```

Далее необходимо создать директории внутри запущенного контейнера:

```bash
./scripts.sh
```

Приложение исполняется с помощью утилиты sbt:

```bash
sbt
```

После завершения работы контейнеры останавливаются командой:

```bash
docker-compose down
```