# Отчёт по лабораторной работе 4.

---

## Задание.

**Цель**: Написать программу на Scala с использованием Apache Spark MLlib для классификации текста с использованием логистической регрессии.

**Реализация**:

### TextClassificationDemo.scala

Основной класс `TextClassificationDemo` настраивает Spark-сессию, создает обучающий и тестовый датасеты, строит конвейер обработки и обучения, затем выполняет предсказания и оценивает точность модели.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, StopWordsRemover, StringIndexer}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object TextClassificationDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TextClassificationDemo")
      .master("spark://spark-master:7077")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val training = Seq(
      (0L, "spark is great", 1.0),
      (1L, "mlib is awesome", 1.0),
      (2L, "learning spark is fun", 1.0),
      (3L, "the weather is bad", 0.0),
      (4L, "the rain is annoying", 0.0),
      (5L, "snow is cold", 0.0)
    ).toDF("id", "text", "label")

    val test = Seq(
      (6L, "spark is cool", 1.0),
      (7L, "apache spark is powerful", 1.0),
      (8L, "the storm is coming", 0.0),
      (9L, "winter is cold", 0.0)
    ).toDF("id", "text", "label")

    println("Training Dataset:")
    training.show()

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filteredWords")
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))

    val hashingTF = new HashingTF()
      .setInputCol("filteredWords")
      .setOutputCol("features")
      .setNumFeatures(1000)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, lr))

    val model = pipeline.fit(training)

    val predictions = model.transform(test)
    println("Predictions:")
    predictions.select("text", "label", "prediction", "probability").show(false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Accuracy = $accuracy")

    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    spark.stop()
  }
}
```

Для запуска Hadoop-Spark кластера используется файл [`docker-compose.yml`](./docker-compose.yml):

```yaml
version: '3.8'

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    hostname: namenode
    ports:
      - "9870:9870"
      - "9000:9000"
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
      - "8080:8080"
      - "7077:7077"
      - "4040:4040"
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
      - "8081:8081"
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

# Копирование приложения в контейнер spark-master.
docker cp target/scala-2.12/lab-4.jar spark-master:/tmp/

# Запуск задачи.
docker exec -it spark-master spark/bin/spark-submit --class TextClassificationDemo \
--deploy-mode client --executor-memory 512M --executor-cores 1 --num-executors 1 \
--master spark://spark-master:7077 /tmp/lab-4.jar
```

**Выходные данные** (результаты классификации):

```
Training Dataset:
+---+--------------------+-----+
| id|                text|label|
+---+--------------------+-----+
|  0|      spark is great|  1.0|
|  1|     mlib is awesome|  1.0|
|  2|learning spark is...|  1.0|
|  3|  the weather is bad|  0.0|
|  4|the rain is annoying|  0.0|
|  5|        snow is cold|  0.0|
+---+--------------------+-----+
```

```
Predictions:
+------------------------+-----+----------+----------------------------------------+
|text                    |label|prediction|probability                             |
+------------------------+-----+----------+----------------------------------------+
|spark is cool           |1.0  |1.0       |[0.14388237674849882,0.8561176232515012]|
|apache spark is powerful|1.0  |1.0       |[0.14388237674849882,0.8561176232515012]|
|the storm is coming     |0.0  |0.0       |[0.580407142496285,0.419592857503715]   |
|winter is cold          |0.0  |0.0       |[0.897059198159274,0.10294080184072596] |
+------------------------+-----+----------+----------------------------------------+
```

```
Test Accuracy = 1.0
```

**Описание процесса**:

1. **Tokenization**: Текст разбивается на отдельные слова
2. **Stop Words Removal**: Удаляются стоп-слова (a, the, is, is, etc.)
3. **HashingTF**: Преобразование слов в векторы фиксированной длины (1000 признаков)
4. **Logistic Regression**: Обучение модели на обучающем датасете с 6 примерами (3 положительных, 3 отрицательных)
5. **Prediction**: Классификация 4 тестовых примеров
6. **Evaluation**: Расчет точности (Accuracy = 1.0 — 100% правильных предсказаний)

Модель успешно классифицировала все тестовые примеры:
- Положительные тексты (содержащие "spark/mlib") получили предсказание 1.0.
- Отрицательные тексты (содержащие погодные термины) получили предсказание 0.0.

Сохраненная модель доступна по пути `/tmp/spark-logistic-regression-model`.
