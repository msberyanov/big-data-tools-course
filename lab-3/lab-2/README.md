# Отчёт по лабораторной работе 3.2.

---

## Задание.

**Цель**: Написать программу на Scala с использованием Apache Spark для анализа данных о продажах из CSV-файла `sales_data.csv` с сохранением результатов в HDFS.

**Реализация**:

Программа состоит из одного основного класса `Main.scala`, который выполняет три аналитических расчета:

### 1. Анализ прибыли по категориям.

Группирует данные по полю `category` и вычисляет суммарную прибыль (`Profit`) для каждой категории, сортируя результаты по убыванию.

```scala
val profitByCategory = df.groupBy("category")
  .agg(sum("Profit").alias("total_profit"))
  .orderBy(desc("total_profit"))
```

### 2. Средняя прибыль на покупку по странам.

Группирует данные по полю `Country` и вычисляет среднюю прибыль на одну покупку (`Profit`) для каждой страны, сортируя результаты по убыванию.

```scala
val avgProfitByCountry = df.groupBy("Country")
  .agg(avg("Profit").alias("avg_profit_per_order"))
  .orderBy(desc("avg_profit_per_order"))
```

### 3. Топ-5 продуктов по количеству заказов.

Группирует данные по полю `Product` и вычисляет суммарное количество заказанных единиц (`Order_Quantity`) для каждого продукта, выбирая топ-5 по убыванию.

```scala
val top5Products = df.groupBy("Product")
  .agg(sum("Order_Quantity").alias("total_quantity"))
  .orderBy(desc("total_quantity"))
  .limit(5)
```

### Main.scala

Основной класс `Main` настраивает Spark-сессию, читает данные из CSV-файла, выполняет три аналитических расчета и сохраняет результаты в HDFS в формате CSV с заголовками.

```scala
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RetailSalesAnalysis")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/sales_data.csv")

      val profitByCategory = df.groupBy("category")
        .agg(sum("Profit").alias("total_profit"))
        .orderBy(desc("total_profit"))

      println("=== Profit by Category ===")
      profitByCategory.show()

      val avgProfitByCountry = df.groupBy("Country")
        .agg(avg("Profit").alias("avg_profit_per_order"))
        .orderBy(desc("avgProfitByCountry"))

      println("=== Average Profit by Country ===")
      avgProfitByCountry.show()

      val top5Products = df.groupBy("Product")
        .agg(sum("Order_Quantity").alias("total_quantity"))
        .orderBy(desc("total_quantity"))
        .limit(5)

      println("=== Top 5 Products ===")
      top5Products.show()

      profitByCategory.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("hdfs://namenode:9000/results/profit_by_category")

      avgProfitByCountry.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("hdfs://namenode:9000/results/avg_profit_by_country")

      top5Products.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("hdfs://namenode:9000/results/top_5_products")
    } finally {
      spark.stop()
    }
  }
}
```

**Входные данные** (первые 5 строк `sales_data.csv`):

```
Date,Day,Month,Year,Customer_Age,Age_Group,Customer_Gender,Country,State,category,Sub_Category,Product,Order_Quantity,Unit_Cost,Unit_Price,Profit,Cost,Revenue
2013-11-26,26,November,2013,19,Youth (<25),M,Canada,British Columbia,Accessories,Bike Racks,Hitch Rack - 4-Bike,8,45,120,590,360,950
2015-11-26,26,November,2015,19,Youth (<25),M,Canada,British Columbia,Accessories,Bike Racks,Hitch Rack - 4-Bike,8,45,120,590,360,950
2014-03-23,23,March,2014,49,Adults (35-64),M,Australia,New South Wales,Accessories,Bike Racks,Hitch Rack - 4-Bike,23,45,120,1366,1035,2401
2016-03-23,23,March,2016,49,Adults (35-64),M,Australia,New South Wales,Accessories,Bike Racks,Hitch Rack - 4-Bike,20,45,120,1188,900,2088
```

**Результат выполнения** (при запущенных Docker-контейнерах Hadoop и Spark):

Вывод в консоль:

```
=== Profit by Category ===
+-----------+------------+
|   category|total_profit|
+-----------+------------+
|      Bikes|    20519276|
|Accessories|     8862377|
|   Clothing|     2839447|
+-----------+------------+

=== Average Profit by Country ===
+--------------+--------------------+
|       Country|avg_profit_per_order|
+--------------+--------------------+
|United Kingdom|   324.0714390602056|
|       Germany|   302.7568030275725|
|     Australia|   283.0894886363636|
| United States|   282.4476865785849|
|        Canada|    262.187614614191|
|        France|  261.89143480632845|
+--------------+--------------------+

=== Top 5 Products ===
+--------------------+--------------+
|             Product|total_quantity|
+--------------------+--------------+
|Water Bottle - 30...|        164086|
| Patch Kit/8 Patches|        157583|
|  Mountain Tire Tube|        102792|
|        AWC Logo Cap|         67316|
|Sport-100 Helmet,...|         63663|
+--------------------+--------------+

```

Для запуска Hadoop-Spark-кластера используется файл [`docker-compose.yml`](./docker-compose.yml):

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

  spark-master:
    image: bde2020/spark-master:3.1.2-hadoop3.2
    container_name: spark-master
    ports:
      - "8080:8080"  # MasterWebUI
      - "7077:7077"  # Spark worker communication
      - "4040:4040" # SparkUI
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

networks:
  hadoop:
    driver: bridge
```

## Примечание.

Для автоматизации всех операций используется скрипт [`scripts.sh`](./scripts.sh):

```bash
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
```

Вывод команд выглядит следующим образом:

```shell
$ /bin/bash /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/scripts.sh
[info] welcome to sbt 1.12.11 (Microsoft Java 11.0.28)
[info] loading settings for project lab-2-build-build from metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/project/project
[info] loading settings for project lab-2-build from assembly.sbt, metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/project
[success] Generated .bloop/lab-2-build.json
[success] Total time: 2 s, completed 6 июн. 2026 г., 15:51:39
[info] loading settings for project lab-2 from build.sbt...
[info] set current project to lab-2 (in build file:/Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/)
[info] Executing in batch mode. For better performance use sbt's shell
[info] compiling 1 Scala source to /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/target/scala-2.12/classes ...
[success] Total time: 6 s, completed 6 июн. 2026 г., 15:51:45
[info] welcome to sbt 1.12.11 (Microsoft Java 11.0.28)
[info] loading settings for project lab-2-build-build from metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/project/project
[info] loading settings for project lab-2-build from assembly.sbt, metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/project
[success] Generated .bloop/lab-2-build.json
[success] Total time: 2 s, completed 6 июн. 2026 г., 15:51:54
[info] loading settings for project lab-2 from build.sbt...
[info] set current project to lab-2 (in build file:/Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-3/lab-2/)
[info] Strategy 'discard' was applied to 412 files (Run the task at debug level to see details)
[info] Strategy 'first' was applied to 172 files (Run the task at debug level to see details)
[success] Total time: 52 s, completed 6 июн. 2026 г., 15:52:46
Successfully copied 116MB to namenode:/tmp
Successfully copied 15.1MB to namenode:/tmp/

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug namenode
    Learn more at https://docs.docker.com/go/debug-cli/
2026-06-06 08:52:53,099 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug namenode
    Learn more at https://docs.docker.com/go/debug-cli/

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug namenode
    Learn more at https://docs.docker.com/go/debug-cli/
Successfully copied 116MB to namenode:/tmp/lab-2.jar
2026-06-06 08:53:00,298 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug namenode
    Learn more at https://docs.docker.com/go/debug-cli/
26/06/06 08:53:03 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
26/06/06 08:53:05 INFO SparkContext: Running Spark version 3.1.2
26/06/06 08:53:05 INFO ResourceUtils: ==============================================================
26/06/06 08:53:05 INFO ResourceUtils: No custom resources configured for spark.driver.
26/06/06 08:53:05 INFO ResourceUtils: ==============================================================
26/06/06 08:53:05 INFO SparkContext: Submitted application: RetailSalesAnalysis
26/06/06 08:53:05 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 512, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/06/06 08:53:05 INFO ResourceProfile: Limiting resource is cpus at 1 tasks per executor
26/06/06 08:53:05 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/06/06 08:53:05 INFO SecurityManager: Changing view acls to: root
26/06/06 08:53:05 INFO SecurityManager: Changing modify acls to: root
26/06/06 08:53:05 INFO SecurityManager: Changing view acls groups to: 
26/06/06 08:53:05 INFO SecurityManager: Changing modify acls groups to: 
26/06/06 08:53:05 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
26/06/06 08:53:05 INFO Utils: Successfully started service 'sparkDriver' on port 43783.
26/06/06 08:53:05 INFO SparkEnv: Registering MapOutputTracker
26/06/06 08:53:05 INFO SparkEnv: Registering BlockManagerMaster
26/06/06 08:53:05 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/06/06 08:53:05 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/06/06 08:53:05 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/06/06 08:53:05 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-abc6518d-1bfa-4c1d-b25a-dfdf5f5fd402
26/06/06 08:53:06 INFO MemoryStore: MemoryStore started with capacity 366.3 MiB
26/06/06 08:53:06 INFO SparkEnv: Registering OutputCommitCoordinator
26/06/06 08:53:06 INFO Utils: Successfully started service 'SparkUI' on port 4040.
26/06/06 08:53:06 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://localhost:4040
26/06/06 08:53:06 INFO SparkContext: Added JAR hdfs:///jobs/lab-2.jar at hdfs:///jobs/lab-2.jar with timestamp 1780735985080
26/06/06 08:53:06 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://spark-master:7077...
26/06/06 08:53:06 INFO TransportClientFactory: Successfully created connection to spark-master/172.18.0.3:7077 after 58 ms (0 ms spent in bootstraps)
26/06/06 08:53:07 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20260606085307-0000
26/06/06 08:53:07 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 45913.
26/06/06 08:53:07 INFO NettyBlockTransferService: Server created on e323cc9233ad:45913
26/06/06 08:53:07 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/06/06 08:53:07 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, e323cc9233ad, 45913, None)
26/06/06 08:53:07 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20260606085307-0000/0 on worker-20260606084727-172.18.0.5-33843 (172.18.0.5:33843) with 1 core(s)
26/06/06 08:53:07 INFO StandaloneSchedulerBackend: Granted executor ID app-20260606085307-0000/0 on hostPort 172.18.0.5:33843 with 1 core(s), 512.0 MiB RAM
26/06/06 08:53:07 INFO BlockManagerMasterEndpoint: Registering block manager e323cc9233ad:45913 with 366.3 MiB RAM, BlockManagerId(driver, e323cc9233ad, 45913, None)
26/06/06 08:53:07 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, e323cc9233ad, 45913, None)
26/06/06 08:53:07 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, e323cc9233ad, 45913, None)
26/06/06 08:53:07 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20260606085307-0000/0 is now RUNNING
26/06/06 08:53:07 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
26/06/06 08:53:07 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/spark-warehouse').
26/06/06 08:53:07 INFO SharedState: Warehouse path is 'file:/spark-warehouse'.
26/06/06 08:53:08 INFO InMemoryFileIndex: It took 65 ms to list leaf files for 1 paths.
26/06/06 08:53:08 INFO InMemoryFileIndex: It took 10 ms to list leaf files for 1 paths.
26/06/06 08:53:11 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.18.0.5:43486) with ID 0,  ResourceProfileId 0
26/06/06 08:53:11 INFO BlockManagerMasterEndpoint: Registering block manager 172.18.0.5:41023 with 93.3 MiB RAM, BlockManagerId(0, 172.18.0.5, 41023, None)
26/06/06 08:53:12 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:12 INFO FileSourceStrategy: Post-Scan Filters: (length(trim(value#0, None)) > 0)
26/06/06 08:53:12 INFO FileSourceStrategy: Output Data Schema: struct<value: string>
26/06/06 08:53:13 INFO CodeGenerator: Code generated in 433.356716 ms
26/06/06 08:53:13 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 304.0 KiB, free 366.0 MiB)
26/06/06 08:53:13 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 27.5 KiB, free 366.0 MiB)
26/06/06 08:53:13 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on e323cc9233ad:45913 (size: 27.5 KiB, free: 366.3 MiB)
26/06/06 08:53:13 INFO SparkContext: Created broadcast 0 from csv at Main.scala:17
26/06/06 08:53:13 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:13 INFO SparkContext: Starting job: csv at Main.scala:17
26/06/06 08:53:13 INFO DAGScheduler: Got job 0 (csv at Main.scala:17) with 1 output partitions
26/06/06 08:53:13 INFO DAGScheduler: Final stage: ResultStage 0 (csv at Main.scala:17)
26/06/06 08:53:13 INFO DAGScheduler: Parents of final stage: List()
26/06/06 08:53:13 INFO DAGScheduler: Missing parents: List()
26/06/06 08:53:13 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[3] at csv at Main.scala:17), which has no missing parents
26/06/06 08:53:13 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 10.8 KiB, free 366.0 MiB)
26/06/06 08:53:13 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 5.4 KiB, free 366.0 MiB)
26/06/06 08:53:13 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on e323cc9233ad:45913 (size: 5.4 KiB, free: 366.3 MiB)
26/06/06 08:53:13 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:13 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[3] at csv at Main.scala:17) (first 15 tasks are for partitions Vector(0))
26/06/06 08:53:13 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
26/06/06 08:53:13 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (172.18.0.5, executor 0, partition 0, ANY, 4869 bytes) taskResourceAssignments Map()
26/06/06 08:53:14 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.18.0.5:41023 (size: 5.4 KiB, free: 93.3 MiB)
26/06/06 08:53:15 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.18.0.5:41023 (size: 27.5 KiB, free: 93.3 MiB)
26/06/06 08:53:15 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1921 ms on 172.18.0.5 (executor 0) (1/1)
26/06/06 08:53:15 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
26/06/06 08:53:15 INFO DAGScheduler: ResultStage 0 (csv at Main.scala:17) finished in 2.078 s
26/06/06 08:53:15 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:15 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
26/06/06 08:53:15 INFO DAGScheduler: Job 0 finished: csv at Main.scala:17, took 2.133335 s
26/06/06 08:53:15 INFO CodeGenerator: Code generated in 18.784161 ms
26/06/06 08:53:16 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:16 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:16 INFO FileSourceStrategy: Output Data Schema: struct<value: string>
26/06/06 08:53:16 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 304.0 KiB, free 365.7 MiB)
26/06/06 08:53:16 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 27.5 KiB, free 365.6 MiB)
26/06/06 08:53:16 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on e323cc9233ad:45913 (size: 27.5 KiB, free: 366.2 MiB)
26/06/06 08:53:16 INFO SparkContext: Created broadcast 2 from csv at Main.scala:17
26/06/06 08:53:16 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:16 INFO SparkContext: Starting job: csv at Main.scala:17
26/06/06 08:53:16 INFO DAGScheduler: Got job 1 (csv at Main.scala:17) with 2 output partitions
26/06/06 08:53:16 INFO DAGScheduler: Final stage: ResultStage 1 (csv at Main.scala:17)
26/06/06 08:53:16 INFO DAGScheduler: Parents of final stage: List()
26/06/06 08:53:16 INFO DAGScheduler: Missing parents: List()
26/06/06 08:53:16 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[9] at csv at Main.scala:17), which has no missing parents
26/06/06 08:53:16 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 15.8 KiB, free 365.6 MiB)
26/06/06 08:53:16 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 8.0 KiB, free 365.6 MiB)
26/06/06 08:53:16 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on e323cc9233ad:45913 (size: 8.0 KiB, free: 366.2 MiB)
26/06/06 08:53:16 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:16 INFO DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[9] at csv at Main.scala:17) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:16 INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks resource profile 0
26/06/06 08:53:16 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (172.18.0.5, executor 0, partition 0, ANY, 4869 bytes) taskResourceAssignments Map()
26/06/06 08:53:16 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.18.0.5:41023 (size: 8.0 KiB, free: 93.3 MiB)
26/06/06 08:53:17 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.18.0.5:41023 (size: 27.5 KiB, free: 93.2 MiB)
26/06/06 08:53:18 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 2) (172.18.0.5, executor 0, partition 1, ANY, 4869 bytes) taskResourceAssignments Map()
26/06/06 08:53:18 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 2057 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:18 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 2) in 213 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:18 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
26/06/06 08:53:18 INFO DAGScheduler: ResultStage 1 (csv at Main.scala:17) finished in 2.319 s
26/06/06 08:53:18 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:18 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
26/06/06 08:53:18 INFO DAGScheduler: Job 1 finished: csv at Main.scala:17, took 2.328506 s
=== Profit by Category ===
26/06/06 08:53:18 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:18 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:18 INFO FileSourceStrategy: Output Data Schema: struct<category: string, Profit: int>
26/06/06 08:53:18 INFO CodeGenerator: Code generated in 16.297597 ms
26/06/06 08:53:18 INFO CodeGenerator: Code generated in 39.859455 ms
26/06/06 08:53:18 INFO CodeGenerator: Code generated in 65.409179 ms
26/06/06 08:53:18 INFO MemoryStore: Block broadcast_4 stored as values in memory (estimated size 303.9 KiB, free 365.3 MiB)
26/06/06 08:53:18 INFO MemoryStore: Block broadcast_4_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 365.3 MiB)
26/06/06 08:53:18 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 366.2 MiB)
26/06/06 08:53:18 INFO SparkContext: Created broadcast 4 from show at Main.scala:25
26/06/06 08:53:18 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:19 INFO SparkContext: Starting job: show at Main.scala:25
26/06/06 08:53:19 INFO DAGScheduler: Registering RDD 13 (show at Main.scala:25) as input to shuffle 0
26/06/06 08:53:19 INFO DAGScheduler: Got job 2 (show at Main.scala:25) with 200 output partitions
26/06/06 08:53:19 INFO DAGScheduler: Final stage: ResultStage 3 (show at Main.scala:25)
26/06/06 08:53:19 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
26/06/06 08:53:19 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 2)
26/06/06 08:53:19 INFO DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[13] at show at Main.scala:25), which has no missing parents
26/06/06 08:53:19 INFO MemoryStore: Block broadcast_5 stored as values in memory (estimated size 30.3 KiB, free 365.3 MiB)
26/06/06 08:53:19 INFO MemoryStore: Block broadcast_5_piece0 stored as bytes in memory (estimated size 14.2 KiB, free 365.2 MiB)
26/06/06 08:53:19 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on e323cc9233ad:45913 (size: 14.2 KiB, free: 366.2 MiB)
26/06/06 08:53:19 INFO SparkContext: Created broadcast 5 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:19 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[13] at show at Main.scala:25) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:19 INFO TaskSchedulerImpl: Adding task set 2.0 with 2 tasks resource profile 0
26/06/06 08:53:19 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 3) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:19 INFO BlockManagerInfo: Added broadcast_5_piece0 in memory on 172.18.0.5:41023 (size: 14.2 KiB, free: 93.2 MiB)
26/06/06 08:53:19 INFO BlockManagerInfo: Added broadcast_4_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 93.2 MiB)
26/06/06 08:53:20 INFO TaskSetManager: Starting task 1.0 in stage 2.0 (TID 4) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:20 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 3) in 945 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:20 INFO TaskSetManager: Finished task 1.0 in stage 2.0 (TID 4) in 164 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:20 INFO TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
26/06/06 08:53:20 INFO DAGScheduler: ShuffleMapStage 2 (show at Main.scala:25) finished in 1.130 s
26/06/06 08:53:20 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:20 INFO DAGScheduler: running: Set()
26/06/06 08:53:20 INFO DAGScheduler: waiting: Set(ResultStage 3)
26/06/06 08:53:20 INFO DAGScheduler: failed: Set()
26/06/06 08:53:20 INFO DAGScheduler: Submitting ResultStage 3 (MapPartitionsRDD[17] at show at Main.scala:25), which has no missing parents
26/06/06 08:53:20 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 34.4 KiB, free 365.2 MiB)
26/06/06 08:53:20 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 17.0 KiB, free 365.2 MiB)
26/06/06 08:53:20 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on e323cc9233ad:45913 (size: 17.0 KiB, free: 366.2 MiB)
26/06/06 08:53:20 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:20 INFO DAGScheduler: Submitting 200 missing tasks from ResultStage 3 (MapPartitionsRDD[17] at show at Main.scala:25) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:20 INFO TaskSchedulerImpl: Adding task set 3.0 with 200 tasks resource profile 0
26/06/06 08:53:20 INFO TaskSetManager: Starting task 40.0 in stage 3.0 (TID 5) (172.18.0.5, executor 0, partition 40, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:20 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on 172.18.0.5:41023 (size: 17.0 KiB, free: 93.2 MiB)
26/06/06 08:53:20 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.18.0.5:43486
26/06/06 08:53:20 INFO TaskSetManager: Starting task 71.0 in stage 3.0 (TID 6) (172.18.0.5, executor 0, partition 71, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:20 INFO TaskSetManager: Starting task 127.0 in stage 3.0 (TID 7) (172.18.0.5, executor 0, partition 127, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:20 INFO TaskSetManager: Finished task 40.0 in stage 3.0 (TID 5) in 453 ms on 172.18.0.5 (executor 0) (1/200)
26/06/06 08:53:20 INFO TaskSetManager: Finished task 71.0 in stage 3.0 (TID 6) in 86 ms on 172.18.0.5 (executor 0) (2/200)
<...>
26/06/06 08:53:25 INFO TaskSetManager: Finished task 198.0 in stage 3.0 (TID 203) in 18 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:25 INFO TaskSetManager: Finished task 199.0 in stage 3.0 (TID 204) in 15 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:25 INFO TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
26/06/06 08:53:25 INFO DAGScheduler: ResultStage 3 (show at Main.scala:25) finished in 5.640 s
26/06/06 08:53:25 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:25 INFO TaskSchedulerImpl: Killing all running tasks in stage 3: Stage finished
26/06/06 08:53:25 INFO DAGScheduler: Job 2 finished: show at Main.scala:25, took 6.834628 s
26/06/06 08:53:25 INFO CodeGenerator: Code generated in 34.030961 ms
26/06/06 08:53:25 INFO CodeGenerator: Code generated in 26.504273 ms
+-----------+------------+
|   category|total_profit|
+-----------+------------+
|      Bikes|    20519276|
|Accessories|     8862377|
|   Clothing|     2839447|
+-----------+------------+

=== Average Profit by Country ===
26/06/06 08:53:26 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:26 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:26 INFO FileSourceStrategy: Output Data Schema: struct<Country: string, Profit: int>
26/06/06 08:53:26 INFO CodeGenerator: Code generated in 34.507139 ms
26/06/06 08:53:26 INFO CodeGenerator: Code generated in 35.237186 ms
26/06/06 08:53:26 INFO CodeGenerator: Code generated in 57.986352 ms
26/06/06 08:53:26 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 303.9 KiB, free 364.9 MiB)
26/06/06 08:53:26 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 364.9 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 366.1 MiB)
26/06/06 08:53:26 INFO SparkContext: Created broadcast 7 from show at Main.scala:33
26/06/06 08:53:26 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:26 INFO SparkContext: Starting job: show at Main.scala:33
26/06/06 08:53:26 INFO DAGScheduler: Registering RDD 21 (show at Main.scala:33) as input to shuffle 1
26/06/06 08:53:26 INFO DAGScheduler: Got job 3 (show at Main.scala:33) with 200 output partitions
26/06/06 08:53:26 INFO DAGScheduler: Final stage: ResultStage 5 (show at Main.scala:33)
26/06/06 08:53:26 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 4)
26/06/06 08:53:26 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 4)
26/06/06 08:53:26 INFO DAGScheduler: Submitting ShuffleMapStage 4 (MapPartitionsRDD[21] at show at Main.scala:33), which has no missing parents
26/06/06 08:53:26 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 31.8 KiB, free 364.8 MiB)
26/06/06 08:53:26 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 14.9 KiB, free 364.8 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on e323cc9233ad:45913 (size: 14.9 KiB, free: 366.1 MiB)
26/06/06 08:53:26 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:26 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 4 (MapPartitionsRDD[21] at show at Main.scala:33) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:26 INFO TaskSchedulerImpl: Adding task set 4.0 with 2 tasks resource profile 0
26/06/06 08:53:26 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 205) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:26 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on 172.18.0.5:41023 (size: 14.9 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_4_piece0 on e323cc9233ad:45913 in memory (size: 27.6 KiB, free: 366.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_4_piece0 on 172.18.0.5:41023 in memory (size: 27.6 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_5_piece0 on e323cc9233ad:45913 in memory (size: 14.2 KiB, free: 366.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_5_piece0 on 172.18.0.5:41023 in memory (size: 14.2 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_3_piece0 on e323cc9233ad:45913 in memory (size: 8.0 KiB, free: 366.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_3_piece0 on 172.18.0.5:41023 in memory (size: 8.0 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_6_piece0 on e323cc9233ad:45913 in memory (size: 17.0 KiB, free: 366.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_6_piece0 on 172.18.0.5:41023 in memory (size: 17.0 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on e323cc9233ad:45913 in memory (size: 5.4 KiB, free: 366.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.18.0.5:41023 in memory (size: 5.4 KiB, free: 93.2 MiB)
26/06/06 08:53:26 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 93.2 MiB)
26/06/06 08:53:27 INFO TaskSetManager: Starting task 1.0 in stage 4.0 (TID 206) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:27 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 205) in 606 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:27 INFO TaskSetManager: Finished task 1.0 in stage 4.0 (TID 206) in 155 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:27 INFO TaskSchedulerImpl: Removed TaskSet 4.0, whose tasks have all completed, from pool 
26/06/06 08:53:27 INFO DAGScheduler: ShuffleMapStage 4 (show at Main.scala:33) finished in 0.825 s
26/06/06 08:53:27 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:27 INFO DAGScheduler: running: Set()
26/06/06 08:53:27 INFO DAGScheduler: waiting: Set(ResultStage 5)
26/06/06 08:53:27 INFO DAGScheduler: failed: Set()
26/06/06 08:53:27 INFO DAGScheduler: Submitting ResultStage 5 (MapPartitionsRDD[25] at show at Main.scala:33), which has no missing parents
26/06/06 08:53:27 INFO MemoryStore: Block broadcast_9 stored as values in memory (estimated size 35.8 KiB, free 365.2 MiB)
26/06/06 08:53:27 INFO MemoryStore: Block broadcast_9_piece0 stored as bytes in memory (estimated size 17.6 KiB, free 365.2 MiB)
26/06/06 08:53:27 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on e323cc9233ad:45913 (size: 17.6 KiB, free: 366.2 MiB)
26/06/06 08:53:27 INFO SparkContext: Created broadcast 9 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:27 INFO DAGScheduler: Submitting 200 missing tasks from ResultStage 5 (MapPartitionsRDD[25] at show at Main.scala:33) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:27 INFO TaskSchedulerImpl: Adding task set 5.0 with 200 tasks resource profile 0
26/06/06 08:53:27 INFO TaskSetManager: Starting task 22.0 in stage 5.0 (TID 207) (172.18.0.5, executor 0, partition 22, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:27 INFO BlockManagerInfo: Added broadcast_9_piece0 in memory on 172.18.0.5:41023 (size: 17.6 KiB, free: 93.2 MiB)
26/06/06 08:53:27 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.18.0.5:43486
26/06/06 08:53:27 INFO TaskSetManager: Starting task 30.0 in stage 5.0 (TID 208) (172.18.0.5, executor 0, partition 30, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:27 INFO TaskSetManager: Finished task 22.0 in stage 5.0 (TID 207) in 100 ms on 172.18.0.5 (executor 0) (1/200)
26/06/06 08:53:27 INFO TaskSetManager: Starting task 59.0 in stage 5.0 (TID 209) (172.18.0.5, executor 0, partition 59, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
<...>
26/06/06 08:53:30 INFO TaskSetManager: Starting task 199.0 in stage 5.0 (TID 406) (172.18.0.5, executor 0, partition 199, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:30 INFO TaskSetManager: Finished task 198.0 in stage 5.0 (TID 405) in 19 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:30 INFO TaskSetManager: Finished task 199.0 in stage 5.0 (TID 406) in 17 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:30 INFO TaskSchedulerImpl: Removed TaskSet 5.0, whose tasks have all completed, from pool 
26/06/06 08:53:30 INFO DAGScheduler: ResultStage 5 (show at Main.scala:33) finished in 2.955 s
26/06/06 08:53:30 INFO DAGScheduler: Job 3 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:30 INFO TaskSchedulerImpl: Killing all running tasks in stage 5: Stage finished
26/06/06 08:53:30 INFO DAGScheduler: Job 3 finished: show at Main.scala:33, took 3.815277 s
26/06/06 08:53:30 INFO CodeGenerator: Code generated in 19.838957 ms
+--------------+--------------------+
|       Country|avg_profit_per_order|
+--------------+--------------------+
|United Kingdom|   324.0714390602056|
|       Germany|   302.7568030275725|
|     Australia|   283.0894886363636|
| United States|   282.4476865785849|
|        Canada|    262.187614614191|
|        France|  261.89143480632845|
+--------------+--------------------+

=== Top 5 Products ===
26/06/06 08:53:30 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:30 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:30 INFO FileSourceStrategy: Output Data Schema: struct<Product: string, Order_Quantity: int>
26/06/06 08:53:30 INFO CodeGenerator: Code generated in 19.281686 ms
26/06/06 08:53:30 INFO MemoryStore: Block broadcast_10 stored as values in memory (estimated size 303.9 KiB, free 364.9 MiB)
26/06/06 08:53:30 INFO MemoryStore: Block broadcast_10_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 364.9 MiB)
26/06/06 08:53:30 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 366.2 MiB)
26/06/06 08:53:30 INFO SparkContext: Created broadcast 10 from show at Main.scala:42
26/06/06 08:53:30 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:30 INFO SparkContext: Starting job: show at Main.scala:42
26/06/06 08:53:30 INFO DAGScheduler: Registering RDD 29 (show at Main.scala:42) as input to shuffle 2
26/06/06 08:53:30 INFO DAGScheduler: Registering RDD 34 (show at Main.scala:42) as input to shuffle 3
26/06/06 08:53:30 INFO DAGScheduler: Got job 4 (show at Main.scala:42) with 1 output partitions
26/06/06 08:53:30 INFO DAGScheduler: Final stage: ResultStage 8 (show at Main.scala:42)
26/06/06 08:53:30 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 7)
26/06/06 08:53:30 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 7)
26/06/06 08:53:30 INFO DAGScheduler: Submitting ShuffleMapStage 6 (MapPartitionsRDD[29] at show at Main.scala:42), which has no missing parents
26/06/06 08:53:30 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 30.3 KiB, free 364.9 MiB)
26/06/06 08:53:30 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 14.2 KiB, free 364.9 MiB)
26/06/06 08:53:30 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on e323cc9233ad:45913 (size: 14.2 KiB, free: 366.1 MiB)
26/06/06 08:53:30 INFO SparkContext: Created broadcast 11 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:30 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 6 (MapPartitionsRDD[29] at show at Main.scala:42) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:30 INFO TaskSchedulerImpl: Adding task set 6.0 with 2 tasks resource profile 0
26/06/06 08:53:30 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 407) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:30 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on 172.18.0.5:41023 (size: 14.2 KiB, free: 93.2 MiB)
26/06/06 08:53:30 INFO BlockManagerInfo: Added broadcast_10_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 93.1 MiB)
26/06/06 08:53:31 INFO TaskSetManager: Starting task 1.0 in stage 6.0 (TID 408) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:31 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 407) in 417 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:31 INFO TaskSetManager: Finished task 1.0 in stage 6.0 (TID 408) in 195 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:31 INFO TaskSchedulerImpl: Removed TaskSet 6.0, whose tasks have all completed, from pool 
26/06/06 08:53:31 INFO DAGScheduler: ShuffleMapStage 6 (show at Main.scala:42) finished in 0.628 s
26/06/06 08:53:31 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:31 INFO DAGScheduler: running: Set()
26/06/06 08:53:31 INFO DAGScheduler: waiting: Set(ShuffleMapStage 7, ResultStage 8)
26/06/06 08:53:31 INFO DAGScheduler: failed: Set()
26/06/06 08:53:31 INFO DAGScheduler: Submitting ShuffleMapStage 7 (MapPartitionsRDD[34] at show at Main.scala:42), which has no missing parents
26/06/06 08:53:31 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 35.6 KiB, free 364.8 MiB)
26/06/06 08:53:31 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 17.6 KiB, free 364.8 MiB)
26/06/06 08:53:31 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on e323cc9233ad:45913 (size: 17.6 KiB, free: 366.1 MiB)
26/06/06 08:53:31 INFO SparkContext: Created broadcast 12 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:31 INFO DAGScheduler: Submitting 200 missing tasks from ShuffleMapStage 7 (MapPartitionsRDD[34] at show at Main.scala:42) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:31 INFO TaskSchedulerImpl: Adding task set 7.0 with 200 tasks resource profile 0
26/06/06 08:53:31 INFO TaskSetManager: Starting task 4.0 in stage 7.0 (TID 409) (172.18.0.5, executor 0, partition 4, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:31 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on 172.18.0.5:41023 (size: 17.6 KiB, free: 93.1 MiB)
26/06/06 08:53:31 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 172.18.0.5:43486
26/06/06 08:53:31 INFO TaskSetManager: Starting task 5.0 in stage 7.0 (TID 410) (172.18.0.5, executor 0, partition 5, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
<...>
26/06/06 08:53:34 INFO TaskSetManager: Finished task 198.0 in stage 7.0 (TID 608) in 10 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:34 INFO TaskSchedulerImpl: Removed TaskSet 7.0, whose tasks have all completed, from pool 
26/06/06 08:53:34 INFO DAGScheduler: ShuffleMapStage 7 (show at Main.scala:42) finished in 2.884 s
26/06/06 08:53:34 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:34 INFO DAGScheduler: running: Set()
26/06/06 08:53:34 INFO DAGScheduler: waiting: Set(ResultStage 8)
26/06/06 08:53:34 INFO DAGScheduler: failed: Set()
26/06/06 08:53:34 INFO DAGScheduler: Submitting ResultStage 8 (MapPartitionsRDD[38] at show at Main.scala:42), which has no missing parents
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 30.6 KiB, free 364.8 MiB)
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 15.4 KiB, free 364.8 MiB)
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on e323cc9233ad:45913 (size: 15.4 KiB, free: 366.1 MiB)
26/06/06 08:53:34 INFO SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:34 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 8 (MapPartitionsRDD[38] at show at Main.scala:42) (first 15 tasks are for partitions Vector(0))
26/06/06 08:53:34 INFO TaskSchedulerImpl: Adding task set 8.0 with 1 tasks resource profile 0
26/06/06 08:53:34 INFO TaskSetManager: Starting task 0.0 in stage 8.0 (TID 609) (172.18.0.5, executor 0, partition 0, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on 172.18.0.5:41023 (size: 15.4 KiB, free: 93.1 MiB)
26/06/06 08:53:34 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 3 to 172.18.0.5:43486
26/06/06 08:53:34 INFO TaskSetManager: Finished task 0.0 in stage 8.0 (TID 609) in 69 ms on 172.18.0.5 (executor 0) (1/1)
26/06/06 08:53:34 INFO TaskSchedulerImpl: Removed TaskSet 8.0, whose tasks have all completed, from pool 
26/06/06 08:53:34 INFO DAGScheduler: ResultStage 8 (show at Main.scala:42) finished in 0.079 s
26/06/06 08:53:34 INFO DAGScheduler: Job 4 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:34 INFO TaskSchedulerImpl: Killing all running tasks in stage 8: Stage finished
26/06/06 08:53:34 INFO DAGScheduler: Job 4 finished: show at Main.scala:42, took 3.632874 s
+--------------------+--------------+
|             Product|total_quantity|
+--------------------+--------------+
|Water Bottle - 30...|        164086|
| Patch Kit/8 Patches|        157583|
|  Mountain Tire Tube|        102792|
|        AWC Logo Cap|         67316|
|Sport-100 Helmet,...|         63663|
+--------------------+--------------+

26/06/06 08:53:34 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:34 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:34 INFO FileSourceStrategy: Output Data Schema: struct<category: string, Profit: int>
26/06/06 08:53:34 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/06/06 08:53:34 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/06/06 08:53:34 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
26/06/06 08:53:34 INFO CodeGenerator: Code generated in 20.784673 ms
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 303.9 KiB, free 364.5 MiB)
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 364.4 MiB)
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 366.1 MiB)
26/06/06 08:53:34 INFO SparkContext: Created broadcast 14 from csv at Main.scala:48
26/06/06 08:53:34 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:34 INFO CodeGenerator: Code generated in 10.152452 ms
26/06/06 08:53:34 INFO SparkContext: Starting job: csv at Main.scala:48
26/06/06 08:53:34 INFO DAGScheduler: Registering RDD 42 (csv at Main.scala:48) as input to shuffle 4
26/06/06 08:53:34 INFO DAGScheduler: Got job 5 (csv at Main.scala:48) with 200 output partitions
26/06/06 08:53:34 INFO DAGScheduler: Final stage: ResultStage 10 (csv at Main.scala:48)
26/06/06 08:53:34 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 9)
26/06/06 08:53:34 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 9)
26/06/06 08:53:34 INFO DAGScheduler: Submitting ShuffleMapStage 9 (MapPartitionsRDD[42] at csv at Main.scala:48), which has no missing parents
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_15 stored as values in memory (estimated size 30.3 KiB, free 364.4 MiB)
26/06/06 08:53:34 INFO MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 14.2 KiB, free 364.4 MiB)
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on e323cc9233ad:45913 (size: 14.2 KiB, free: 366.1 MiB)
26/06/06 08:53:34 INFO SparkContext: Created broadcast 15 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:34 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 9 (MapPartitionsRDD[42] at csv at Main.scala:48) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:34 INFO TaskSchedulerImpl: Adding task set 9.0 with 2 tasks resource profile 0
26/06/06 08:53:34 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 610) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_15_piece0 in memory on 172.18.0.5:41023 (size: 14.2 KiB, free: 93.1 MiB)
26/06/06 08:53:34 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 93.1 MiB)
26/06/06 08:53:35 INFO TaskSetManager: Starting task 1.0 in stage 9.0 (TID 611) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:35 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 610) in 256 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:35 INFO TaskSetManager: Finished task 1.0 in stage 9.0 (TID 611) in 120 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:35 INFO TaskSchedulerImpl: Removed TaskSet 9.0, whose tasks have all completed, from pool 
26/06/06 08:53:35 INFO DAGScheduler: ShuffleMapStage 9 (csv at Main.scala:48) finished in 0.386 s
26/06/06 08:53:35 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:35 INFO DAGScheduler: running: Set()
26/06/06 08:53:35 INFO DAGScheduler: waiting: Set(ResultStage 10)
26/06/06 08:53:35 INFO DAGScheduler: failed: Set()
26/06/06 08:53:35 INFO DAGScheduler: Submitting ResultStage 10 (MapPartitionsRDD[47] at csv at Main.scala:48), which has no missing parents
26/06/06 08:53:35 INFO MemoryStore: Block broadcast_16 stored as values in memory (estimated size 34.0 KiB, free 364.4 MiB)
26/06/06 08:53:35 INFO MemoryStore: Block broadcast_16_piece0 stored as bytes in memory (estimated size 16.9 KiB, free 364.3 MiB)
26/06/06 08:53:35 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on e323cc9233ad:45913 (size: 16.9 KiB, free: 366.1 MiB)
26/06/06 08:53:35 INFO SparkContext: Created broadcast 16 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:35 INFO DAGScheduler: Submitting 200 missing tasks from ResultStage 10 (MapPartitionsRDD[47] at csv at Main.scala:48) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:35 INFO TaskSchedulerImpl: Adding task set 10.0 with 200 tasks resource profile 0
26/06/06 08:53:35 INFO TaskSetManager: Starting task 40.0 in stage 10.0 (TID 612) (172.18.0.5, executor 0, partition 40, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:35 INFO BlockManagerInfo: Added broadcast_16_piece0 in memory on 172.18.0.5:41023 (size: 16.9 KiB, free: 93.1 MiB)
26/06/06 08:53:35 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 4 to 172.18.0.5:43486
26/06/06 08:53:35 INFO TaskSetManager: Starting task 71.0 in stage 10.0 (TID 613) (172.18.0.5, executor 0, partition 71, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
<...>taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 194.0 in stage 10.0 (TID 806) in 89 ms on 172.18.0.5 (executor 0) (195/200)
26/06/06 08:53:37 INFO TaskSetManager: Starting task 196.0 in stage 10.0 (TID 808) (172.18.0.5, executor 0, partition 196, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 195.0 in stage 10.0 (TID 807) in 44 ms on 172.18.0.5 (executor 0) (196/200)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_12_piece0 on e323cc9233ad:45913 in memory (size: 17.6 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_12_piece0 on 172.18.0.5:41023 in memory (size: 17.6 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO TaskSetManager: Starting task 197.0 in stage 10.0 (TID 809) (172.18.0.5, executor 0, partition 197, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 196.0 in stage 10.0 (TID 808) in 17 ms on 172.18.0.5 (executor 0) (197/200)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_13_piece0 on 172.18.0.5:41023 in memory (size: 15.4 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_13_piece0 on e323cc9233ad:45913 in memory (size: 15.4 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO TaskSetManager: Starting task 198.0 in stage 10.0 (TID 810) (172.18.0.5, executor 0, partition 198, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 197.0 in stage 10.0 (TID 809) in 17 ms on 172.18.0.5 (executor 0) (198/200)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_15_piece0 on e323cc9233ad:45913 in memory (size: 14.2 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_15_piece0 on 172.18.0.5:41023 in memory (size: 14.2 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO TaskSetManager: Starting task 199.0 in stage 10.0 (TID 811) (172.18.0.5, executor 0, partition 199, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 198.0 in stage 10.0 (TID 810) in 12 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_11_piece0 on e323cc9233ad:45913 in memory (size: 14.2 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO TaskSetManager: Finished task 199.0 in stage 10.0 (TID 811) in 13 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:37 INFO TaskSchedulerImpl: Removed TaskSet 10.0, whose tasks have all completed, from pool 
26/06/06 08:53:37 INFO DAGScheduler: ResultStage 10 (csv at Main.scala:48) finished in 2.091 s
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_11_piece0 on 172.18.0.5:41023 in memory (size: 14.2 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:37 INFO TaskSchedulerImpl: Killing all running tasks in stage 10: Stage finished
26/06/06 08:53:37 INFO DAGScheduler: Job 5 finished: csv at Main.scala:48, took 2.493643 s
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_8_piece0 on e323cc9233ad:45913 in memory (size: 14.9 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_8_piece0 on 172.18.0.5:41023 in memory (size: 14.9 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_9_piece0 on e323cc9233ad:45913 in memory (size: 17.6 KiB, free: 366.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_9_piece0 on 172.18.0.5:41023 in memory (size: 17.6 KiB, free: 93.1 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_10_piece0 on e323cc9233ad:45913 in memory (size: 27.6 KiB, free: 366.2 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Removed broadcast_10_piece0 on 172.18.0.5:41023 in memory (size: 27.6 KiB, free: 93.2 MiB)
26/06/06 08:53:37 INFO SparkContext: Starting job: csv at Main.scala:48
26/06/06 08:53:37 INFO DAGScheduler: Registering RDD 48 (csv at Main.scala:48) as input to shuffle 5
26/06/06 08:53:37 INFO DAGScheduler: Got job 6 (csv at Main.scala:48) with 3 output partitions
26/06/06 08:53:37 INFO DAGScheduler: Final stage: ResultStage 13 (csv at Main.scala:48)
26/06/06 08:53:37 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 12)
26/06/06 08:53:37 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 12)
26/06/06 08:53:37 INFO DAGScheduler: Submitting ShuffleMapStage 12 (MapPartitionsRDD[48] at csv at Main.scala:48), which has no missing parents
26/06/06 08:53:37 INFO MemoryStore: Block broadcast_17 stored as values in memory (estimated size 34.4 KiB, free 364.9 MiB)
26/06/06 08:53:37 INFO MemoryStore: Block broadcast_17_piece0 stored as bytes in memory (estimated size 17.3 KiB, free 364.9 MiB)
26/06/06 08:53:37 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on e323cc9233ad:45913 (size: 17.3 KiB, free: 366.2 MiB)
26/06/06 08:53:37 INFO SparkContext: Created broadcast 17 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:37 INFO DAGScheduler: Submitting 200 missing tasks from ShuffleMapStage 12 (MapPartitionsRDD[48] at csv at Main.scala:48) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:37 INFO TaskSchedulerImpl: Adding task set 12.0 with 200 tasks resource profile 0
26/06/06 08:53:37 INFO TaskSetManager: Starting task 40.0 in stage 12.0 (TID 812) (172.18.0.5, executor 0, partition 40, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO BlockManagerInfo: Added broadcast_17_piece0 in memory on 172.18.0.5:41023 (size: 17.3 KiB, free: 93.2 MiB)
26/06/06 08:53:37 INFO TaskSetManager: Starting task 71.0 in stage 12.0 (TID 813) (172.18.0.5, executor 0, partition 71, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:37 INFO TaskSetManager: Finished task 40.0 in stage 12.0 (TID 812) in 40 ms on 172.18.0.5 (executor 0) (1/200)
<...>
26/06/06 08:53:39 INFO TaskSetManager: Finished task 197.0 in stage 12.0 (TID 1009) in 11 ms on 172.18.0.5 (executor 0) (198/200)
26/06/06 08:53:39 INFO TaskSetManager: Starting task 199.0 in stage 12.0 (TID 1011) (172.18.0.5, executor 0, partition 199, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:39 INFO TaskSetManager: Finished task 198.0 in stage 12.0 (TID 1010) in 8 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:39 INFO TaskSetManager: Finished task 199.0 in stage 12.0 (TID 1011) in 8 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:39 INFO TaskSchedulerImpl: Removed TaskSet 12.0, whose tasks have all completed, from pool 
26/06/06 08:53:39 INFO DAGScheduler: ShuffleMapStage 12 (csv at Main.scala:48) finished in 2.169 s
26/06/06 08:53:39 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:39 INFO DAGScheduler: running: Set()
26/06/06 08:53:39 INFO DAGScheduler: waiting: Set(ResultStage 13)
26/06/06 08:53:39 INFO DAGScheduler: failed: Set()
26/06/06 08:53:39 INFO DAGScheduler: Submitting ResultStage 13 (MapPartitionsRDD[50] at csv at Main.scala:48), which has no missing parents
26/06/06 08:53:39 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 192.3 KiB, free 364.7 MiB)
26/06/06 08:53:39 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 70.9 KiB, free 364.6 MiB)
26/06/06 08:53:39 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on e323cc9233ad:45913 (size: 70.9 KiB, free: 366.1 MiB)
26/06/06 08:53:39 INFO SparkContext: Created broadcast 18 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:39 INFO DAGScheduler: Submitting 3 missing tasks from ResultStage 13 (MapPartitionsRDD[50] at csv at Main.scala:48) (first 15 tasks are for partitions Vector(0, 1, 2))
26/06/06 08:53:39 INFO TaskSchedulerImpl: Adding task set 13.0 with 3 tasks resource profile 0
26/06/06 08:53:39 INFO TaskSetManager: Starting task 0.0 in stage 13.0 (TID 1012) (172.18.0.5, executor 0, partition 0, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:39 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on 172.18.0.5:41023 (size: 70.9 KiB, free: 93.1 MiB)
26/06/06 08:53:39 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 5 to 172.18.0.5:43486
26/06/06 08:53:39 INFO TaskSetManager: Starting task 1.0 in stage 13.0 (TID 1013) (172.18.0.5, executor 0, partition 1, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:39 INFO TaskSetManager: Finished task 0.0 in stage 13.0 (TID 1012) in 275 ms on 172.18.0.5 (executor 0) (1/3)
26/06/06 08:53:39 INFO TaskSetManager: Starting task 2.0 in stage 13.0 (TID 1014) (172.18.0.5, executor 0, partition 2, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:39 INFO TaskSetManager: Finished task 1.0 in stage 13.0 (TID 1013) in 81 ms on 172.18.0.5 (executor 0) (2/3)
26/06/06 08:53:40 INFO TaskSetManager: Finished task 2.0 in stage 13.0 (TID 1014) in 488 ms on 172.18.0.5 (executor 0) (3/3)
26/06/06 08:53:40 INFO TaskSchedulerImpl: Removed TaskSet 13.0, whose tasks have all completed, from pool 
26/06/06 08:53:40 INFO DAGScheduler: ResultStage 13 (csv at Main.scala:48) finished in 0.864 s
26/06/06 08:53:40 INFO DAGScheduler: Job 6 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:40 INFO TaskSchedulerImpl: Killing all running tasks in stage 13: Stage finished
26/06/06 08:53:40 INFO DAGScheduler: Job 6 finished: csv at Main.scala:48, took 3.047322 s
26/06/06 08:53:40 INFO FileFormatWriter: Write Job 7b546c4e-5f5e-4939-87f2-5917c492b717 committed.
26/06/06 08:53:40 INFO FileFormatWriter: Finished processing stats for write job 7b546c4e-5f5e-4939-87f2-5917c492b717.
26/06/06 08:53:40 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:40 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:40 INFO FileSourceStrategy: Output Data Schema: struct<Country: string, Profit: int>
26/06/06 08:53:40 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/06/06 08:53:40 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/06/06 08:53:40 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
26/06/06 08:53:40 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 303.9 KiB, free 364.4 MiB)
26/06/06 08:53:40 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 364.3 MiB)
26/06/06 08:53:40 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 366.1 MiB)
26/06/06 08:53:40 INFO SparkContext: Created broadcast 19 from csv at Main.scala:53
26/06/06 08:53:40 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:40 INFO CodeGenerator: Code generated in 14.349261 ms
26/06/06 08:53:40 INFO SparkContext: Starting job: csv at Main.scala:53
26/06/06 08:53:40 INFO DAGScheduler: Registering RDD 56 (csv at Main.scala:53) as input to shuffle 6
26/06/06 08:53:40 INFO DAGScheduler: Got job 7 (csv at Main.scala:53) with 200 output partitions
26/06/06 08:53:40 INFO DAGScheduler: Final stage: ResultStage 15 (csv at Main.scala:53)
26/06/06 08:53:40 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 14)
26/06/06 08:53:40 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 14)
26/06/06 08:53:40 INFO DAGScheduler: Submitting ShuffleMapStage 14 (MapPartitionsRDD[56] at csv at Main.scala:53), which has no missing parents
26/06/06 08:53:40 INFO MemoryStore: Block broadcast_20 stored as values in memory (estimated size 31.8 KiB, free 364.3 MiB)
26/06/06 08:53:40 INFO MemoryStore: Block broadcast_20_piece0 stored as bytes in memory (estimated size 14.9 KiB, free 364.3 MiB)
26/06/06 08:53:40 INFO BlockManagerInfo: Added broadcast_20_piece0 in memory on e323cc9233ad:45913 (size: 14.9 KiB, free: 366.0 MiB)
26/06/06 08:53:40 INFO SparkContext: Created broadcast 20 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:40 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 14 (MapPartitionsRDD[56] at csv at Main.scala:53) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:40 INFO TaskSchedulerImpl: Adding task set 14.0 with 2 tasks resource profile 0
26/06/06 08:53:40 INFO TaskSetManager: Starting task 0.0 in stage 14.0 (TID 1015) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:40 INFO BlockManagerInfo: Added broadcast_20_piece0 in memory on 172.18.0.5:41023 (size: 14.9 KiB, free: 93.1 MiB)
26/06/06 08:53:40 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 93.0 MiB)
26/06/06 08:53:40 INFO TaskSetManager: Starting task 1.0 in stage 14.0 (TID 1016) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:40 INFO TaskSetManager: Finished task 0.0 in stage 14.0 (TID 1015) in 249 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:41 INFO TaskSetManager: Finished task 1.0 in stage 14.0 (TID 1016) in 126 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:41 INFO TaskSchedulerImpl: Removed TaskSet 14.0, whose tasks have all completed, from pool 
26/06/06 08:53:41 INFO DAGScheduler: ShuffleMapStage 14 (csv at Main.scala:53) finished in 0.384 s
26/06/06 08:53:41 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:41 INFO DAGScheduler: running: Set()
26/06/06 08:53:41 INFO DAGScheduler: waiting: Set(ResultStage 15)
26/06/06 08:53:41 INFO DAGScheduler: failed: Set()
26/06/06 08:53:41 INFO DAGScheduler: Submitting ResultStage 15 (MapPartitionsRDD[61] at csv at Main.scala:53), which has no missing parents
26/06/06 08:53:41 INFO MemoryStore: Block broadcast_21 stored as values in memory (estimated size 35.4 KiB, free 364.2 MiB)
26/06/06 08:53:41 INFO MemoryStore: Block broadcast_21_piece0 stored as bytes in memory (estimated size 17.5 KiB, free 364.2 MiB)
26/06/06 08:53:41 INFO BlockManagerInfo: Added broadcast_21_piece0 in memory on e323cc9233ad:45913 (size: 17.5 KiB, free: 366.0 MiB)
26/06/06 08:53:41 INFO SparkContext: Created broadcast 21 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:41 INFO DAGScheduler: Submitting 200 missing tasks from ResultStage 15 (MapPartitionsRDD[61] at csv at Main.scala:53) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:41 INFO TaskSchedulerImpl: Adding task set 15.0 with 200 tasks resource profile 0
26/06/06 08:53:41 INFO TaskSetManager: Starting task 22.0 in stage 15.0 (TID 1017) (172.18.0.5, executor 0, partition 22, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:41 INFO BlockManagerInfo: Added broadcast_21_piece0 in memory on 172.18.0.5:41023 (size: 17.5 KiB, free: 93.0 MiB)
26/06/06 08:53:41 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 6 to 172.18.0.5:43486
26/06/06 08:53:41 INFO TaskSetManager: Starting task 30.0 in stage 15.0 (TID 1018) (172.18.0.5, executor 0, partition 30, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
<...>taskResourceAssignments Map()
26/06/06 08:53:43 INFO TaskSetManager: Finished task 197.0 in stage 15.0 (TID 1214) in 10 ms on 172.18.0.5 (executor 0) (198/200)
26/06/06 08:53:43 INFO TaskSetManager: Starting task 199.0 in stage 15.0 (TID 1216) (172.18.0.5, executor 0, partition 199, PROCESS_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:43 INFO TaskSetManager: Finished task 198.0 in stage 15.0 (TID 1215) in 8 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:43 INFO TaskSetManager: Finished task 199.0 in stage 15.0 (TID 1216) in 9 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:43 INFO TaskSchedulerImpl: Removed TaskSet 15.0, whose tasks have all completed, from pool 
26/06/06 08:53:43 INFO DAGScheduler: ResultStage 15 (csv at Main.scala:53) finished in 1.969 s
26/06/06 08:53:43 INFO DAGScheduler: Job 7 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:43 INFO TaskSchedulerImpl: Killing all running tasks in stage 15: Stage finished
26/06/06 08:53:43 INFO DAGScheduler: Job 7 finished: csv at Main.scala:53, took 2.364518 s
26/06/06 08:53:43 INFO SparkContext: Starting job: csv at Main.scala:53
26/06/06 08:53:43 INFO DAGScheduler: Registering RDD 62 (csv at Main.scala:53) as input to shuffle 7
26/06/06 08:53:43 INFO DAGScheduler: Got job 8 (csv at Main.scala:53) with 6 output partitions
26/06/06 08:53:43 INFO DAGScheduler: Final stage: ResultStage 18 (csv at Main.scala:53)
26/06/06 08:53:43 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 17)
26/06/06 08:53:43 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 17)
26/06/06 08:53:43 INFO DAGScheduler: Submitting ShuffleMapStage 17 (MapPartitionsRDD[62] at csv at Main.scala:53), which has no missing parents
26/06/06 08:53:43 INFO MemoryStore: Block broadcast_22 stored as values in memory (estimated size 35.9 KiB, free 364.2 MiB)
26/06/06 08:53:43 INFO MemoryStore: Block broadcast_22_piece0 stored as bytes in memory (estimated size 17.9 KiB, free 364.2 MiB)
26/06/06 08:53:43 INFO BlockManagerInfo: Added broadcast_22_piece0 in memory on e323cc9233ad:45913 (size: 17.9 KiB, free: 366.0 MiB)
26/06/06 08:53:43 INFO SparkContext: Created broadcast 22 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:43 INFO DAGScheduler: Submitting 200 missing tasks from ShuffleMapStage 17 (MapPartitionsRDD[62] at csv at Main.scala:53) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:43 INFO TaskSchedulerImpl: Adding task set 17.0 with 200 tasks resource profile 0
26/06/06 08:53:43 INFO TaskSetManager: Starting task 22.0 in stage 17.0 (TID 1217) (172.18.0.5, executor 0, partition 22, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:43 INFO BlockManagerInfo: Added broadcast_22_piece0 in memory on 172.18.0.5:41023 (size: 17.9 KiB, free: 93.0 MiB)
26/06/06 08:53:43 INFO TaskSetManager: Starting task 30.0 in stage 17.0 (TID 1218) (172.18.0.5, executor 0, partition 30, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
<...>
26/06/06 08:53:45 INFO TaskSetManager: Finished task 197.0 in stage 17.0 (TID 1414) in 15 ms on 172.18.0.5 (executor 0) (198/200)
26/06/06 08:53:45 INFO TaskSetManager: Starting task 199.0 in stage 17.0 (TID 1416) (172.18.0.5, executor 0, partition 199, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:45 INFO TaskSetManager: Finished task 198.0 in stage 17.0 (TID 1415) in 15 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:45 INFO TaskSetManager: Finished task 199.0 in stage 17.0 (TID 1416) in 14 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:45 INFO TaskSchedulerImpl: Removed TaskSet 17.0, whose tasks have all completed, from pool 
26/06/06 08:53:45 INFO DAGScheduler: ShuffleMapStage 17 (csv at Main.scala:53) finished in 2.342 s
26/06/06 08:53:45 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:45 INFO DAGScheduler: running: Set()
26/06/06 08:53:45 INFO DAGScheduler: waiting: Set(ResultStage 18)
26/06/06 08:53:45 INFO DAGScheduler: failed: Set()
26/06/06 08:53:45 INFO DAGScheduler: Submitting ResultStage 18 (MapPartitionsRDD[64] at csv at Main.scala:53), which has no missing parents
26/06/06 08:53:45 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 193.1 KiB, free 364.0 MiB)
26/06/06 08:53:45 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 71.3 KiB, free 363.9 MiB)
26/06/06 08:53:45 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on e323cc9233ad:45913 (size: 71.3 KiB, free: 365.9 MiB)
26/06/06 08:53:45 INFO SparkContext: Created broadcast 23 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:45 INFO DAGScheduler: Submitting 6 missing tasks from ResultStage 18 (MapPartitionsRDD[64] at csv at Main.scala:53) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5))
26/06/06 08:53:45 INFO TaskSchedulerImpl: Adding task set 18.0 with 6 tasks resource profile 0
26/06/06 08:53:45 INFO TaskSetManager: Starting task 0.0 in stage 18.0 (TID 1417) (172.18.0.5, executor 0, partition 0, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:45 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on 172.18.0.5:41023 (size: 71.3 KiB, free: 92.9 MiB)
26/06/06 08:53:45 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 7 to 172.18.0.5:43486
26/06/06 08:53:45 INFO TaskSetManager: Starting task 1.0 in stage 18.0 (TID 1418) (172.18.0.5, executor 0, partition 1, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:45 INFO TaskSetManager: Finished task 0.0 in stage 18.0 (TID 1417) in 139 ms on 172.18.0.5 (executor 0) (1/6)
26/06/06 08:53:46 INFO TaskSetManager: Starting task 2.0 in stage 18.0 (TID 1419) (172.18.0.5, executor 0, partition 2, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:46 INFO TaskSetManager: Finished task 1.0 in stage 18.0 (TID 1418) in 523 ms on 172.18.0.5 (executor 0) (2/6)
26/06/06 08:53:46 INFO TaskSetManager: Starting task 3.0 in stage 18.0 (TID 1420) (172.18.0.5, executor 0, partition 3, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:46 INFO TaskSetManager: Finished task 2.0 in stage 18.0 (TID 1419) in 487 ms on 172.18.0.5 (executor 0) (3/6)
26/06/06 08:53:47 INFO TaskSetManager: Starting task 4.0 in stage 18.0 (TID 1421) (172.18.0.5, executor 0, partition 4, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:47 INFO TaskSetManager: Finished task 3.0 in stage 18.0 (TID 1420) in 477 ms on 172.18.0.5 (executor 0) (4/6)
26/06/06 08:53:47 INFO TaskSetManager: Starting task 5.0 in stage 18.0 (TID 1422) (172.18.0.5, executor 0, partition 5, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:47 INFO TaskSetManager: Finished task 4.0 in stage 18.0 (TID 1421) in 487 ms on 172.18.0.5 (executor 0) (5/6)
26/06/06 08:53:48 INFO TaskSetManager: Finished task 5.0 in stage 18.0 (TID 1422) in 478 ms on 172.18.0.5 (executor 0) (6/6)
26/06/06 08:53:48 INFO TaskSchedulerImpl: Removed TaskSet 18.0, whose tasks have all completed, from pool 
26/06/06 08:53:48 INFO DAGScheduler: ResultStage 18 (csv at Main.scala:53) finished in 2.620 s
26/06/06 08:53:48 INFO DAGScheduler: Job 8 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:48 INFO TaskSchedulerImpl: Killing all running tasks in stage 18: Stage finished
26/06/06 08:53:48 INFO DAGScheduler: Job 8 finished: csv at Main.scala:53, took 4.978067 s
26/06/06 08:53:48 INFO FileFormatWriter: Write Job 31de40a7-0e63-492c-b01c-7eddba274a4f committed.
26/06/06 08:53:48 INFO FileFormatWriter: Finished processing stats for write job 31de40a7-0e63-492c-b01c-7eddba274a4f.
26/06/06 08:53:48 INFO FileSourceStrategy: Pushed Filters: 
26/06/06 08:53:48 INFO FileSourceStrategy: Post-Scan Filters: 
26/06/06 08:53:48 INFO FileSourceStrategy: Output Data Schema: struct<Product: string, Order_Quantity: int>
26/06/06 08:53:48 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
26/06/06 08:53:48 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
26/06/06 08:53:48 INFO SQLHadoopMapReduceCommitProtocol: Using output committer class org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_24 stored as values in memory (estimated size 303.9 KiB, free 363.6 MiB)
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_24_piece0 stored as bytes in memory (estimated size 27.6 KiB, free 363.6 MiB)
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on e323cc9233ad:45913 (size: 27.6 KiB, free: 365.9 MiB)
26/06/06 08:53:48 INFO SparkContext: Created broadcast 24 from csv at Main.scala:58
26/06/06 08:53:48 INFO FileSourceScanExec: Planning scan with bin packing, max size: 9660770 bytes, open cost is considered as scanning 4194304 bytes.
26/06/06 08:53:48 INFO SparkContext: Starting job: csv at Main.scala:58
26/06/06 08:53:48 INFO DAGScheduler: Registering RDD 70 (csv at Main.scala:58) as input to shuffle 8
26/06/06 08:53:48 INFO DAGScheduler: Registering RDD 75 (csv at Main.scala:58) as input to shuffle 9
26/06/06 08:53:48 INFO DAGScheduler: Got job 9 (csv at Main.scala:58) with 1 output partitions
26/06/06 08:53:48 INFO DAGScheduler: Final stage: ResultStage 21 (csv at Main.scala:58)
26/06/06 08:53:48 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 20)
26/06/06 08:53:48 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 20)
26/06/06 08:53:48 INFO DAGScheduler: Submitting ShuffleMapStage 19 (MapPartitionsRDD[70] at csv at Main.scala:58), which has no missing parents
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_25 stored as values in memory (estimated size 30.3 KiB, free 363.6 MiB)
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_25_piece0 stored as bytes in memory (estimated size 14.2 KiB, free 363.5 MiB)
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_25_piece0 in memory on e323cc9233ad:45913 (size: 14.2 KiB, free: 365.9 MiB)
26/06/06 08:53:48 INFO SparkContext: Created broadcast 25 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:48 INFO DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 19 (MapPartitionsRDD[70] at csv at Main.scala:58) (first 15 tasks are for partitions Vector(0, 1))
26/06/06 08:53:48 INFO TaskSchedulerImpl: Adding task set 19.0 with 2 tasks resource profile 0
26/06/06 08:53:48 INFO TaskSetManager: Starting task 0.0 in stage 19.0 (TID 1423) (172.18.0.5, executor 0, partition 0, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_25_piece0 in memory on 172.18.0.5:41023 (size: 14.2 KiB, free: 92.9 MiB)
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on 172.18.0.5:41023 (size: 27.6 KiB, free: 92.9 MiB)
26/06/06 08:53:48 INFO TaskSetManager: Starting task 1.0 in stage 19.0 (TID 1424) (172.18.0.5, executor 0, partition 1, ANY, 4858 bytes) taskResourceAssignments Map()
26/06/06 08:53:48 INFO TaskSetManager: Finished task 0.0 in stage 19.0 (TID 1423) in 344 ms on 172.18.0.5 (executor 0) (1/2)
26/06/06 08:53:48 INFO TaskSetManager: Finished task 1.0 in stage 19.0 (TID 1424) in 220 ms on 172.18.0.5 (executor 0) (2/2)
26/06/06 08:53:48 INFO TaskSchedulerImpl: Removed TaskSet 19.0, whose tasks have all completed, from pool 
26/06/06 08:53:48 INFO DAGScheduler: ShuffleMapStage 19 (csv at Main.scala:58) finished in 0.578 s
26/06/06 08:53:48 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:48 INFO DAGScheduler: running: Set()
26/06/06 08:53:48 INFO DAGScheduler: waiting: Set(ShuffleMapStage 20, ResultStage 21)
26/06/06 08:53:48 INFO DAGScheduler: failed: Set()
26/06/06 08:53:48 INFO DAGScheduler: Submitting ShuffleMapStage 20 (MapPartitionsRDD[75] at csv at Main.scala:58), which has no missing parents
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_26 stored as values in memory (estimated size 35.6 KiB, free 363.5 MiB)
26/06/06 08:53:48 INFO MemoryStore: Block broadcast_26_piece0 stored as bytes in memory (estimated size 17.6 KiB, free 363.5 MiB)
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_26_piece0 in memory on e323cc9233ad:45913 (size: 17.6 KiB, free: 365.9 MiB)
26/06/06 08:53:48 INFO SparkContext: Created broadcast 26 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:48 INFO DAGScheduler: Submitting 200 missing tasks from ShuffleMapStage 20 (MapPartitionsRDD[75] at csv at Main.scala:58) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
26/06/06 08:53:48 INFO TaskSchedulerImpl: Adding task set 20.0 with 200 tasks resource profile 0
26/06/06 08:53:48 INFO TaskSetManager: Starting task 4.0 in stage 20.0 (TID 1425) (172.18.0.5, executor 0, partition 4, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:48 INFO BlockManagerInfo: Added broadcast_26_piece0 in memory on 172.18.0.5:41023 (size: 17.6 KiB, free: 92.9 MiB)
26/06/06 08:53:48 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 8 to 172.18.0.5:43486
26/06/06 08:53:48 INFO TaskSetManager: Starting task 5.0 in stage 20.0 (TID 1426) (172.18.0.5, executor 0, partition 5, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:48 INFO TaskSetManager: Finished task 4.0 in stage 20.0 (TID 1425) in 24 ms on 172.18.0.5 (executor 0) (1/200)
26/06/06 08:53:49 INFO TaskSetManager: Starting task 6.0 in stage 20.0 (TID 1427) (172.18.0.5, executor 0, partition 6, NODE_LOCAL, 4446 bytes) taskResourceAssignments Map()
<...>
26/06/06 08:53:51 INFO TaskSetManager: Finished task 195.0 in stage 20.0 (TID 1622) in 12 ms on 172.18.0.5 (executor 0) (198/200)
26/06/06 08:53:51 INFO TaskSetManager: Starting task 198.0 in stage 20.0 (TID 1624) (172.18.0.5, executor 0, partition 198, PROCESS_LOCAL, 4446 bytes) taskResourceAssignments Map()
26/06/06 08:53:51 INFO TaskSetManager: Finished task 196.0 in stage 20.0 (TID 1623) in 9 ms on 172.18.0.5 (executor 0) (199/200)
26/06/06 08:53:51 INFO TaskSetManager: Finished task 198.0 in stage 20.0 (TID 1624) in 11 ms on 172.18.0.5 (executor 0) (200/200)
26/06/06 08:53:51 INFO TaskSchedulerImpl: Removed TaskSet 20.0, whose tasks have all completed, from pool 
26/06/06 08:53:51 INFO DAGScheduler: ShuffleMapStage 20 (csv at Main.scala:58) finished in 2.427 s
26/06/06 08:53:51 INFO DAGScheduler: looking for newly runnable stages
26/06/06 08:53:51 INFO DAGScheduler: running: Set()
26/06/06 08:53:51 INFO DAGScheduler: waiting: Set(ResultStage 21)
26/06/06 08:53:51 INFO DAGScheduler: failed: Set()
26/06/06 08:53:51 INFO DAGScheduler: Submitting ResultStage 21 (MapPartitionsRDD[77] at csv at Main.scala:58), which has no missing parents
26/06/06 08:53:51 INFO MemoryStore: Block broadcast_27 stored as values in memory (estimated size 190.1 KiB, free 364.4 MiB)
26/06/06 08:53:51 INFO MemoryStore: Block broadcast_27_piece0 stored as bytes in memory (estimated size 70.0 KiB, free 364.3 MiB)
26/06/06 08:53:51 INFO BlockManagerInfo: Added broadcast_27_piece0 in memory on e323cc9233ad:45913 (size: 70.0 KiB, free: 366.1 MiB)
26/06/06 08:53:51 INFO SparkContext: Created broadcast 27 from broadcast at DAGScheduler.scala:1388
26/06/06 08:53:51 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 21 (MapPartitionsRDD[77] at csv at Main.scala:58) (first 15 tasks are for partitions Vector(0))
26/06/06 08:53:51 INFO TaskSchedulerImpl: Adding task set 21.0 with 1 tasks resource profile 0
26/06/06 08:53:51 INFO TaskSetManager: Starting task 0.0 in stage 21.0 (TID 1625) (172.18.0.5, executor 0, partition 0, NODE_LOCAL, 4457 bytes) taskResourceAssignments Map()
26/06/06 08:53:51 INFO BlockManagerInfo: Added broadcast_27_piece0 in memory on 172.18.0.5:41023 (size: 70.0 KiB, free: 93.1 MiB)
26/06/06 08:53:51 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 9 to 172.18.0.5:43486
26/06/06 08:53:51 INFO TaskSetManager: Finished task 0.0 in stage 21.0 (TID 1625) in 102 ms on 172.18.0.5 (executor 0) (1/1)
26/06/06 08:53:51 INFO TaskSchedulerImpl: Removed TaskSet 21.0, whose tasks have all completed, from pool 
26/06/06 08:53:51 INFO DAGScheduler: ResultStage 21 (csv at Main.scala:58) finished in 0.132 s
26/06/06 08:53:51 INFO DAGScheduler: Job 9 is finished. Cancelling potential speculative or zombie tasks for this job
26/06/06 08:53:51 INFO TaskSchedulerImpl: Killing all running tasks in stage 21: Stage finished
26/06/06 08:53:51 INFO DAGScheduler: Job 9 finished: csv at Main.scala:58, took 3.158131 s
26/06/06 08:53:51 INFO FileFormatWriter: Write Job d986ecfd-4c55-4742-a08d-55dafcd7642a committed.
26/06/06 08:53:51 INFO FileFormatWriter: Finished processing stats for write job d986ecfd-4c55-4742-a08d-55dafcd7642a.
26/06/06 08:53:51 INFO SparkUI: Stopped Spark web UI at http://localhost:4040
26/06/06 08:53:51 INFO StandaloneSchedulerBackend: Shutting down all executors
26/06/06 08:53:51 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
26/06/06 08:53:51 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
26/06/06 08:53:51 INFO MemoryStore: MemoryStore cleared
26/06/06 08:53:51 INFO BlockManager: BlockManager stopped
26/06/06 08:53:51 INFO BlockManagerMaster: BlockManagerMaster stopped
26/06/06 08:53:51 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
26/06/06 08:53:51 INFO SparkContext: Successfully stopped SparkContext
26/06/06 08:53:51 INFO ShutdownHookManager: Shutdown hook called
26/06/06 08:53:51 INFO ShutdownHookManager: Deleting directory /tmp/spark-adefbd82-eda4-4a68-b5cb-806c15b9d60c
26/06/06 08:53:51 INFO ShutdownHookManager: Deleting directory /tmp/spark-36345859-a93c-42d6-b170-fcdb5bac911d

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug spark-master
    Learn more at https://docs.docker.com/go/debug-cli/

2026-06-06 09:13:08,757 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
Product,total_quantity
Water Bottle - 30 oz.,164086
Patch Kit/8 Patches,157583
Mountain Tire Tube,102792
AWC Logo Cap,67316
"Sport-100 Helmet, Red",63663

What's next:
    Try Docker Debug for seamless, persistent debugging tools in any container or image → docker debug namenode
    Learn more at https://docs.docker.com/go/debug-cli/
```

