# Отчёт по лабораторной работе 3.1.

---

## Задание.

**Цель**: Написать программу на Scala, реализующую алгоритм MapReduce для подсчёта частоты встречаемости слов в текстовом файле с использованием Hadoop.

**Реализация**:

Программа состоит из трёх компонентов:

### 1. WordMapper.scala.

Класс `WordMapper` реализует этап Map. Он принимает на вход текстовую строку, преобразует её в нижний регистр, удаляет знаки препинания и разбивает на отдельные слова. Каждому слову сопоставляется значение 1 (факт наличия).

```scala
package com.example

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  private val word = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    value.toString.toLowerCase
      .replaceAll("[^a-z\\s]", "")
      .split("\\s+")
      .filter(_.nonEmpty)
      .foreach { token =>
        word.set(token)
        context.write(word, one)
      }
  }
}
```

### 2. WordReducer.scala.

Класс `WordReducer` реализует этап Reduce. Он суммирует все значения для каждого уникального слова, получая итоговую частоту встречаемости.

```scala
package com.example

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

class WordReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, 
                     values: java.lang.Iterable[IntWritable],
                     context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    
    val valuesIter = values.asScala
    
    val sum = valuesIter.map(_.get()).sum
    
    context.write(key, new IntWritable(sum))
  }
}
```

### 3. WordCount.scala.

Основной класс `WordCount` настраивает и запускает MapReduce-задачу, указывая классы mapper, combiner, reducer, а также входной и выходной пути.

```scala
package com.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input path> <output path>")
      System.exit(-1)
    }

    val conf = new Configuration()
    val job = Job.getInstance(conf, "Word Count")
    
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[WordMapper])
    job.setCombinerClass(classOf[WordReducer])
    job.setReducerClass(classOf[WordReducer])
    
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
```

**Результат выполнения** (при запущенных Docker-контейнерах Hadoop):

Для входного файла `input.txt` с содержимым:
```
Hello world
Hello Hadoop
Goodbye Hadoop
```

Результат работы программы:
```
Goodbye    1
Hadoop     2
Hello      2
world      1
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

## Примечание.

Для автоматизации всех операций используется скрипт [`scripts.sh`](./scripts.sh):

```bash
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
```

Вывод команд выглядит следующим образом:

```shell
$ docker exec -it namenode hdfs dfs -mkdir -p /tmp

$ docker exec -it namenode hdfs dfs -ls /
Found 1 items
drwxr-xr-x   - root supergroup          0 2026-06-06 02:35 /tmp

$ sbt assembly
[info] welcome to sbt 1.12.11 (Microsoft Java 21.0.8)
[info] loading settings for project lab-1-build-build from metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-1/project/project
[info] loading settings for project lab-1-build from assembly.sbt, metals.sbt...
[info] loading project definition from /Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-1/project
[success] Generated .bloop/lab-1-build.json
[success] Total time: 2 s, completed 6 июн. 2026 г., 09:36:08
[info] loading settings for project lab-1 from build.sbt...
[info] set current project to lab-3_1 (in build file:/Users/beryanov.m.s/Documents/courses/big-data-tools-course/lab-1/)
[info] Strategy 'discard' was applied to 258 files (Run the task at debug level to see details)
[info] Strategy 'filterDistinctLines' was applied to 7 files (Run the task at debug level to see details)
[info] Strategy 'first' was applied to 110 files (Run the task at debug level to see details)
[success] Total time: 38 s, completed 6 июн. 2026 г., 09:36:47

$ docker cp target/scala-2.13/lab-1.jar namenode:/tmp/
Successfully copied 65.6MB to namenode:/tmp/

$ docker cp input.txt namenode:/tmp/
Successfully copied 3.07kB to namenode:/tmp/

$ docker exec -it namenode hadoop fs -mkdir -p /input

$ docker exec -it namenode hadoop fs -put /tmp/input.txt /input/input.txt
2026-06-06 02:37:10,382 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false

$ docker exec -it namenode hadoop jar /tmp/lab-1.jar /input/input.txt /output/
2026-06-06 02:37:23,778 INFO impl.MetricsConfig: Loaded properties from hadoop-metrics2.properties
2026-06-06 02:37:23,850 INFO impl.MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
2026-06-06 02:37:23,851 INFO impl.MetricsSystemImpl: JobTracker metrics system started
2026-06-06 02:37:24,075 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2026-06-06 02:37:24,505 INFO input.FileInputFormat: Total input files to process : 1
2026-06-06 02:37:24,573 INFO mapreduce.JobSubmitter: number of splits:1
2026-06-06 02:37:24,756 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local2035663043_0001
2026-06-06 02:37:24,756 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-06-06 02:37:24,891 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
2026-06-06 02:37:24,896 INFO mapreduce.Job: Running job: job_local2035663043_0001
2026-06-06 02:37:24,900 INFO mapred.LocalJobRunner: OutputCommitter set in config null
2026-06-06 02:37:24,914 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 2
2026-06-06 02:37:24,914 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-06-06 02:37:24,918 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
2026-06-06 02:37:24,973 INFO mapred.LocalJobRunner: Waiting for map tasks
2026-06-06 02:37:24,974 INFO mapred.LocalJobRunner: Starting task: attempt_local2035663043_0001_m_000000_0
2026-06-06 02:37:25,006 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 2
2026-06-06 02:37:25,007 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-06-06 02:37:25,037 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
2026-06-06 02:37:25,043 INFO mapred.MapTask: Processing split: hdfs://namenode:9000/input/input.txt:0+40
2026-06-06 02:37:25,071 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
2026-06-06 02:37:25,071 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
2026-06-06 02:37:25,071 INFO mapred.MapTask: soft limit at 83886080
2026-06-06 02:37:25,071 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
2026-06-06 02:37:25,071 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
2026-06-06 02:37:25,084 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
2026-06-06 02:37:25,114 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-06-06 02:37:25,430 INFO mapred.LocalJobRunner: 
2026-06-06 02:37:25,432 INFO mapred.MapTask: Starting flush of map output
2026-06-06 02:37:25,432 INFO mapred.MapTask: Spilling map output
2026-06-06 02:37:25,433 INFO mapred.MapTask: bufstart = 0; bufend = 64; bufvoid = 104857600
2026-06-06 02:37:25,433 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26214376(104857504); length = 21/6553600
2026-06-06 02:37:25,481 INFO mapred.MapTask: Finished spill 0
2026-06-06 02:37:25,494 INFO mapred.Task: Task:attempt_local2035663043_0001_m_000000_0 is done. And is in the process of committing
2026-06-06 02:37:25,502 INFO mapred.LocalJobRunner: map
2026-06-06 02:37:25,503 INFO mapred.Task: Task 'attempt_local2035663043_0001_m_000000_0' done.
2026-06-06 02:37:25,513 INFO mapred.Task: Final Counters for attempt_local2035663043_0001_m_000000_0: Counters: 24
        File System Counters
                FILE: Number of bytes read=65589225
                FILE: Number of bytes written=66623915
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=40
                HDFS: Number of bytes written=0
                HDFS: Number of read operations=5
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=1
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Map input records=3
                Map output records=6
                Map output bytes=64
                Map output materialized bytes=57
                Input split bytes=101
                Combine input records=6
                Combine output records=4
                Spilled Records=4
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=454033408
        File Input Format Counters 
                Bytes Read=40
2026-06-06 02:37:25,514 INFO mapred.LocalJobRunner: Finishing task: attempt_local2035663043_0001_m_000000_0
2026-06-06 02:37:25,515 INFO mapred.LocalJobRunner: map task executor complete.
2026-06-06 02:37:25,520 INFO mapred.LocalJobRunner: Waiting for reduce tasks
2026-06-06 02:37:25,521 INFO mapred.LocalJobRunner: Starting task: attempt_local2035663043_0001_r_000000_0
2026-06-06 02:37:25,541 INFO output.FileOutputCommitter: File Output Committer Algorithm version is 2
2026-06-06 02:37:25,541 INFO output.FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
2026-06-06 02:37:25,542 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
2026-06-06 02:37:25,548 INFO mapred.ReduceTask: Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@3524f26b
2026-06-06 02:37:25,551 WARN impl.MetricsSystemImpl: JobTracker metrics system already initialized!
2026-06-06 02:37:25,582 INFO reduce.MergeManagerImpl: MergerManager: memoryLimit=1279000576, maxSingleShuffleLimit=319750144, mergeThreshold=844140416, ioSortFactor=10, memToMemMergeOutputsThreshold=10
2026-06-06 02:37:25,587 INFO reduce.EventFetcher: attempt_local2035663043_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
2026-06-06 02:37:25,616 INFO reduce.LocalFetcher: localfetcher#1 about to shuffle output of map attempt_local2035663043_0001_m_000000_0 decomp: 53 len: 57 to MEMORY
2026-06-06 02:37:25,622 INFO reduce.InMemoryMapOutput: Read 53 bytes from map-output for attempt_local2035663043_0001_m_000000_0
2026-06-06 02:37:25,624 INFO reduce.MergeManagerImpl: closeInMemoryFile -> map-output of size: 53, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->53
2026-06-06 02:37:25,627 INFO reduce.EventFetcher: EventFetcher is interrupted.. Returning
2026-06-06 02:37:25,630 INFO mapred.LocalJobRunner: 1 / 1 copied.
2026-06-06 02:37:25,630 INFO reduce.MergeManagerImpl: finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
2026-06-06 02:37:25,641 INFO mapred.Merger: Merging 1 sorted segments
2026-06-06 02:37:25,641 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 43 bytes
2026-06-06 02:37:25,643 INFO reduce.MergeManagerImpl: Merged 1 segments, 53 bytes to disk to satisfy reduce memory limit
2026-06-06 02:37:25,644 INFO reduce.MergeManagerImpl: Merging 1 files, 57 bytes from disk
2026-06-06 02:37:25,645 INFO reduce.MergeManagerImpl: Merging 0 segments, 0 bytes from memory into reduce
2026-06-06 02:37:25,645 INFO mapred.Merger: Merging 1 sorted segments
2026-06-06 02:37:25,646 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 43 bytes
2026-06-06 02:37:25,647 INFO mapred.LocalJobRunner: 1 / 1 copied.
2026-06-06 02:37:25,682 INFO Configuration.deprecation: mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
2026-06-06 02:37:25,710 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2026-06-06 02:37:25,750 INFO mapred.Task: Task:attempt_local2035663043_0001_r_000000_0 is done. And is in the process of committing
2026-06-06 02:37:25,754 INFO mapred.LocalJobRunner: 1 / 1 copied.
2026-06-06 02:37:25,755 INFO mapred.Task: Task attempt_local2035663043_0001_r_000000_0 is allowed to commit now
2026-06-06 02:37:25,780 INFO output.FileOutputCommitter: Saved output of task 'attempt_local2035663043_0001_r_000000_0' to hdfs://namenode:9000/output
2026-06-06 02:37:25,782 INFO mapred.LocalJobRunner: reduce > reduce
2026-06-06 02:37:25,783 INFO mapred.Task: Task 'attempt_local2035663043_0001_r_000000_0' done.
2026-06-06 02:37:25,784 INFO mapred.Task: Final Counters for attempt_local2035663043_0001_r_000000_0: Counters: 30
        File System Counters
                FILE: Number of bytes read=65589371
                FILE: Number of bytes written=66623972
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=40
                HDFS: Number of bytes written=35
                HDFS: Number of read operations=10
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=3
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Combine input records=0
                Combine output records=0
                Reduce input groups=4
                Reduce shuffle bytes=57
                Reduce input records=4
                Reduce output records=4
                Spilled Records=4
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=454033408
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Output Format Counters 
                Bytes Written=35
2026-06-06 02:37:25,785 INFO mapred.LocalJobRunner: Finishing task: attempt_local2035663043_0001_r_000000_0
2026-06-06 02:37:25,785 INFO mapred.LocalJobRunner: reduce task executor complete.
2026-06-06 02:37:25,905 INFO mapreduce.Job: Job job_local2035663043_0001 running in uber mode : false
2026-06-06 02:37:25,905 INFO mapreduce.Job:  map 100% reduce 100%
2026-06-06 02:37:25,906 INFO mapreduce.Job: Job job_local2035663043_0001 completed successfully
2026-06-06 02:37:25,919 INFO mapreduce.Job: Counters: 36
        File System Counters
                FILE: Number of bytes read=131178596
                FILE: Number of bytes written=133247887
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=80
                HDFS: Number of bytes written=35
                HDFS: Number of read operations=15
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=4
                HDFS: Number of bytes read erasure-coded=0
        Map-Reduce Framework
                Map input records=3
                Map output records=6
                Map output bytes=64
                Map output materialized bytes=57
                Input split bytes=101
                Combine input records=6
                Combine output records=4
                Reduce input groups=4
                Reduce shuffle bytes=57
                Reduce input records=4
                Reduce output records=4
                Spilled Records=8
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=0
                Total committed heap usage (bytes)=908066816
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters 
                Bytes Read=40
        File Output Format Counters 
                Bytes Written=35

$ docker exec -it namenode hadoop fs -cat /output/part-r-00000
2026-06-06 02:37:30,032 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
goodbye 1
hadoop  2
hello   2
world   1
```
