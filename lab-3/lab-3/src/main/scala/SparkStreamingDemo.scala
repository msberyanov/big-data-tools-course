import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaSparkStreamingDemo")
      .master("spark://spark-master:7077")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("user_id", StringType),
      StructField("product_id", StringType),
      StructField("amount", DoubleType),
      StructField("timestamp", TimestampType)
    ))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093") // внутри докер-сети
      .option("subscribe", "transactions")
      .option("startingOffsets", "earliest")
      .load()

    val jsonDF = df.selectExpr("CAST(value AS STRING) as json")

    val transactionsDF = jsonDF
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    val windowedCounts = transactionsDF
      .withWatermark("timestamp", "1 minute") // водяной знак для обработки задержанных данных
      .groupBy(
        window($"timestamp", "5 minutes"),
        $"user_id"
      )
      .agg(
        count("*").as("transaction_count"),
        sum("amount").as("total_amount")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"user_id",
        $"transaction_count",
        $"total_amount"
      )

    val query = windowedCounts.writeStream
      .outputMode("update") // или "complete" для полного вывода
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}