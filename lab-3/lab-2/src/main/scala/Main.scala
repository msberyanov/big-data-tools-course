import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Инициализация Spark
    val spark = SparkSession.builder()
      .appName("RetailSalesAnalysis")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    try {
      // Чтение данных
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs:///data/sales_data.csv")

      // 1. Анализ прибыли по категориям
      val profitByCategory = df.groupBy("category")
        .agg(sum("Profit").alias("total_profit"))
        .orderBy(desc("total_profit"))

      println("=== Profit by Category ===")
      profitByCategory.show()

      // 2. Средняя прибыль на покупку по странам
      val avgProfitByCountry = df.groupBy("Country")
        .agg(avg("Profit").alias("avg_profit_per_order"))
        .orderBy(desc("avg_profit_per_order"))

      println("=== Average Profit by Country ===")
      avgProfitByCountry.show()

      // 3. Топ-5 продуктов по количеству заказов
      val top5Products = df.groupBy("Product")
        .agg(sum("Order_Quantity").alias("total_quantity"))
        .orderBy(desc("total_quantity"))
        .limit(5)

      println("=== Top 5 Products ===")
      top5Products.show()

      // Сохранение результатов
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
