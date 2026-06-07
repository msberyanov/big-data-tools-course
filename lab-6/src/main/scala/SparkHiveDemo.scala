import org.apache.spark.sql.SparkSession

object SparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Hive Integration")
      .master("spark://spark-master:7077")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
      .config("spark.hadoop.hive.metastore.schema.verification", "false")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
    spark.sql("USE demo_db")
    spark.sql(
      """
CREATE TABLE IF NOT EXISTS users (
id INT,
name STRING,
age INT
)
STORED AS PARQUET
""")
    spark.sql("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")

    println("Available users:")
    spark.sql("SELECT * FROM users").show()

    println("Users aged more than 26:")
    spark.table("demo_db.users")
      .filter($"age" > 26)
      .show()

    spark.stop()
  }
}