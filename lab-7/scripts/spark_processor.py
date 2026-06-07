from pyspark.sql import SparkSession

def process_data():
    spark = SparkSession.builder \
        .appName("AirflowSparkProcessor") \
        .getOrCreate()
    
    # Чтение данных из HDFS
    df = spark.read.csv("hdfs://namenode:8020/data/input/raw_data.csv", header=True)
    
    # Пример обработки
    processed_df = df.filter(df["value"] > 100).groupBy("category").count()
    
    # Сохранение результатов
    processed_df.write.parquet("hdfs://namenode:8020/data/output/processed_data.parquet")
    
    spark.stop()

if __name__ == "__main__":
    process_data()
