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