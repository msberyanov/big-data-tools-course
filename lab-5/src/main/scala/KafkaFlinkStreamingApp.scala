import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy

object KafkaFlinkStreamingApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9093")
      .setTopics("input-topic")
      .setGroupId("flink-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka Source"
    )

    stream.map { msg =>
      println(s"Processing: $msg")
      s"Processed: $msg (length: ${msg.length})"
    }.print()

    env.execute("Kafka to Flink Streaming Job")
  }
}