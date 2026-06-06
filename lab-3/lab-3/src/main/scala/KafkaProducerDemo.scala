import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object KafkaProducerDemo {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092") // внешний порт Kafka
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val topic = "transactions"
    val users = Seq("user1", "user2", "user3", "user4", "user5")
    val products = Seq("productA", "productB", "productC", "productD")
    try {
      while (true) {
        val user = users(Random.nextInt(users.length))
        val product = products(Random.nextInt(products.length))
        val amount = BigDecimal(Random.nextDouble() * 100).setScale(2,
          BigDecimal.RoundingMode.HALF_UP).toDouble
        val timestamp = System.currentTimeMillis()
        val transaction =
          s"""{
"user_id": "$user",
"product_id": "$product",
"amount": $amount,
"timestamp": ${timestamp}
}"""
        val record = new ProducerRecord[String, String](topic, user, transaction)
        producer.send(record)
        println(s"Sent: $transaction")
        TimeUnit.SECONDS.sleep(Random.nextInt(5) + 1)
      }
    } finally {
      producer.close()
    }
  }
}