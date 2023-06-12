package scalaProducers

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

object StringProducer {

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks", "1") // We only have one broker in local
    properties.put("retries", "3")
    properties.put("compression.type", "snappy")

    val start = System.currentTimeMillis()
    val r = new Random()

    val n = new AtomicInteger()
    val names = Vector(
      "Ana", "Pepe", "Luis", "Juan", "Sara", "Laura", "Javi", "Paula", "Ana", "Jose", "Alex", "Eva"
    )

    val callback: Callback = (metadata: RecordMetadata, exception: Exception) =>
      if (exception != null) exception.printStackTrace()
      else {
        println(s"Mensaje producido => topic: ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}, timestamp: ${metadata.timestamp()}")
      }

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties, new StringSerializer(), new StringSerializer())

    while (true) {

      val value = names.apply(r.nextInt(names.length))
      val key = n.incrementAndGet()

      val record = new ProducerRecord("names", key.toString, value)
      producer.send(record, callback)
      Thread.sleep(500)
    }

  }
}
