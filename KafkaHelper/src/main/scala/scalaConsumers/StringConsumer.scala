package scalaConsumers

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serdes.IntegerSerde
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Random

/**
 * This example expects a topic "purchases2" to exist with 2 partitions
 */
class SimpleTopicConsumer(inputTopic: String, numberPartitions: Int) {

  @volatile var doneConsuming = false
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  def startConsuming(): Unit = {
    val properties = getConsumerProps

    for (_ <- 0 until numberPartitions) {
      getConsumerThread(properties)
    }
  }

  def getConsumerThread(properties: Properties): Future[Unit] = {
    Future {
      val consumer: Consumer[String, Integer] = new KafkaConsumer(properties, new StringDeserializer(), new IntegerDeserializer())
      try {
        consumer.subscribe(Collections.singletonList(inputTopic))

        while (!doneConsuming) {
          val records: ConsumerRecords[String, Integer] = consumer.poll(Duration.ofSeconds(5))
          for (record <- records.asScala) {
            val message = s"[${Thread.currentThread().getName}] Consumed: key = ${record.key()} value = ${record.value()} with offset = ${record.offset()} partition = ${record.partition()}"
            System.out.println(message)
          }

        }
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        consumer.close()
      }
    }
  }

  def stopConsuming(): Unit = {
    doneConsuming = true
  }

  def getConsumerProps: Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "simple-consumer-example3")
    properties.put("auto.offset.reset", "earliest")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "3000")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    properties
  }

  /**
   * Change the constructor arg to match the actual number of partitions
   */

}

object StringConsumer {
  def main(args: Array[String]): Unit = {
    val consumerExample = new SimpleTopicConsumer("names3", 2)

    consumerExample.startConsuming()
    Thread.sleep(30000) //Run for 30 seconds minute
    consumerExample.stopConsuming()
  }

}
