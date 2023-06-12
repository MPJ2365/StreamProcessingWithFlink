package chapter8.scala

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer

import java.lang

object KafkaSourceExample1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Kafka uses checkpointing to commit the current offset.
    // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/#consumer-offset-committing

    // start a checkpoint every 3000 ms
    env.enableCheckpointing(3000)
    // sets the checkpoint storage where checkpoint snapshots will be written
    env.getCheckpointConfig.setCheckpointStorage("file:///tmp/my/checkpoint/dir")

    val brokers = "localhost:9092"

    val source = KafkaSource.builder[String]()
      .setBootstrapServers(brokers)
      .setTopics("names")
      .setGroupId("my-group")
      .setStartingOffsets(OffsetsInitializer.earliest()) // Always from the first offset.
      //.setStartingOffsets(OffsetsInitializer.committedOffsets()) //  Start from committed offset of the consuming group. Fails in first execution.
      //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist.
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val names: SingleOutputStreamOperator[(String, Int)] =
      env
        .fromSource(source, WatermarkStrategy.forMonotonousTimestamps[String](), "Kafka Source")
        .map(new MapFunction[String, String] {
          override def map(t: String): String = t.toUpperCase
        })
        .keyBy((sr: String) => sr)
        .process(new KeyedProcessFunction[String, String, (String, Int)] {
          lazy val counter: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("counter", classOf[Int]))

          override def processElement(i: String, context: KeyedProcessFunction[String, String, (String, Int)]#Context, collector: Collector[(String, Int)]): Unit = {
            val count = counter.value()
            val updatedCount = count + 1
            counter.update(updatedCount)
            collector.collect((i, updatedCount))
          }
        }
        )

    names.print()

    val sink: KafkaSink[(String, Int)] = KafkaSink.builder[(String, Int)]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(new CounterSerializationSchema)
      .build()

    names.sinkTo(sink)

    env.execute()

  }
}

// Custom serializer. We produce send records to Kafka with a key and a value.
class CounterSerializationSchema extends KafkaRecordSerializationSchema[(String, Int)] {
  override def serialize(t: (String, Int), kafkaSinkContext: KafkaRecordSerializationSchema.KafkaSinkContext, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord(
      "names3",
      t._1.getBytes,
      new IntegerSerializer().serialize("", t._2)
    )
  }
}
