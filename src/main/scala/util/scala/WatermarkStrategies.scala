package util.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.time.Duration

object WatermarkStrategies {

  val sensorReadingStrategy: WatermarkStrategy[SensorReading] = WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofSeconds(5L))
    .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
      override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
    })

}
