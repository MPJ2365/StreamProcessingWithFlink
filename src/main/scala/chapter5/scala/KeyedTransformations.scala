package chapter5.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import util.scala.{SensorReading, SensorSource}

import java.time.Duration

object KeyedTransformations {

    def main(args: Array[String]): Unit = {

        // set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val strategy: WatermarkStrategy[SensorReading] = WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5L))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })

        // ingest sensor stream
        val readings = env
          .addSource(new SensorSource())
          .assignTimestampsAndWatermarks(strategy)

        // group sensor readings by sensor id
        val keyed = readings.keyBy((in: SensorReading) => in.id)

        // a rolling reduce that computes the highest temperature of each sensor and the corresponding timestamp

        val maxTempPerSensor = keyed
          .reduce((r1, r2) => if (r1.temperature > r2.temperature) r1 else r2)

        maxTempPerSensor.print()

        // execute application
        env.execute("Keyed Transformations Example")
    }
}