package chapter5.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource}

import java.time.Duration

object AverageSensorReadings {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val strategy: WatermarkStrategy[SensorReading] = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
        override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
      })

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource())
      .assignTimestampsAndWatermarks(strategy)

    val avgTemp: DataStream[SensorReading] = sensorData
      .map(r => new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0))) // Convert to Fahrenheit.
      .keyBy((sr: SensorReading) => sr.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(1L))) // group readings in 1 second windows
      .apply(new TemperatureAverager) // compute average temperature using a user-defined function

    // print result stream to standard out
    avgTemp.print()

    // execute application
    env.execute("Compute average sensor temperature")
  }
}

/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(sensorId: String, window: TimeWindow, iterable: java.lang.Iterable[SensorReading], collector: Collector[SensorReading]): Unit = {

    val it = iterable.iterator()
    val vals = collection.mutable.ArrayBuffer[SensorReading]()
    while (it.hasNext) vals += it.next()

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    collector.collect(new SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
