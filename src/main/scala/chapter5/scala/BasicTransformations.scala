package chapter5.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource}

import java.time.Duration

object BasicTransformations {

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
    val readings: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(strategy)

    // filter out sensor measurements from sensors with temperature under 25 degrees
    val filteredSensors: DataStream[SensorReading] = readings
      .filter(r => r.temperature >= 25)

    // the above filter transformation using a UDF
    // val filteredSensors: DataStream[SensorReading] = readings
    //   .filter(new TemperatureFilter(25))

    // project the id of each sensor reading
    val sensorIds: DataStream[String] = filteredSensors
      .map( r => r.id )

    // the above map transformation using a UDF
    // val sensorIds2: DataStream[String] = readings
    //   .map(new ProjectionMap)

    // split the String id of each sensor to the prefix "sensor" and sensor number
    val splitIds: DataStream[String] = sensorIds
      .flatMap(new FlatMapFunction[String, String] {
        override def flatMap(t: String, collector: Collector[String]): Unit = t.split("_").foreach(collector.collect)
      })

    // the above flatMap transformation using a UDF
    // val splitIds: DataStream[String] = sensorIds
    //  .flatMap( new SplitIdFlatMap )

    // print result stream to standard out
    splitIds.print()

    // execute application
    env.execute("Basic Transformations Example")
  }

  /** User-defined FilterFunction to filter out SensorReading with temperature below the threshold */
  class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {

    override def filter(r: SensorReading): Boolean = r.temperature >= threshold

  }

  /** User-defined MapFunction to project a sensor's id */
  class ProjectionMap extends MapFunction[SensorReading, String] {

    override def map(r: SensorReading): String  = r.id

  }

  /** User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number */
  class SplitIdFlatMap extends FlatMapFunction[String, String] {

    override def flatMap(id: String, collector: Collector[String]): Unit =
      id.split("_").foreach(collector.collect)

  }

}
