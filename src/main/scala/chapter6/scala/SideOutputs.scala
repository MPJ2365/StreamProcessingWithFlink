package chapter6.scala

import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import util.scala.{SensorReading, SensorSource, WatermarkStrategies}

object SideOutputs {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(WatermarkStrategies.sensorReadingStrategy)

    val monitoredReadings: SingleOutputStreamOperator[SensorReading] = readings
      // monitor stream for readings with freezing temperatures
      .process(new FreezingMonitor)

    // retrieve and print the freezing alarms
    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"){})
      .print("freezing-alarms")

    // print the main output
    readings.print("Readings")

    env.execute()
  }
}

/** Emits freezing alarms to a side output for readings with a temperature below 32F. */
class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

  // define a side output tag
  private lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms"){}

  override def processElement(
                               r: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    // emit freezing alarm if temperature is below 32F.
    if (r.temperature < 32.0) ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")

    // forward all readings to the regular output
    out.collect(r)
  }
}