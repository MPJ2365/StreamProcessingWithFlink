package chapter6.scala

import org.apache.flink.api.common.eventtime.{TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import util.scala.{SensorReading, SensorSource}

object WatermarkGeneration {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure interval of periodic watermark generation
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

    val readingsWithPeriodicWMs = readings
      // assign timestamps and periodic watermarks
      .assignTimestampsAndWatermarks(new PeriodicWatermarkStrategy)

    val readingsWithPunctuatedWMs = readings
      // assign timestamps and punctuated watermarks
      .assignTimestampsAndWatermarks(new PunctuatedWatermarkStrategy)

    readingsWithPeriodicWMs.print()
    //readingsWithPunctuatedWMs.print()

    env.execute("Assign timestamps and generate watermarks")
  }
}

/**
 * Watermark strategy. The Flink Api expects a startegy that contains both a TimestampAssigner and a WatermarkGenerator.
 */
class PeriodicWatermarkStrategy extends WatermarkStrategy[SensorReading] {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = new PeriodicAssigner
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorReading] = (t: SensorReading, l: Long) => t.timestamp
}

class PunctuatedWatermarkStrategy extends WatermarkStrategy[SensorReading] {
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorReading] = new PunctuatedAssigner
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorReading] = (t: SensorReading, l: Long) => t.timestamp
}

/**
 * Provides watermarks with a 1 minute out-of-ourder bound when being asked.
 *
 */
class PeriodicAssigner extends WatermarkGenerator[SensorReading] {

  // 1 min in ms
  private val bound: Long = 60 * 1000
  // the maximum observed timestamp
  private var maxTs: Long = Long.MinValue

  override def onEvent(t: SensorReading, l: Long, watermarkOutput: WatermarkOutput): Unit = {
    maxTs = maxTs max t.timestamp
  }

  // triggered every second: env.getConfig.setAutoWatermarkInterval(1000L)
  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit =
    watermarkOutput.emitWatermark(new Watermark(maxTs - bound))
}

/**
 * Emits a watermark for each reading with sensorId == "sensor_1".
 */
class PunctuatedAssigner extends WatermarkGenerator[SensorReading] {

  // 1 min in ms
  private val bound: Long = 60 * 1000

  override def onEvent(t: SensorReading, eventTS: Long, watermarkOutput: WatermarkOutput): Unit = {
    if (t.id == "sensor_1") watermarkOutput.emitWatermark(new Watermark(eventTS - bound))
  }

  override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {}
}