package chapter6.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}
import util.scala.{SensorReading, SensorSource, WatermarkStrategies}

import java.lang
import scala.util.Random

object LateDataHandling {

  // define a side output tag
  val lateReadingsOutput: OutputTag[SensorReading] = new OutputTag[SensorReading]("late-readings"){}

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // ingest sensor stream and shuffle timestamps to produce out-of-order records
    val outOfOrderReadings: DataStream[SensorReading] = env.addSource(new SensorSource)
      // shuffle timestamps by max 7 seconds to generate late data
      .map(new TimestampShuffler(7 * 1000))
      // assign timestamps and watermarks with an offset of 5 seconds
      .assignTimestampsAndWatermarks(WatermarkStrategies.sensorReadingStrategy)

    // Different strategies to handle late records.
    // Select and uncomment on of the lines below to demonstrate a strategy.

    // 1. Filter out late readings (to a side output) using a ProcessFunction
    // filterLateReadings(outOfOrderReadings)
    // 2. Redirect late readings to a side output in a window operator
    // sideOutputLateEventsWindow(outOfOrderReadings)
    // 3. Update results when late readings are received in a window operator
    updateForLateEventsWindow(outOfOrderReadings)

    env.execute()
  }

  /** Filter late readings to a side output and print the on-time and late streams. */
  private def filterLateReadings(readings: DataStream[SensorReading]): Unit = {
    // re-direct late readings to the side output
    val filteredReadings: SingleOutputStreamOperator[SensorReading] = readings.process(new LateReadingsFilter)

    // retrieve late readings
    val lateReadings: DataStream[SensorReading] = filteredReadings.getSideOutput(lateReadingsOutput)

    // print the filtered stream
    filteredReadings.print()

    // print messages for late readings
    lateReadings
      .map(r => "*** late reading *** " + r.id)
      .print()
  }

  /** Count reading per tumbling window and emit late readings to a side output.
   * Print results and late events. */
  private def sideOutputLateEventsWindow(readings: DataStream[SensorReading]): Unit = {

    val countPer10Secs: SingleOutputStreamOperator[(String, Long, Int)] = readings
      .keyBy((sr: SensorReading) => sr.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // emit late readings to a side output
      .sideOutputLateData(lateReadingsOutput)
      // count readings per window
      .process(new ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow] {
           override def process(key: String,
                             context: ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow]#Context,
                             iterable: lang.Iterable[SensorReading],
                             collector: Collector[(String, Long, Int)]): Unit = {
             var cnt = 0
             val it = iterable.iterator()
             while (it.hasNext) {it.next(); cnt += 1}
             collector.collect((key, context.window().getEnd, cnt))
        }
      })

    // retrieve and print messages for late readings
    countPer10Secs
      .getSideOutput(lateReadingsOutput)
      .map(r => "*** late reading *** " + r.id)
      .print()

    // print results
    countPer10Secs.print()
  }

  /** Count reading per tumbling window and update results if late readings are received.
   * Print results. */
  private def updateForLateEventsWindow(readings: DataStream[SensorReading]): Unit = {

    val countPer10Secs: DataStream[(String, Long, Int, String)] = readings
      .keyBy((sr: SensorReading) => sr.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // process late readings for 5 additional seconds
      .allowedLateness(Time.seconds(5))
      // count readings and update results if late readings arrive
      .process(new UpdatingWindowCountFunction)

    // print results
    countPer10Secs.print()
  }
}

/** A ProcessFunction that filters out late sensor readings and re-directs them to a side output */
class LateReadingsFilter extends ProcessFunction[SensorReading, SensorReading] {

  override def processElement(
                               r: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    // compare record timestamp with current watermark
    if (r.timestamp < ctx.timerService().currentWatermark()) {
      // this is a late reading => redirect it to the side output
      ctx.output(LateDataHandling.lateReadingsOutput, r)
    }
    else out.collect(r)
  }
}

/** A counting WindowProcessFunction that distinguishes between first results and updates. */
class UpdatingWindowCountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {

  override def process(key: String,
                       ctx: ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow]#Context,
                       elements: lang.Iterable[SensorReading],
                       collector: Collector[(String, Long, Int, String)]): Unit = {
    // count the number of readings
    var cnt = 0
    val it = elements.iterator()
    while (it.hasNext) {
      it.next()
      cnt += 1
    }

    // state to check if this is the first evaluation of the window or not.
    // Keep in mind that the process method is called once "the window ends" and after that, it's called only if late data arrives.
    // So it's not called very often and it's not inefficient to get the state like this.
    // In the documentation, Flink recommends using window state in these cases (handle late data, custom triggers).
    // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/#using-per-window-state-in-processwindowfunction
    val isUpdate = ctx.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate", classOf[Boolean]))

    if (!isUpdate.value()) {
      // first evaluation, emit first result
      collector.collect((key, ctx.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    }
    else {
      // not the first evaluation, emit an update
      collector.collect((key, ctx.window.getEnd, cnt, "update"))
    }
  }

  override def clear(context: ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow]#Context): Unit = {
    context.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate", classOf[Boolean])).clear()
  }
}

/** A MapFunction to shuffle (up to a max offset) the timestamps of SensorReadings to produce out-of-order events. */
class TimestampShuffler(maxRandomOffset: Int) extends MapFunction[SensorReading, SensorReading] {

  lazy val rand: Random = new Random()

  override def map(r: SensorReading): SensorReading = {
    val shuffleTs = r.timestamp + rand.nextInt(maxRandomOffset)
    new SensorReading(r.id, shuffleTs, r.temperature)
  }
}