package chapter6.scala

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource}

object ProcessFunctionTimers {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

    val warnings = readings
      .keyBy((in: SensorReading) => in.id)
      .process(new TempIncreaseAlertFunction)

    warnings.print()

    env.execute("Monitor sensor temperatures.")
  }
}

/** Emits a warning if the temperature of a sensor
 * monotonically increases for 1 second (in processing time).
 */
class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

  // hold temperature of last sensor reading
  private lazy val lastTemp: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  // hold timestamp of currently active timer
  private lazy val currentTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))

  override def processElement(
                               r: SensorReading,
                               ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                               out: Collector[String]): Unit = {

    // get previous temperature
    val prevTemp = lastTemp.value()
    // update last temperature
    lastTemp.update(r.temperature)

    val curTimerTimestamp = currentTimer.value()
    if (prevTemp == 0.0) {
      // first sensor reading for this key. We cannot compare it with a previous value.
    }
    else if (r.temperature < prevTemp) {
      // temperature decreased. Delete current timer.
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    }
    else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
      // temperature increased and a timer is not set yet. Set timer for now + 1 second
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // remember current timer
      currentTimer.update(timerTs)
    }
  }

  override def onTimer(
                        ts: Long,
                        ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                        out: Collector[String]): Unit = {

    out.collect("Temperature of sensor '" + ctx.getCurrentKey + "' monotonically increased for 1 second.")
    currentTimer.clear() // reset current timer
  }
}