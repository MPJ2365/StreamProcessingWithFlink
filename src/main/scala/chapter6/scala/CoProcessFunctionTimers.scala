package chapter6.scala

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource}

/**
 * This example shows how to use a CoProcessFunction and Timers.
 */
object CoProcessFunctionTimers {

    def main(args: Array[String]): Unit = {

        // set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // switch messages disable filtering of sensor readings for a specific amount of time
        val filterSwitches: DataStream[(String, Long)] = env.fromElements(
            ("sensor_2", 10000L), ("sensor_7", 60000L)
        )

        // ingest sensor stream
        val readings = env.addSource(new SensorSource())

        val forwardedReadings = readings
          .connect(filterSwitches)
          .keyBy((sr: SensorReading) => sr.id, (in: (String, Long)) => in._1)
          .process(new ReadingFilter())

        forwardedReadings.print()

        env.execute("Filter sensor readings")
    }
}

class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

    // switch to enable forwarding
    private lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor("filterSwitch", classOf[Boolean]))
    // timestamp to disable the currently active timer
    private lazy val disableTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor("timer", classOf[Long]))

    override def processElement1(in1: SensorReading,
                                 context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 collector: Collector[SensorReading]): Unit = {
        // check if we need to forward the reading
        val forward = forwardingEnabled.value()
        if (forward) collector.collect(in1)
    }

    override def processElement2(in2: (String, Long),
                                 context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 collector: Collector[SensorReading]): Unit = {
        val (_, currentTimer1) = in2
        val currentTimer = context.timerService().currentProcessingTime() + currentTimer1
        forwardingEnabled.update(true)

        val prevTimer = disableTimer.value()

        if (prevTimer < currentTimer) {
            context.timerService().deleteProcessingTimeTimer(prevTimer)
            context.timerService().registerProcessingTimeTimer(currentTimer)
            disableTimer.update(currentTimer)
        }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
        forwardingEnabled.clear()
        disableTimer.clear()
    }

}

