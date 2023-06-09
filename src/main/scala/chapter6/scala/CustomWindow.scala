package chapter6.scala

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource, WatermarkStrategies}

import java.{lang, util}

object CustomWindow {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // checkpoint every 10 seconds
        env.getCheckpointConfig.setCheckpointInterval(10000)

        // ingest sensor stream
        val sensorData = env
            .addSource(new SensorSource())
            .assignTimestampsAndWatermarks(WatermarkStrategies.sensorReadingStrategy)

        val countsPerThirtySecs: DataStream[(String, Long, Long, Int)] = sensorData
            .keyBy((in: SensorReading) => in.id)
            // a custom window assigner for 30 seconds tumbling windows
            .window(new ThirtySecondsWindows())
            // a custom trigger that fires early (at most) every second
            .trigger(new OneSecondIntervalTrigger())
            // count readings per window
            .process(new CountFunction())

        countsPerThirtySecs.print()

        env.execute("Run custom window example")
    }

    /**
     * A custom window that groups events in to 30 second tumbling windows.
     */
    class ThirtySecondsWindows extends WindowAssigner[Object, TimeWindow] {

        val windowSize = 30000L

        override def assignWindows(e: Object,
                                   ts: Long,
                                   windowAssignerContext: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {
            // rounding down by 30 seconds
            val startTime = ts - (ts % windowSize)
            val endTime = startTime + windowSize

            // emitting the corresponding time window
            util.Collections.singletonList(new TimeWindow(startTime, endTime))
        }

        override def getDefaultTrigger(streamExecutionEnvironment: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = EventTimeTrigger.create()

        override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = new TimeWindow.Serializer

        override def isEventTime: Boolean = true
    }

    /**
     * A trigger thet fires early. The trigger fires at most every second.
     */
    class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {

        override def onElement(t: SensorReading, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
            // firstSeen will be false if not set yet
            val firstSeen: ValueState[Boolean] = triggerContext.getPartitionedState(new ValueStateDescriptor("firstSeen", classOf[Boolean]))

            // register initial timer only for first element
            if (!firstSeen.value) {
                // compute time for next early firing by rounding watermark to second
                val t: Long = triggerContext.getCurrentWatermark + (1000 - (triggerContext.getCurrentWatermark % 1000))
                triggerContext.registerEventTimeTimer(t)

                // register timer for the end of the window
                triggerContext.registerEventTimeTimer(w.getEnd)
                firstSeen.update(true)
            }
            // Continue. Do not evaluate window per element
            TriggerResult.CONTINUE
        }

        override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
            // Continue. We don't use processing time timers
            TriggerResult.CONTINUE
        }

        override def onEventTime(ts: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
            if (ts == w.getEnd) {
                // final evaluation and purge window state
                TriggerResult.FIRE_AND_PURGE
            }
            else {
                // register next early firing timer
                val t: Long = triggerContext.getCurrentWatermark + (1000 - (triggerContext.getCurrentWatermark % 1000))

                if (t < w.getEnd) triggerContext.registerEventTimeTimer(t)

                // fire trigger to early evaluate window
                TriggerResult.FIRE
            }
        }

        override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
            val firstSeen: ValueState[Boolean] = triggerContext.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
            firstSeen.clear()
        }
    }

    /**
     * A window function that counts the readings per sensor and window.
     * The function emits the sensor id, window end, time of function evaluation, and count.
     */
    class CountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow] {

        /*
        @Override
        public void process(
            String id,
            Context ctx,
            Iterable<SensorReading> readings,
            Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            // count readings
            int cnt = 0;
            for (SensorReading ignored : readings) {
                cnt++;
            }
            // get current watermark
            long evalTime = ctx.currentWatermark();
            // emit result
            out.collect(Tuple4.of(id, ctx.window().getEnd(), evalTime, cnt));
        }

         */

        override def process(key: String,
                             context: ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow]#Context,
                             iterable: lang.Iterable[SensorReading],
                             collector: Collector[(String, Long, Long, Int)]): Unit = {
            var cnt = 0
            val it = iterable.iterator()
            while (it.hasNext) {cnt += 1; it.next()}

            val evalTime = context.currentWatermark()

            collector.collect((key, context.window().getEnd, evalTime, cnt))
        }
    }
}