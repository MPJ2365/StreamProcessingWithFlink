package chapter6;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;

/**
 * This example shows how to use a CoProcessFunction and Timers.
 */
public class CoProcessFunctionTimers {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // switch messages disable filtering of sensor readings for a specific amount of time
        DataStream<Tuple2<String, Long>> filterSwitches = env.fromElements(
            Tuple2.of("sensor_2", 10_000L), Tuple2.of("sensor_7", 60_000L)
        );

        // ingest sensor stream
        DataStream<SensorReading> readings = env.addSource(new SensorSource());

        DataStream<SensorReading> forwardedReadings = readings
            // connect readings and switches
            .connect(filterSwitches)
            // key by sensor ids
            .keyBy(SensorReading::getId, s -> s.f0)
            // apply filtering CoProcessFunction
            .process(new ReadingFilter());

        forwardedReadings.print();

        env.execute("Filter sensor readings");
    }

    public static class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        // switch to enable forwarding
        private ValueState<Boolean> forwardingEnabled;
        // timestamp to disable the currently active timer
        private ValueState<Long> disableTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN));
            disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
        }

        @Override
        public void processElement1(SensorReading r, Context ctx, Collector<SensorReading> out) throws Exception {
            // check if we need to forward the reading
            Boolean forward = forwardingEnabled.value();
            if (forward != null && forward) {
                out.collect(r);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> s, Context ctx, Collector<SensorReading> out) throws Exception {
            // enable forwarding of readings
            forwardingEnabled.update(true);
            // set timer to disable switch
            long timerTimestamp = ctx.timerService().currentProcessingTime() + s.f1;
            Long curTimerTimestamp = disableTimer.value();
            if (curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
                // remove current timer
                if (curTimerTimestamp != null) {
                    ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                }
                // register new timer
                ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                disableTimer.update(timerTimestamp);
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            // remove all state
            forwardingEnabled.clear();
            disableTimer.clear();
        }
    }
}

