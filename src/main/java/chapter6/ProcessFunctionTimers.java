package chapter6;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;

public class ProcessFunctionTimers {

  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // ingest sensor stream
    DataStream<SensorReading> readings = env.addSource(new SensorSource());

    SingleOutputStreamOperator<String> warnings = readings
      .keyBy(SensorReading::getId)
      .process(new TempIncreaseAlertFunction());

    warnings.print();

    env.execute("Monitor sensor temperatures.");
  }
}

/** Emits a warning if the temperature of a sensor
 * monotonically increases for 1 second (in processing time).
 */
class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

  // hold temperature of last sensor reading
  ValueState<Double> lastTemp;

  // hold timestamp of currently active timer
  ValueState<Long> currentTimer;

  @Override
  public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<Double> desc1 = new ValueStateDescriptor<>("lastTemp", Types.DOUBLE);
    ValueStateDescriptor<Long> desc2 = new ValueStateDescriptor<>("timer", Types.LONG);

    lastTemp = getRuntimeContext().getState(desc1);
    currentTimer = getRuntimeContext().getState(desc2);
  }

  @Override
  public void processElement(SensorReading sensorReading, KeyedProcessFunction<String, SensorReading, String>.Context context, Collector<String> collector) throws Exception {

    Double prevTemp = lastTemp.value(); // get previous temperature
    lastTemp.update(sensorReading.getTemperature()); // update last temperature

    Long curTimerTimestamp = currentTimer.value();

    if (prevTemp == null) {
      // first sensor reading for this key. We cannot compare it with a previous value.
    }
    else if (sensorReading.getTemperature() < prevTemp && curTimerTimestamp != null) {
      // temperature decreased. Delete current timer.
      context.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
      currentTimer.clear();
    }
    else if (sensorReading.getTemperature() > prevTemp && curTimerTimestamp == null) {
      // temperature increased and a timer is not set yet. Set timer for now + 1 second
      long timerTs = context.timerService().currentProcessingTime() + 1000;
      context.timerService().registerProcessingTimeTimer(timerTs);
      // remember current timer
      currentTimer.update(timerTs);
    }
  }

  @Override
  public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
    out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 1 second.");
    currentTimer.clear(); // reset current timer
  }
}