package chapter7;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

public class StatefulProcessFunction {

  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

    // ingest sensor stream
    DataStream<SensorReading> sensorData = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource())
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new WatermarkStrategies().sensorReadingStrategy);

    KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(SensorReading::getId);

    DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
      .process(new SelfCleaningTemperatureAlertFunction(1.7));

    // print result stream to standard out
    alerts.print();

    // execute application
    env.execute("Generate Temperature Alerts");
  }
}

/**
 * The function emits an alert if the temperature measurement of a sensor changed by more than
 * a configured threshold compared to the last reading.
 */
class SelfCleaningTemperatureAlertFunction extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>> {

  private final Double threshold;

  public SelfCleaningTemperatureAlertFunction(Double threshold) {
    this.threshold = threshold;
  }


  // the state handle object
  private ValueState<Double> lastTempState = null;
  private ValueState<Long> lastTimerState = null;

  @Override
  public void open(Configuration parameters) {
    // register state for last temperature
    ValueStateDescriptor<Double> lastTempDescriptor = new ValueStateDescriptor<>("lastTemp", Types.DOUBLE);
    lastTempState = getRuntimeContext().getState(lastTempDescriptor);

    // register state for last timer
    ValueStateDescriptor<Long> timestampDescriptor = new ValueStateDescriptor<>("timestampState", Types.LONG);
    lastTimerState = getRuntimeContext().getState(timestampDescriptor);
  }

  @Override
  public void processElement(SensorReading reading,
                             KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>.Context ctx,
                             Collector<Tuple3<String, Double, Double>> collector) throws Exception {

    long newTimer = ctx.timestamp() + (3600 * 1000); // compute timestamp of new clean up timer as record timestamp + one hour
    Long curTimer = lastTimerState.value(); // get timestamp of current timer

    // delete previous timer and register new timer
    if (curTimer != null) ctx.timerService().deleteEventTimeTimer(curTimer);
    ctx.timerService().registerEventTimeTimer(newTimer);

    // update timer timestamp state
    lastTimerState.update(newTimer);

    // fetch the last temperature from state
    Double lastTemp = lastTempState.value();
    if (lastTemp != null) {
      // check if we need to emit an alert
      double tempDiff = Math.abs(reading.getTemperature() - lastTemp);
      if (tempDiff > threshold) {
        // temperature increased by more than the thresholdTimer
        collector.collect(Tuple3.of(reading.getId(), reading.getTemperature(), tempDiff));
      }
    }

    // update lastTemp state
    this.lastTempState.update(reading.getTemperature());
  }

  @Override
  public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>.OnTimerContext ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
    lastTempState.clear();
    lastTimerState.clear();
  }
}