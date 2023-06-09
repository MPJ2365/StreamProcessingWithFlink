package chapter7;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

public class KeyedStateFunction {

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

    DataStream< Tuple3<String, Double, Double>> alerts = keyedSensorData
      .flatMap(new TemperatureAlertFunction(1.7));

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
class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

  private final Double threshold;

  public TemperatureAlertFunction(Double threshold) {
    this.threshold = threshold;
  }

  // The state handle object. We should clear it using a timer. Implemented in StatefulProcessFunction.java.
  ValueState<Double> lastTempState = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Types.DOUBLE));
  }

  public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {

    // fetch the last temperature from state
    Double lastTemp = lastTempState.value();

    // check if we need to emit an alert
    if (lastTemp != null) {
      Double tempDiff = Math.abs(sensorReading.getTemperature() - lastTemp);

      if (tempDiff > threshold) {
        // temperature changed by more than the threshold
        collector.collect(Tuple3.of(sensorReading.getId(), sensorReading.getTemperature(), tempDiff));
      }
    }

    // update lastTemp state
    this.lastTempState.update(sensorReading.getTemperature());
  }
}