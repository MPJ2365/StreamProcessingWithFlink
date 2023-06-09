package chapter6;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

public class SideOutputs {

  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

    // ingest sensor stream
    DataStream<SensorReading> readings = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource())
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new WatermarkStrategies().sensorReadingStrategy);

    SingleOutputStreamOperator<SensorReading> monitoredReadings = readings
      // monitor stream for readings with freezing temperatures
      .process(new FreezingMonitor());

    // retrieve and print the freezing alarms
    monitoredReadings
      .getSideOutput(new OutputTag<String>("freezing-alarms"){})
      .print("freezing-alarms");

    // print the main output
    readings.print("Readings");

    env.execute();
  }
}

/** Emits freezing alarms to a side output for readings with a temperature below 32F. */
class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {

  final OutputTag<String> freezingAlarmOutput = new OutputTag<String>("freezing-alarms"){};
  @Override
  public void processElement(SensorReading sensorReading, ProcessFunction<SensorReading, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {

    // emit freezing alarm if temperature is below 32F.
    if (sensorReading.getTemperature() < 32.0) context.output(freezingAlarmOutput, "Freezing Alarm for " + sensorReading.getId());

    // forward all readings to the regular output
    collector.collect(sensorReading);
  }
}