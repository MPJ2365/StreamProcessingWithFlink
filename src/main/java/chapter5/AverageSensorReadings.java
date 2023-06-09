package chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.SensorReading;
import util.SensorSource;

import java.time.Duration;

public class AverageSensorReadings {

  /** main() defines and executes the DataStream program */
  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
      .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner((sr, ts) -> sr.getTimestamp());

    // ingest sensor stream
    DataStream<SensorReading> sensorData = env
      .addSource(new SensorSource())
      .assignTimestampsAndWatermarks(strategy);

    DataStream<SensorReading> avgTemp = sensorData
      .map(r -> new SensorReading(r.getId(), r.getTimestamp(), (r.getTemperature() - 32) * (5.0 / 9.0))) // Convert to Fahrenheit.
      .keyBy(SensorReading::getId)
      .window(TumblingEventTimeWindows.of(Time.seconds(1L))) // group readings in 1 second windows
      .apply(new TemperatureAvenger()); // compute average temperature using a user-defined function

    // print result stream to standard out
    avgTemp.print();

    // execute application
    env.execute("Compute average sensor temperature");
  }
}

