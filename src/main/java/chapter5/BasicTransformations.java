package chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;

import java.time.Duration;
import java.util.Arrays;

public class BasicTransformations {

  /** main() defines and executes the DataStream program */
  public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
      .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
      .withTimestampAssigner((sr, ts) -> sr.getTimestamp());

    // ingest sensor stream
    DataStream<SensorReading> readings = env
      .addSource(new SensorSource())
      .assignTimestampsAndWatermarks(strategy);

    class TemperatureFilter implements FilterFunction<SensorReading> {
      final long threshold;

      public TemperatureFilter(long threshold) {
        this.threshold = threshold;
      }

      @Override
      public boolean filter(SensorReading sensorReading) throws Exception {
        return sensorReading.getTemperature() > threshold;
      }
    }
    class ProjectionMap implements MapFunction<SensorReading, String> {

      @Override
      public String map(SensorReading sensorReading) throws Exception {
        return sensorReading.getId();
      }
    }
    class SplitIdFlatMap implements FlatMapFunction<String, String> {

      @Override
      public void flatMap(String s, Collector<String> collector) throws Exception {
        Arrays.stream(s.split("_")).forEach(collector::collect);
      }
    }

    // filter out sensor measurements from sensors with temperature under 25 degrees
    // DataStream<SensorReading> filteredSensors = readings
    //  .filter(r -> r.getTemperature() >= 25);

    // the above filter transformation using a UDF
    DataStream<SensorReading> filteredSensors = readings
      .filter(new TemperatureFilter(25));

    // project the id of each sensor reading
    // DataStream<String> sensorIds = filteredSensors
    //   .map(SensorReading::getId);

    // the above map transformation using a UDF
    DataStream<String> sensorIds = filteredSensors
      .map(new ProjectionMap());

    // split the String id of each sensor to the prefix "sensor" and sensor number
//    DataStream<String> splitIds = sensorIds
//      .flatMap(new FlatMapFunction<String, String>() {
//                 @Override
//                 public void flatMap(String s, Collector<String> collector) throws Exception {
//                   Arrays.stream(s.split("_")).forEach(collector::collect);
//                 }
//               }
//      );

    // the above flatMap transformation using a UDF
    DataStream<String> splitIds = sensorIds
     .flatMap( new SplitIdFlatMap() );

    // print result stream to standard out
    splitIds.print();

    // execute application
    env.execute("Basic Transformations Example");
  }

}
