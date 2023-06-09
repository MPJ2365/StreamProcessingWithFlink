package chapter6;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.SensorReading;
import util.SensorSource;

import java.util.Objects;

public class WatermarkGeneration {

    public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // configure interval of periodic watermark generation
    env.getConfig().setAutoWatermarkInterval(1000L);

    // ingest sensor stream
    DataStream<SensorReading> readings = env.addSource(new SensorSource());

    DataStream<SensorReading> readingsWithPeriodicWMs = readings
      // assign timestamps and periodic watermarks
      .assignTimestampsAndWatermarks(new PeriodicWatermarkStrategy());

    DataStream<SensorReading>  readingsWithPunctuatedWMs = readings
      // assign timestamps and punctuated watermarks
      .assignTimestampsAndWatermarks(new PunctuatedWatermarkStrategy());

    readingsWithPeriodicWMs.print();
    //readingsWithPunctuatedWMs.print();

    env.execute("Assign timestamps and generate watermarks");
  }
}

/**
 * Watermark strategy. The Flink Api expects a startegy that contains both a TimestampAssigner and a WatermarkGenerator.
 */
class PeriodicWatermarkStrategy implements WatermarkStrategy<SensorReading> {

  @Override
  public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
    return new PunctuatedAssigner();
  }

  @Override
  public TimestampAssigner<SensorReading> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
    return (sensorReading, l) -> sensorReading.getTimestamp();
  }
}

class PunctuatedWatermarkStrategy implements WatermarkStrategy<SensorReading> {

  @Override
  public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
    return new PunctuatedAssigner();
  }

  @Override
  public TimestampAssigner<SensorReading> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
    return (sensorReading, l) -> sensorReading.getTimestamp();
  }
}

/**
 * Provides watermarks with a 1 minute out-of-ourder bound when being asked.
 *
 */
class PeriodicAssigner implements WatermarkGenerator<SensorReading> {

  // 1 min in ms
  long bound = 60*1000;
  // the maximum observed timestamp
  long maxTs = Long.MIN_VALUE;

  @Override
  public void onEvent(SensorReading sensorReading, long l, WatermarkOutput watermarkOutput) {
    maxTs = Math.max(maxTs, sensorReading.getTimestamp());
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
    watermarkOutput.emitWatermark(new Watermark(maxTs - bound));
  }
}

class PunctuatedAssigner implements WatermarkGenerator<SensorReading> {

  // 1 min in ms
  long bound = 60*1000;

  @Override
  public void onEvent(SensorReading sensorReading, long eventTS, WatermarkOutput watermarkOutput) {
    if (Objects.equals(sensorReading.getId(), "sensor_1")) watermarkOutput.emitWatermark(new Watermark(eventTS - bound));
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput watermarkOutput) {}
}