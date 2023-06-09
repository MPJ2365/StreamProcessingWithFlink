package util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public class WatermarkStrategies {

  public WatermarkStrategy<SensorReading> sensorReadingStrategy;

  public WatermarkStrategies() {
    this.sensorReadingStrategy = WatermarkStrategy
        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
        .withTimestampAssigner((sr, ts) -> sr.getTimestamp());
  }

}
