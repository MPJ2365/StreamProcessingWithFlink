package chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.SensorReading;
import util.SensorSource;

import java.time.Duration;

public class KeyedTransformations {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner((sr, ts) -> sr.getTimestamp());

        // ingest sensor stream
        DataStream<SensorReading> readings = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(strategy);

        // group sensor readings by sensor id
        KeyedStream<SensorReading, String> keyed = readings
                .keyBy(SensorReading::getId);

        // a rolling reduce that computes the highest temperature of each sensor and
        // the corresponding timestamp
        DataStream<SensorReading> maxTempPerSensor = keyed
                .reduce((r1, r2) -> {
                    if (r1.getTemperature() > r2.getTemperature()) {
                        return r1;
                    } else {
                        return r2;
                    }
                });

        maxTempPerSensor.print();

        // execute application
        env.execute("Keyed Transformations Example");
    }
}