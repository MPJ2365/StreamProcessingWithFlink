package chapter5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import util.*;

import java.time.Duration;

/**
* A simple application that outputs an alert whenever there is a high risk of fire.
* The application receives the stream of temperature sensor readings and a stream of smoke level measurements.
* When the temperature is over a given threshold and the smoke level is high, we emit a fire alert.
*/

public class MultiStreamTransformations {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<SensorReading> strategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                .withTimestampAssigner((sr, ts) -> sr.getTimestamp());

        // ingest sensor stream
        DataStream<SensorReading> tempReadings = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(strategy);

        // ingest smoke level stream
        DataStream<SmokeLevel> smokeReadings = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        // group sensor readings by sensor id (In this example, it's not necessary, since smoke alerts don't have a key)
        KeyedStream<SensorReading, String> keyedTempReadings = tempReadings
                .keyBy(SensorReading::getId);

        // connect the two streams and raise an alert if the temperature and
        // smoke levels are high
        DataStream<Alert> alerts = keyedTempReadings
                .connect(smokeReadings.broadcast()) // Broadcast, the smoke stream produces very few events (1 per sec)
                .flatMap(new RaiseAlertFlatMap());

        alerts.print();

        // execute the application
        env.execute("Multi-Stream Transformations Example");
    }

    /**
     * A CoFlatMapFunction that processes a stream of temperature readings and a control stream
     * of smoke level events. The control stream updates a shared variable with the current smoke level.
     * For every event in the sensor stream, if the temperature reading is above 100 degrees
     * and the smoke level is high, a "Risk of fire" alert is generated.
     */
    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading tempReading, Collector<Alert> out) throws Exception {
            // high chance of fire => true
            if (this.smokeLevel == SmokeLevel.HIGH && tempReading.getTemperature() > 100) {
                out.collect(new Alert("Risk of fire! " + tempReading, tempReading.getTimestamp()));
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> out) {
            // update smoke level
            this.smokeLevel = smokeLevel;
        }
    }
}