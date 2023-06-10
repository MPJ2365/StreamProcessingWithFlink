package chapter7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.MaxTemperature;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

public class TrackMaximumTemperature {

    /**
     * main() defines and executes the DataStream program
     */
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        Configuration conf = new Configuration();
        conf.setString("queryable-state.enable", "true"); // Need to enable this config to query state. As well as adding some dependencies to the pom.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new WatermarkStrategies().sensorReadingStrategy);

        DataStream<MaxTemperature> tenSecsMaxTemps = sensorData
            .map(new MapFunction<SensorReading, MaxTemperature>() {
                @Override
                public MaxTemperature map(SensorReading sensorReading) throws Exception {
                    return new MaxTemperature(sensorReading.getId(), sensorReading.getTemperature());
                }
            })
            // compute every 10 seconds the max temperature per sensor
            .keyBy(sr -> sr.id)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<MaxTemperature>() {
                @Override
                public MaxTemperature reduce(MaxTemperature stringDoubleTuple2, MaxTemperature t1) throws Exception {
                    return new MaxTemperature(t1.id, Math.max(t1.temperature, stringDoubleTuple2.temperature));
                }
            });

        // store latest value for each sensor in a queryable state
        tenSecsMaxTemps
            .keyBy(sr -> sr.id)
            .asQueryableState("maxTemperature");

        // execute application
        env.execute("Track max temperature");
    }
}
