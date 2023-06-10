package chapter7;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

public class TrackMaximumTemperature2 {

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

        DataStream<Tuple2<String, Double>> tenSecsMaxTemps = sensorData
            .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                    return Tuple2.of(sensorReading.getId(), sensorReading.getTemperature());
                }
            })
            // compute every 10 seconds the max temperature per sensor
            .keyBy(sr -> sr.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> reduce(Tuple2<String, Double> stringDoubleTuple2, Tuple2<String, Double> t1) throws Exception {
                    return Tuple2.of(t1.f0, Math.max(t1.f1, stringDoubleTuple2.f1));
                }
            });

        // store latest value for each sensor in a queryable state
        tenSecsMaxTemps
            .keyBy(sr -> sr.f0)
            .asQueryableState("maxTemperature");

        // execute application
        env.execute("Track max temperature");
    }
}
