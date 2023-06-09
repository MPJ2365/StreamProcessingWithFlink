package chapter6;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Iterator;

public class WindowFunctions {

    double threshold = 25.0;

    public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

    // ingest sensor stream
    DataStream<SensorReading> sensorData = env
      .addSource(new SensorSource())
      .assignTimestampsAndWatermarks(new WatermarkStrategies().sensorReadingStrategy);

    DataStream<Tuple2<String, Double>> minTempPerWindow = sensorData
        .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return Tuple2.of(sensorReading.getId(), sensorReading.getTemperature());
            }
        })
        .keyBy(in -> in.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(15)))
        .reduce((r1, r2) -> Tuple2.of(r1.f0, Math.min(r1.f1, r2.f1)));

    DataStream<Tuple2<String, Double>> minTempPerWindow2 = sensorData
        .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return Tuple2.of(sensorReading.getId(), sensorReading.getTemperature());
            }
        })
        .keyBy(in -> in.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(15)))
        .reduce(new MinTempFunction());

    DataStream<Tuple2<String, Double>> avgTempPerWindow = sensorData
        .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return Tuple2.of(sensorReading.getId(), sensorReading.getTemperature());
            }
        })
        .keyBy(in -> in.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(15)))
        .aggregate(new AvgTempFunction());

    // output the lowest and highest temperature reading every 5 seconds
    DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData
        .keyBy(SensorReading::getId)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(new HighAndLowTempProcessFunction());

    DataStream<MinMaxTemp> minMaxTempPerWindow2 = sensorData
        .map(new MapFunction<SensorReading, Tuple3<String, Double, Double>>() {
            @Override
            public Tuple3<String, Double, Double> map(SensorReading sensorReading) throws Exception {
                return Tuple3.of(sensorReading.getId(), sensorReading.getTemperature(), sensorReading.getTemperature());
            }
        })
        .keyBy(in -> in.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(
            (r1, r2) -> Tuple3.of(r1.f0, Math.min(r1.f1, r2.f1), Math.max(r1.f2, r2.f2)),
            new AssignWindowEndProcessFunction()
        );

    // print result stream
    minMaxTempPerWindow2.print();

    env.execute();
  }
}

class MinTempFunction implements ReduceFunction<Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> reduce (Tuple2<String, Double> r1, Tuple2<String, Double> r2) {
        return Tuple2.of(r1.f0, Math.min(r1.f1, r2.f1));
    }

}

class AvgTempFunction implements AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Long>, Tuple2<String, Double>> {

    @Override
    public Tuple3<String, Double, Long> createAccumulator() {
        return Tuple3.of("", 0.0, 0L);
    }

    @Override
    public Tuple3<String, Double, Long> add(Tuple2<String, Double> stringDoubleTuple2, Tuple3<String, Double, Long> stringDoubleLongTuple3) {
        return Tuple3.of(stringDoubleLongTuple3.f0, stringDoubleLongTuple3.f1 + stringDoubleTuple2.f1, stringDoubleLongTuple3.f2 + 1);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Long> stringDoubleLongTuple3) {
        return Tuple2.of(stringDoubleLongTuple3.f0, stringDoubleLongTuple3.f1 / stringDoubleLongTuple3.f2);
    }

    @Override
    public Tuple3<String, Double, Long> merge(Tuple3<String, Double, Long> acc1, Tuple3<String, Double, Long> acc2) {
        return Tuple3.of(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2);
    }
}

class MinMaxTemp {
    public String id;
    public Double min;
    public Double max;
    public Long endTs;

    @Override
    public String toString() {
        return "MinMaxTemp{" +
            "id='" + id + '\'' +
            ", min=" + min +
            ", max=" + max +
            ", endTs=" + endTs +
            '}';
    }

    public MinMaxTemp(String id, Double min, Double max, Long endTs) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.endTs = endTs;
    }
}

/**
 * A ProcessWindowFunction that computes the lowest and highest temperature
 * reading per window and emits a them together with the
 * end timestamp of the window.
 */
class HighAndLowTempProcessFunction extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {

    @Override
    public void process(String key,
                        ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>.Context context,
                        Iterable<SensorReading> iterable,
                        Collector<MinMaxTemp> collector) throws Exception {

        Iterator<SensorReading> it = iterable.iterator();
        ArrayDeque<Double> deque = new ArrayDeque<>();

        while (it.hasNext()) deque.add(it.next().getTemperature());

        Long windowEnd = context.window().getEnd();

        collector.collect(new MinMaxTemp(key, deque.stream().min(Comparator.naturalOrder()).get(), deque.stream().max(Comparator.naturalOrder()).get(), windowEnd));

    }
}

class AssignWindowEndProcessFunction extends ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow> {

    @Override
    public void process(String key,
                        ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow>.Context context,
                        Iterable<Tuple3<String, Double, Double>> iterable,
                        Collector<MinMaxTemp> collector) throws Exception {

      Tuple3<String, Double, Double> minMax = iterable.iterator().next();
      Long windowEnd = context.window().getEnd();
      collector.collect(new MinMaxTemp(key, minMax.f1, minMax.f2, windowEnd));
    }
}