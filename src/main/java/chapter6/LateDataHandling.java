package chapter6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.SensorReading;
import util.SensorSource;
import util.WatermarkStrategies;

import java.util.Random;

public class LateDataHandling {

  // define a side output tag
    static OutputTag<SensorReading> lateReadingsOutput = new OutputTag<SensorReading>("late-readings"){};

    public static void main(String[] args) throws Exception {

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

    // ingest sensor stream and shuffle timestamps to produce out-of-order records
    DataStream<SensorReading> outOfOrderReadings = env.addSource(new SensorSource())
      // shuffle timestamps by max 7 seconds to generate late data
      .map(new TimestampShuffler(7 * 1000))
      // assign timestamps and watermarks with an offset of 5 seconds
      .assignTimestampsAndWatermarks(new WatermarkStrategies().sensorReadingStrategy);

    // Different strategies to handle late records.
    // Select and uncomment on of the lines below to demonstrate a strategy.

    // 1. Filter out late readings (to a side output) using a ProcessFunction
    // filterLateReadings(outOfOrderReadings);
    // 2. Redirect late readings to a side output in a window operator
    // sideOutputLateEventsWindow(outOfOrderReadings);
    // 3. Update results when late readings are received in a window operator
    updateForLateEventsWindow(outOfOrderReadings);

    env.execute();
  }

  /** Filter late readings to a side output and print the on-time and late streams. */
  private static void filterLateReadings(DataStream<SensorReading> readings) {
    // re-direct late readings to the side output
    SingleOutputStreamOperator<SensorReading> filteredReadings = readings.process(new LateReadingsFilter());

    // retrieve late readings
    DataStream<SensorReading> lateReadings = filteredReadings.getSideOutput(lateReadingsOutput);

    // print the filtered stream
    filteredReadings.print();

    // print messages for late readings
    lateReadings
      .map(r -> "*** late reading *** " + r.getId())
      .print();
  }

  /** Count reading per tumbling window and emit late readings to a side output.
   * Print results and late events. */
  private static void sideOutputLateEventsWindow(DataStream<SensorReading> readings) {

    SingleOutputStreamOperator<Tuple3<String, Long, Integer>> countPer10Secs = readings
            .keyBy(SensorReading::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // emit late readings to a side output
            .sideOutputLateData(lateReadingsOutput)
            .process(new ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
              @Override
              public void process(String s,
                                  ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>.Context context,
                                  Iterable<SensorReading> iterable,
                                  Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                int cnt = 0;
                for (SensorReading ignored : iterable) cnt += 1;

                collector.collect(Tuple3.of(s, context.window().getEnd(), cnt));
              }
            });

    // retrieve and print messages for late readings
    countPer10Secs
      .getSideOutput(lateReadingsOutput)
      .map(r -> "*** late reading *** " + r.getId())
      .print();

    // print results
    countPer10Secs.print();
  }

  /** Count reading per tumbling window and update results if late readings are received.
   * Print results. */
  private static void updateForLateEventsWindow(DataStream<SensorReading> readings) {

    DataStream<Tuple4<String, Long, Integer, String>> countPer10Secs = readings
            .keyBy(SensorReading::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // process late readings for 5 additional seconds
            .allowedLateness(Time.seconds(5))
            // count readings and update results if late readings arrive
            .process(new UpdatingWindowCountFunction());

    // print results
    countPer10Secs.print();
  }
}

/** A ProcessFunction that filters out late sensor readings and re-directs them to a side output */
class LateReadingsFilter extends ProcessFunction<SensorReading, SensorReading> {
  @Override
  public void processElement(SensorReading sensorReading,
                             ProcessFunction<SensorReading, SensorReading>.Context context,
                             Collector<SensorReading> collector) throws Exception {

    OutputTag<SensorReading> lateReadingsOutput = new OutputTag<SensorReading>("late-readings"){};

    // compare record timestamp with current watermark
    if (sensorReading.getTimestamp() < context.timerService().currentWatermark()) {
      // this is a late reading => redirect it to the side output
      context.output(lateReadingsOutput, sensorReading);
    }
    else collector.collect(sensorReading);
  }
}

/** A counting WindowProcessFunction that distinguishes between first results and updates. */
class UpdatingWindowCountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer, String>, String, TimeWindow> {

  @Override
  public void clear(ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer, String>, String, TimeWindow>.Context context) throws Exception {
    ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<>("isUpdate", Types.BOOLEAN));
    isUpdate.clear();
  }

  @Override
  public void process(String key,
                      ProcessWindowFunction<SensorReading, Tuple4<String, Long, Integer, String>, String, TimeWindow>.Context context,
                      Iterable<SensorReading> iterable,
                      Collector<Tuple4<String, Long, Integer, String>> collector) throws Exception {
    // count the number of readings
    int cnt = 0;
    for (SensorReading ignored : iterable) cnt += 1;

    // state to check if this is the first evaluation of the window or not.
    // Internal state to the window. We must ensure we clean it up by implementing the clear method.
    ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<>("isUpdate", Types.BOOLEAN));

    if (isUpdate.value() == null) {
      // first evaluation, emit first result
      collector.collect(Tuple4.of(key, context.window().getEnd(), cnt, "first"));
      isUpdate.update(true);
    }
    else {
      // not the first evaluation, emit an update
      collector.collect(Tuple4.of(key, context.window().getEnd(), cnt, "update"));
    }
  }
}

/** A MapFunction to shuffle (up to a max offset) the timestamps of SensorReadings to produce out-of-order events. */
class TimestampShuffler implements MapFunction<SensorReading, SensorReading> {

  int maxRandomOffset;

  public TimestampShuffler(int maxRandomOffset) {
    this.maxRandomOffset = maxRandomOffset;
  }

  Random rand = new Random();

  @Override
  public SensorReading map(SensorReading sensorReading) throws Exception {
    long shuffleTs = sensorReading.getTimestamp() + rand.nextInt(maxRandomOffset);
    return new SensorReading(sensorReading.getId(), shuffleTs, sensorReading.getTemperature());
  }
}