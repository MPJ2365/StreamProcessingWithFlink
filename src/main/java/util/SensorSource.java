package util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    Boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        // initialize random number generator
        Random rand = new Random();

        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // initialize sensor ids and temperatures
        ArrayDeque<Tuple2<String, Double>> curFTemp = new ArrayDeque<>();

        for (int i = 1; i <= 10; i++) {
            Tuple2<String, Double> val = Tuple2.of("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20));
            curFTemp.add(val);
        }

        // emit data until being canceled
        while (running) {

            Long curTime = Calendar.getInstance().getTimeInMillis();

            // emit new SensorReading
            curFTemp.stream()
                .map(t -> Tuple2.of(t.f0, t.f1 + (rand.nextGaussian() * 0.5)))
                .forEach(t -> sourceContext.collect(new SensorReading(t.f0, curTime, t.f1)));

            // wait for 100 ms
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
