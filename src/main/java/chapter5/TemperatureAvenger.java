package chapter5;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.SensorReading;

public class TemperatureAvenger implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<SensorReading> iterable, Collector<SensorReading> collector) throws Exception {

        int i = 0;
        double sum = 0;

        for (SensorReading sr : iterable) {
            i += 1;
            sum += sr.getTemperature();
        }

        collector.collect(new SensorReading(key, window.getEnd(), sum/i));
    }
}
