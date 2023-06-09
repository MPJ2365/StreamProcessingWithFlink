package chapter5.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import util.scala.{Alert, SensorReading, SensorSource, SmokeLevel, SmokeLevelSource}

import java.time.Duration

/**
* A simple application that outputs an alert whenever there is a high risk of fire.
* The application receives the stream of temperature sensor readings and a stream of smoke level measurements.
* When the temperature is over a given threshold and the smoke level is high, we emit a fire alert.
*/

object MultiStreamTransformations {

    def main(args: Array[String]): Unit = {

        // set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val strategy: WatermarkStrategy[SensorReading] = WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })

        val tempReadings = env
          .addSource(new SensorSource())
          .assignTimestampsAndWatermarks(strategy)

        val smokeReadings = env
          .addSource(new SmokeLevelSource())
          .setParallelism(1)

        val alerts = tempReadings
          .connect(smokeReadings.broadcast()) // Broadcast, the smoke stream produces very few events (1 per sec)
          .flatMap(new RaiseAlertFlatMap())

        alerts.print()

        // execute the application
        env.execute("Multi-Stream Transformations Example")
    }

    /**
     * A CoFlatMapFunction that processes a stream of temperature readings and a control stream
     * of smoke level events. The control stream updates a shared variable with the current smoke level.
     * For every event in the sensor stream, if the temperature reading is above 100 degrees
     * and the smoke level is high, a "Risk of fire" alert is generated.
     */
    class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, Alert] {

        private var smokeLevel: SmokeLevel = SmokeLevel.LOW

        override def flatMap1(tempReading: SensorReading, out: Collector[Alert]): Unit = {
            // high chance of fire => true
            if (this.smokeLevel == SmokeLevel.HIGH && tempReading.temperature > 100) {
                out.collect(new Alert("Risk of fire! " + tempReading, tempReading.timestamp))
            }
        }

        override def flatMap2(smokeLevel: SmokeLevel, out: Collector[Alert]): Unit = {
            // update smoke level
            this.smokeLevel = smokeLevel
        }
    }
}