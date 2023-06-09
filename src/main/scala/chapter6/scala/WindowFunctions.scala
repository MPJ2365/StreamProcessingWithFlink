package chapter6.scala

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, ReduceFunction}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.scala.{SensorReading, SensorSource, WatermarkStrategies}

import java.lang

object WindowFunctions {

  def threshold = 25.0

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(WatermarkStrategies.sensorReadingStrategy)

    val minTempPerWindow: DataStream[(String, Double)] = sensorData
      .map(new MapFunction[SensorReading, (String, Double)] {
        override def map(t: SensorReading): (String, Double) = (t.id, t.temperature)
      })
      .keyBy((in: (String, Double)) => in._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .reduce((r1, r2) => (r1._1, r1._2 min r2._2))

    val minTempPerWindow2: DataStream[(String, Double)] = sensorData
      .map(new MapFunction[SensorReading, (String, Double)] {
        override def map(t: SensorReading): (String, Double) = (t.id, t.temperature)
      })
      .keyBy((in: (String, Double)) => in._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .reduce(new MinTempFunction)

    val avgTempPerWindow: DataStream[(String, Double)] = sensorData
      .map(new MapFunction[SensorReading, (String, Double)] {
        override def map(t: SensorReading): (String, Double) = (t.id, t.temperature)
      })
      .keyBy((in: (String, Double)) => in._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
      .aggregate(new AvgTempFunction)

    // output the lowest and highest temperature reading every 5 seconds
    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      .keyBy((in: SensorReading) => in.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new HighAndLowTempProcessFunction)

    val minMaxTempPerWindow2: DataStream[MinMaxTemp] = sensorData
      .map(new MapFunction[SensorReading, (String, Double, Double)] {
        override def map(t: SensorReading): (String, Double, Double) = (t.id, t.temperature, t.temperature)
      })
      .keyBy((in: (String, Double, Double)) => in._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce(
        // incrementally compute min and max temperature
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2 min r2._2, r1._3 max r2._3)
        },
        // finalize result in ProcessWindowFunction
        new AssignWindowEndProcessFunction()
      )

    // print result stream
    minMaxTempPerWindow2.print()

    env.execute()
  }
}

class MinTempFunction extends ReduceFunction[(String, Double)] {
  override def reduce(r1: (String, Double), r2: (String, Double)): (String, Double) = (r1._1, r1._2 min r2._2)
}

class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {

  override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

  override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = (in._1, in._2 + acc._2, 1 + acc._3)

  override def getResult(acc: (String, Double, Int)): (String, Double) = (acc._1, acc._2 / acc._3)

  override def merge(acc1: (String, Double, Int), acc2: (String, Double, Int)): (String, Double, Int) = (acc1._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
}

case class MinMaxTemp(id: String, min: Double, max:Double, endTs: Long)

/**
 * A ProcessWindowFunction that computes the lowest and highest temperature
 * reading per window and emits a them together with the
 * end timestamp of the window.
 */
class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {

  override def process(key: String,
                       ctx: ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow]#Context,
                       vals: lang.Iterable[SensorReading],
                       collector: Collector[MinMaxTemp]): Unit = {

    val temps = collection.mutable.ArrayBuffer[Double]()
    val it = vals.iterator()
    while (it.hasNext) temps += it.next().temperature

    val windowEnd = ctx.window.getEnd

    collector.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))

  }
}

class AssignWindowEndProcessFunction extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {

  override def process(key: String,
                       ctx: ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow]#Context,
                       iterable: lang.Iterable[(String, Double, Double)],
                       collector: Collector[MinMaxTemp]): Unit = {

    val minMax = iterable.iterator().next()
    val windowEnd = ctx.window.getEnd
    collector.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
  }
}