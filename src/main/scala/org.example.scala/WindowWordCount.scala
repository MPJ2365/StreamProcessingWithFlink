package org.example.scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowWordCount {

    // Flink does not like scala tuples in sum("field"). Must use POJO.
    case class WC(var word: String, var count: Int) {
        def this() = this("", 1)
    }

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        val dataStream: DataStream[WC] = env
            .socketTextStream("localhost", 9999)
            .flatMap[WC](new Splitter())
            .keyBy((wc: WC) => wc.word)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum("count")

        dataStream.print()

        env.execute("Window WordCount")
    }

    class Splitter extends FlatMapFunction[String, WC] {
        override def flatMap(t: String, collector: Collector[WC]): Unit =
            t.split("\\s+").foreach(str => collector.collect(WC(str, 1)))
    }

}
