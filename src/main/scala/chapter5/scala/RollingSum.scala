package chapter5.scala

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object RollingSum {

     def main(args: Array[String]): Unit = {

        // set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

         val inputStream: DataStream[(Int, Int, Int)] =
             env.fromElements((1, 2, 2), (1, 4, 4), (2, 3, 1), (2, 2, 4), (1, 5, 3))

         val resultStream: DataStream[(Int, Int, Int)] = inputStream
           .keyBy((in: (Int, Int, Int)) => in._1)
           .reduce((t: (Int, Int, Int), t1: (Int, Int, Int)) => {t.copy(_2 = t._2 + t1._2)})

        resultStream.print()

        // execute the application
        env.execute()
    }

}