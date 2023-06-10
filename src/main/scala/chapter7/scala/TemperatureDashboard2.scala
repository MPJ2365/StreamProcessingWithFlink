package chapter7.scala

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation, Types}
import org.apache.flink.configuration.Configuration
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import util.scala.{SensorReading, SensorSource, WatermarkStrategies}

import java.util.concurrent.CompletableFuture

case class MaxTemperature(var id: String, var temperature: Double) {
  def this() = this("", 0)
}

// THIS WAS NOT EASY.
// THE SERIALIZER WORKS WHEN USING FLINK TUPLE2 WITH JAVA.LANG.DOUBLE (WITH SCALA DOUBLE FAILS).
// IT ALSO WORKS WITH POJO. BUT WE NEED TO CREATE THE SERIALIZER EXPLICITLY IN THE DESCRIPTOR. THIS TIME, IT WORKS WITH SCALA DOUBLE.
// IT ALSO WORKS WITH SCALA TUPLES, BUT IF WE USE SCALA DOUBLE, WE GET WARNINGS. WITH JAVA DOUBLE, IT WORKS PERFECTLY.

object TrackMaximumTemperature2 {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val conf = new Configuration()
    conf.setString("queryable-state.enable", "true") // Need to enable this config to query state. As well as adding some dependencies to the pom.
    val env = StreamExecutionEnvironment.getExecutionEnvironment(conf)

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(WatermarkStrategies.sensorReadingStrategy)

    val tenSecsMaxTemps: DataStream[MaxTemperature] = sensorData
      // project to sensor id and temperature
      .map(new MapFunction[SensorReading, MaxTemperature] {
        override def map(t: SensorReading): MaxTemperature = MaxTemperature(t.id, t.temperature)
      })
      // compute every 10 seconds the max temperature per sensor
      .keyBy((sr: MaxTemperature) => sr.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce((t: MaxTemperature, t1: MaxTemperature) => MaxTemperature(t.id, Math.max(t.temperature, t1.temperature)))

    // store latest value for each sensor in a queryable state
    tenSecsMaxTemps
      .keyBy((sr: MaxTemperature) => sr.id)
      .asQueryableState("maxTemperature")

    // execute application
    env.execute("Track max temperature")
  }
}

object TemperatureDashboard2 {

  // queryable state proxy connection information.
  // can be looked up in logs of running QueryableStateJob
  private val proxyHost = "127.0.0.1"
  private val proxyPort = 9069 // Default port

  // jobId of running QueryableStateJob.
  // Execute first job "TrackMaximumTemperature" and look up the jobId in logs of running job or the web UI
  private val jobId = "e5b8e9c663d3cbc2b738a83296357b0e"

  // how many sensors to query
  private val numSensors = 5
  // how often to query
  private val refreshInterval = 10000

  def main(args: Array[String]): Unit = {

    // configure client with host and port of queryable state proxy
    val client = new QueryableStateClient(proxyHost, proxyPort)

    val futures = new Array[CompletableFuture[ValueState[MaxTemperature]]](numSensors)
    val results = new Array[Double](numSensors)

    // print header line of dashboard table
    val header = (for (i <- 0 until numSensors) yield "sensor_" + (i + 1)).mkString("\t| ")
    println(header)

    // loop forever
    while (true) {

      // send out async queries
      for (i <- 0 until numSensors) {
        futures(i) = queryState("sensor_" + (i + 1), client)
      }
      // wait for results
      for (i <- 0 until numSensors) {
        results(i) = futures(i).get().value().temperature
      }
      // print result
      val line = results.map(t => f"$t%1.3f").mkString("\t| ")
      println(line)

      // wait to send out next queries
      Thread.sleep(refreshInterval)
    }

    client.shutdownAndWait()

  }

  private def queryState(key: String, client: QueryableStateClient): CompletableFuture[ValueState[MaxTemperature]] = {

    client.getKvState[String, ValueState[MaxTemperature], MaxTemperature](
      JobID.fromHexString(jobId),
      "maxTemperature",
      key,
      Types.STRING,
      new ValueStateDescriptor[MaxTemperature]("", TypeInformation.of(new TypeHint[MaxTemperature]() {}).createSerializer(new ExecutionConfig)))
  }

}