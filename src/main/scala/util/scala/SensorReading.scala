package util.scala

class SensorReading(var id: String, var timestamp: Long, var temperature: Double) {

  def this() = this("", 0L, 0)

  override def toString: String = s"SensorReading($id, $timestamp, $temperature)"

}
