package util.scala;

class Alert (var message: String, var timestamp: Long){

    def this() = this("", 0)

    override def toString = s"Alert($message, $timestamp)"
}