package util.scala

// Does not work:
//sealed class SmokeLevel(level: String, ordinal: Int) extends java.lang.Enum[SmokeLevel](level, ordinal)
//
//object SmokeLevel {
//    case object LOW extends SmokeLevel("LOW", 0)
//    case object HIGH extends SmokeLevel("HIGH", 1)
//}

sealed trait SmokeLevel

object SmokeLevel {
    case object LOW extends SmokeLevel
    case object HIGH extends SmokeLevel
}