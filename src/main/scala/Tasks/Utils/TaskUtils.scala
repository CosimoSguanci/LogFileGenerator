package Tasks.Utils

import com.typesafe.config.Config

import scala.collection.JavaConverters.*
import java.time.LocalTime
import scala.util.matching.Regex

object TaskUtils {
  def parseTimeIntervalsFromConfig(config: Config, path: String): List[Map[String, LocalTime]] = {
    val timeIntervals = config.getObjectList(path).asScala.toList

    val timeIntervalMaps: List[Map[String, LocalTime]] = timeIntervals.map(timeInterval => {
      val interval = timeInterval.toConfig
      val startTime: LocalTime = LocalTime.parse(interval.getString("start"))
      val endTime: LocalTime = LocalTime.parse(interval.getString("end"))

      Map.apply("start" -> startTime, "end" -> endTime)
    })
    
    return timeIntervalMaps
  }
  
  def getRightTimeInterval(logTime: LocalTime, timeIntervalMaps: List[Map[String, LocalTime]]): Map[String, LocalTime] = {
    val rightTimeInterval: Map[String, LocalTime] = timeIntervalMaps.find(timeIntervalMap => {
      logTime.isAfter(timeIntervalMap("start")) && logTime.isBefore(timeIntervalMap("end"))
    }).getOrElse(null)
    
    return rightTimeInterval
  }
}
