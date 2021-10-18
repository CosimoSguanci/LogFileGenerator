package Tasks.Utils

import com.typesafe.config.Config

import scala.collection.JavaConverters.*
import java.time.LocalTime
import scala.util.matching.Regex

/**
 * Singleton object made to share common methods across different tasks
 */
object TaskUtils {

  /**
   * Parses all predefined time intervals in the configuration, from Strings to LocalTime
   * 
   * @param config the configuration for the current task
   * @param path the path of the time intervals in the config
   * @return a List of Maps corresponding to the predefined time intervals. For each time interval, the format is the following:
   *         "start" -> <time>
   *         "end" -> <time>  
   */
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

  /**
   * Returns the correct time interval for the log line that is currently being analyzed, if it is included in a time interval
   * inserted in the configuration
   * 
   * @param logTime the timestamp of the current log
   * @param timeIntervalMaps all the time intervals inserted in the configuration
   * @return the Map corresponding to the right time interval if there's one, null otherwise
   */
  def getRightTimeInterval(logTime: LocalTime, timeIntervalMaps: List[Map[String, LocalTime]]): Map[String, LocalTime] = {
    val rightTimeInterval: Map[String, LocalTime] = timeIntervalMaps.find(timeIntervalMap => {
      logTime.isAfter(timeIntervalMap("start")) && logTime.isBefore(timeIntervalMap("end"))
    }).getOrElse(null)
    
    return rightTimeInterval
  }
}
