package Tasks

import Tasks.Utils.TaskUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalTime

class TaskUtilsTest extends AnyFlatSpec with Matchers {
  behavior of "TaskUtils"

  it should "correctly parse time intervals from config" in {
    val config: Config = ConfigFactory.load("test.conf")
    val timeIntervalMaps: List[Map[String, LocalTime]] = TaskUtils.parseTimeIntervalsFromConfig(config, "test.timeIntervals")

    timeIntervalMaps(0)("start") shouldBe LocalTime.parse("11:44:27")
    timeIntervalMaps(0)("end") shouldBe LocalTime.parse("11:44:27.999")

    timeIntervalMaps(1)("start") shouldBe LocalTime.parse("11:44:28")
    timeIntervalMaps(1)("end") shouldBe LocalTime.parse("11:44:28.999")

    timeIntervalMaps(2)("start") shouldBe LocalTime.parse("11:44:29")
    timeIntervalMaps(2)("end") shouldBe LocalTime.parse("11:44:29.999")
  }

  it should "correctly determine the right time interval" in {
    val config: Config = ConfigFactory.load("test.conf")
    val timeIntervalMaps: List[Map[String, LocalTime]] = TaskUtils.parseTimeIntervalsFromConfig(config, "test.timeIntervals")

    val time: LocalTime = LocalTime.parse("11:44:28.231")
    val rightTimeInterval: Map[String, LocalTime] = TaskUtils.getRightTimeInterval(time, timeIntervalMaps)

    rightTimeInterval should not be (null)
    rightTimeInterval("start") shouldBe LocalTime.parse("11:44:28")
    rightTimeInterval("end") shouldBe LocalTime.parse("11:44:28.999")

    val time2: LocalTime = LocalTime.parse("11:44:32.134")
    val rightTimeInterval2: Map[String, LocalTime] = TaskUtils.getRightTimeInterval(time2, timeIntervalMaps)

    rightTimeInterval2 shouldBe (null)
  }
}
