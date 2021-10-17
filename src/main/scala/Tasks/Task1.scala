package Tasks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.util.matching.Regex

object Task1 {

  class Task1Mapper extends Mapper[Object, Text, Text, Text] {

    val logPattern = new Regex("(DEBUG)|(INFO)|(WARN)|(ERROR)")

    val config: Config = ConfigFactory.load("application.conf")

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val tokens = value.toString.split(" ")

      val time: LocalTime = LocalTime.parse(tokens(0))

      val timeIntervals = config.getObjectList("task1.timeIntervals").asScala.toList

      val timeIntervalMaps: List[Map[String, LocalTime]] = timeIntervals.map(timeInterval => {
        val interval = timeInterval.toConfig
        val startTime: LocalTime = LocalTime.parse(interval.getString("start"))
        val endTime: LocalTime = LocalTime.parse(interval.getString("end"))

        Map.apply("start" -> startTime, "end" -> endTime)
      })

      val rightTimeInterval: Map[String, LocalTime] = timeIntervalMaps.find(timeIntervalMap => {
        time.isAfter(timeIntervalMap("start")) && time.isBefore(timeIntervalMap("end"))
      }).getOrElse(null)

      if (rightTimeInterval != null) {
        val k = s"${rightTimeInterval("start")} - ${rightTimeInterval("end")}"

        tokens.foreach(t => {
          val logLevel = logPattern.findFirstIn(t).getOrElse(null)
          if (logLevel != null) {
            context.write(new Text(s"$k - $logLevel"), new Text(tokens(tokens.length - 1)))
          }
        })
      }
    }
  }

  class Task1Reducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      val stringInstances = values.asScala.map(v => v.toString).toList
      val csvString = stringInstances.mkString(",")
      val num = stringInstances.length

      val value = new Text()
      value.set(s"$num,$csvString")
      context.write(key, value)

    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job = Job.getInstance(configuration, "task1")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task1Mapper])
    job.setReducerClass(classOf[Task1Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

  //  def test(): Unit = {
  //    val pattern = new Regex("(TRACE)|(DEBUG)|(INFO)|(WARN)|(ERROR)|(FATAL)")
  //
  //    val value = "17:44:16.682 [scala-execution-context-global-116] WARN  HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z"
  //    val tokens = value.toString.split(" ")
  //
  //    tokens.foreach(t => {
  //      val logLevel = pattern.findFirstIn(t).getOrElse(null)
  //      if(logLevel != null) {
  //        print(tokens(tokens.length - 1))
  //      }
  //
  //    })
  //  }

}