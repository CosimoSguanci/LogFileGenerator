package Tasks

import HelperUtils.CreateLogger
import Tasks.Utils.TaskUtils
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
    val logger = CreateLogger(classOf[Task1Mapper])

    // The log line is splitted using spaces ad delimiter
    val logLineDelimiter = " "

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      logger.info("Starting map to analyze log lines...")

      val tokens = value.toString.split(logLineDelimiter)

      logger.info("Log line splitted in tokens...")

      // tokens(0) contains the timestamp of the log
      val logTime: LocalTime = LocalTime.parse(tokens(0))

      logger.info("Parsed timestamp of the current analyzed log...")

      val timeIntervalMaps: List[Map[String, LocalTime]] = TaskUtils.parseTimeIntervalsFromConfig(config, "task1.timeIntervals")

      logger.info("Retrieved time intervals from config to be analyzed in log file...")

      val rightTimeInterval: Map[String, LocalTime] = TaskUtils.getRightTimeInterval(logTime, timeIntervalMaps)

      // If the right time interval is not found, this means that the current log doesn't have to be analyzed,
      // because it is not in one of the time intervals specified in config

      if (rightTimeInterval != null) {

        logger.info("Right time interval identified for current log...")

        val k = s"${rightTimeInterval("start")} - ${rightTimeInterval("end")}"

        tokens.foreach(token => {
          val logLevel = logPattern.findFirstIn(token).getOrElse(null)
          if (logLevel != null) {

            logger.info(s"Found log type: $logLevel")

            // The key is composed by the time interval and the log type being analyzed, the value is the current Regex instance.
            // The length of the array of values (Regex instances) corresponding to each key will be used by the reducer to identify
            // the distribution of log types across the time intervals defined in config
            context.write(new Text(s"$k - $logLevel"), new Text(tokens(tokens.length - 1)))

            logger.info("Context written for current log line [MAP]")
          }
        })
      }
    }
  }

  class Task1Reducer extends Reducer[Text, Text, Text, Text] {

    val logger = CreateLogger(classOf[Task1Reducer])
    val csvDelimiter = ","

    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      logger.info("Starting reducer to analyze log lines...")
      
      // We convert Texts to Strings
      val stringInstances = values.asScala.map(v => v.toString).toList

      logger.info("Converted Texts to Strings [Regex instances]...")
      
      // We create a long csv-compliant String that contains all the Regex instances for the current key
      val csvString = stringInstances.mkString(csvDelimiter)

      logger.info("Created csv compliant string with all the Regex instances...")
      
      // The length of the string instances of a certain log type in a certain time interval represents the 
      // distribution that we are searching
      val num = stringInstances.length

      val value = new Text(s"$num,$csvString")
      context.write(key, value)

      logger.info("Context written for current log line [REDUCER]")
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