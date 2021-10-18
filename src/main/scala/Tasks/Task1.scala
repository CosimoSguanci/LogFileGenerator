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

/**
 * Task1: shows the distribution of different log types across predefined time intervals,
 * also showing the injected Regex String instances injected by the Log generator
 */
object Task1 {

  /**
   * Mapper for Task1, it takes as input shards of logs and read them line by line.
   */
  class Task1Mapper extends Mapper[Object, Text, Text, Text] {

    val config: Config = ConfigFactory.load("application.conf")
    val logPattern = new Regex(config.getString("task1.logPattern"))
    val logger = CreateLogger(classOf[Task1Mapper])

    // The log line is splitted using spaces ad delimiter
    val logLineDelimiter = config.getString("task1.logLineDelimiter")

    /**
     * The Map function produces the key-value pair with the following format:
     *
     * key: <Time Interval> + <Log type>
     * value: <Regex Instance>
     *
     * If the current analyzed log timestamp is not included in the configured time intervals to be analyzed, it gets skipped.
     *
     * @param key     original map key
     * @param value   log line
     * @param context Hadoop context to write key-value pairs
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      logger.info("[TASK 1] Starting map to analyze log lines...")

      val tokens = value.toString.split(logLineDelimiter)

      logger.info("[TASK 1] Log line splitted in tokens...")

      // tokens(0) contains the timestamp of the log
      val logTime: LocalTime = LocalTime.parse(tokens(0))

      logger.info("[TASK 1] Parsed timestamp of the current analyzed log...")

      val timeIntervalMaps: List[Map[String, LocalTime]] = TaskUtils.parseTimeIntervalsFromConfig(config, "task1.timeIntervals")

      logger.info("[TASK 1] Retrieved time intervals from config to be analyzed in log file...")

      val rightTimeInterval: Map[String, LocalTime] = TaskUtils.getRightTimeInterval(logTime, timeIntervalMaps)

      // If the right time interval is not found, this means that the current log doesn't have to be analyzed,
      // because it is not in one of the time intervals specified in config

      if (rightTimeInterval != null) {

        logger.info("[TASK 1] Right time interval identified for current log...")

        val k = s"${rightTimeInterval("start")} - ${rightTimeInterval("end")}"

        tokens.foreach(token => {
          val logLevel = logPattern.findFirstIn(token).getOrElse(null)
          if (logLevel != null) {

            logger.info(s"[TASK 1] Found log type: $logLevel")

            // The key is composed by the time interval and the log type being analyzed, the value is the current Regex instance.
            // The length of the array of values (Regex instances) corresponding to each key will be used by the reducer to identify
            // the distribution of log types across the time intervals defined in config
            context.write(new Text(s"$k - $logLevel"), new Text(tokens(tokens.length - 1)))

            logger.info("[TASK 1] Context written for current log line [MAP]")
          }
        })
      }
    }
  }

  /**
   * Reducer for Task1, it takes as input the output of the Map task,
   * and produces the distribution of each log type in each predefined time interval
   */
  class Task1Reducer extends Reducer[Text, Text, Text, Text] {

    val config: Config = ConfigFactory.load("application.conf")
    val logger = CreateLogger(classOf[Task1Reducer])
    val csvDelimiter = config.getString("task1.csvDelimiter")

    /**
     * The Reduce function aggregates the key-value pairs passed by the Mapper in order to count
     * the number of Regex Instances for each log type and for each analyzed time interval
     *
     * key: <Time Interval> + <Log type>
     * value: <Number of detected Regex Instances>, <List of detected Regex Instances>
     *
     * @param key     The key produced by the Mapper [<Time Interval> + <Log type>]
     * @param values  The list of Regex Instances for each key
     * @param context Hadoop context to write key-value pairs
     */
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      logger.info("[TASK 1] Starting reducer to analyze log lines...")

      // We convert Texts to Strings
      val stringInstances = values.asScala.map(v => v.toString).toList

      logger.info("[TASK 1] Converted Texts to Strings [Regex instances]...")

      // We create a long csv-compliant String that contains all the Regex instances for the current key
      val csvString = stringInstances.mkString(csvDelimiter)

      logger.info("[TASK 1] Created the csv-compliant string that includes all the Regex instances...")

      // The length of the string instances of a certain log type in a certain time interval represents the
      // distribution that we are searching
      val num = stringInstances.length

      val value = new Text(s"$num,$csvString")
      context.write(key, value)

      logger.info("[TASK 1] Context written for current log line [REDUCER]")
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
}