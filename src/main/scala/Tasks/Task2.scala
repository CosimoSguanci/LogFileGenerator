package Tasks

import HelperUtils.CreateLogger
import Tasks.Utils.{DecreasingIntComparator, TaskUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.util.{Comparator, StringTokenizer}
import scala.collection.JavaConverters.*
import scala.util.matching.Regex

/**
 * Task2: shows time intervals sorted in descending order by number of ERROR log messages
 */
object Task2 {

  /**
   * Mapper for job #1 of Task2, it takes as input shards of logs and read them line by line.
   */
  class Task2Mapper extends Mapper[Object, Text, Text, Text] {

    val logPattern = new Regex("(ERROR)")
    val config: Config = ConfigFactory.load("application.conf")
    val logger = CreateLogger(classOf[Task2Mapper])

    // The log line is splitted using spaces ad delimiter
    val logLineDelimiter = " "

    /**
     * The Map function produces the key-value pair with the following format:
     *
     * key: <Time Interval>
     * value: <Regex Instance>
     *
     * If the current analyzed log timestamp is not included in the configured time intervals to be analyzed, it gets skipped.
     *
     * @param key original map key
     * @param value log line
     * @param context Hadoop context to write key-value pairs
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      logger.info("Starting map to analyze log lines...")

      val tokens = value.toString.split(logLineDelimiter)

      logger.info("Log line splitted in tokens...")

      // tokens(0) contains the timestamp of the log
      val logTime: LocalTime = LocalTime.parse(tokens(0))

      logger.info("Parsed timestamp of the current analyzed log...")

      val timeIntervalMaps: List[Map[String, LocalTime]] = TaskUtils.parseTimeIntervalsFromConfig(config, "task2.timeIntervals")

      logger.info("Retrieved time intervals from config to be analyzed in log file...")

      val rightTimeInterval: Map[String, LocalTime] = TaskUtils.getRightTimeInterval(logTime, timeIntervalMaps)

      // If the right time interval is not found, this means that the current log doesn't have to be analyzed,
      // because it is not in one of the time intervals specified in config

      if (rightTimeInterval != null) {

        logger.info("Right time interval identified for current log...")

        // The key is represented by the Time Interval, all numbers will be unambiguously referred to the ERROR log messages,
        // unlike Task 1 in which we had to divide the output for each possible log message type
        val k = s"${rightTimeInterval("start")} - ${rightTimeInterval("end")}"

        tokens.foreach(t => {
          val logLevel = logPattern.findFirstIn(t).getOrElse(null)
          if (logLevel != null) {

            logger.info(s"Found log type: ERROR")

            // It is an error log line
            context.write(new Text(k), new Text(tokens(tokens.length - 1)))

            logger.info("Context written for current log line [MAP]")
          }

        })
      }
    }
  }

  /**
   * Reducer for job #1 of Task2 it takes as input the output of the Map task,
   * and produces the number of ERROR messages in each predefined time interval
   */
  class Task2Reducer extends Reducer[Text, Text, Text, Text] {

    val logger = CreateLogger(classOf[Task2Reducer])
    val csvDelimiter = ","

    /**
     * The Reduce function aggregates the key-value pairs passed by the Mapper in order to count
     * the number of Regex Instances relative to the ERROR log type, for each time interval in the configuration
     *
     * key: <Time Interval>
     * value: <Number of detected Regex Instances>, <List of detected Regex Instances>
     *
     * @param key The key produced by the Mapper [<Time Interval>]
     * @param values The list of Regex Instances for each key
     * @param context Hadoop context to write key-value pairs
     */
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      logger.info("[TASK 2] Starting reducer to analyze log lines...")

      // We convert Texts to Strings
      val stringInstances = values.asScala.map(v => v.toString).toList

      logger.info("Converted Texts to Strings [Regex instances]...")

      // We create a long csv-compliant String that contains all the Regex instances for the current key
      val csvString = stringInstances.mkString(csvDelimiter)

      logger.info("Created the csv-compliant string that includes all the Regex instances...")

      // The length of the string instances of a certain log type in a certain time interval represents the
      // distribution that we are searching
      val num = stringInstances.length

      val value = new Text(s"$num,$csvString")
      context.write(key, value)

      logger.info("Context written for current log line [REDUCER]")
    }
  }

  /**
   * Mapper for job #2 of Task2, it takes as input the output of job #1 and read it line by line.
   */
  class Task2SortMapper extends Mapper[Object, Text, IntWritable, Text] {

    val csvDelimiter = ","

    /**
     * The Map function produces the key-value pair with the following format:
     *
     * key: <Number of ERROR log messages in a certain time interval>
     * value: <Time Interval> <Regex Instance>
     *
     * In practice this Map task swaps the key returned by the previous job and a part of the value.
     * Doing this allows us to use the number of ERROR message occurrences in a certain time interval as KEY, and therefore
     * allows the use of a Comparator to sort the results by KEY, achieving the desired descending order.
     *
     * @param key the time interval
     * @param value the line of the results of job #1
     * @param context Hadoop context to write key-value pairs
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val stringValues = value.toString
      val values = stringValues.split(csvDelimiter).toList

      // values(1) contains the number of occurrences of ERROR messages in the current line of job #1 output
      val newKey = values(1)

      // We remove the new key from the previous values
      val newValues = values.filter(v => !v.equals(newKey))

      // We write the new key-value pairs in the new format (num : time interval + instances)
      context.write(new IntWritable(newKey.toInt), new Text(newValues.mkString(csvDelimiter)))
    }
  }

/*  /**
   * Reducer for job #2 of Task2, it takes as input the output of job #2 Map and decouples the time intervals
   * that have the same number of occurences of ERROR log messages
   */
  class Task2SortReducer extends Reducer[IntWritable, Text, IntWritable, Text] {

    /**
     * The Reduce function decouples the time intervals that have the same number of occurences of ERROR log messages
     * to show them in separate lines
     *
     * key: <Number of ERROR log messages in a certain time interval>
     * value: <Time Interval> <Regex Instance>
     *
     * @param key The key produced by the Mapper
     * @param values The time interval and the list of Regex Instances for each key
     * @param context Hadoop context to write key-value pairs
     */
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[Text, Text, IntWritable, Text]#Context): Unit = {

      // We convert Texts to Strings
      val stringInstances: List[String] = values.asScala.map(v => v.toString).toList

      // As of now, if two time intervals A and B had the same number of ERROR log messages, they are associated by the same key.
      // We decouple them by rewriting the context
      stringInstances.foreach(entry => {
        context.write(key, new Text(entry))
      })
    }
  }*/


  def main(args: Array[String]): Unit = {
    // JOB 1
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job = Job.getInstance(configuration, "task2")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task2Mapper])
    job.setReducerClass(classOf[Task2Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)

    // JOB 2
    val configuration2 = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    val job2 = Job.getInstance(configuration, "task2 sort")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[Task2SortMapper])
    //job2.setReducerClass(classOf[Task2SortReducer])
    job2.setOutputKeyClass(classOf[IntWritable])
    job2.setOutputValueClass(classOf[Text])

    // Custom Sort Comparator for descending ordering
    job2.setSortComparatorClass(classOf[DecreasingIntComparator])
    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))
    System.exit(if (job2.waitForCompletion(true)) 0 else 1)
  }
}