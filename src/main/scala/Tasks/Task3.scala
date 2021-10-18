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
 * Task3: shows the distribution of each log type across all the logs (aggregation of Task1)
 */
object Task3 {

  /**
   * Mapper for Task3, it takes as input shards of logs and read them line by line.
   */
  class Task3Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val config: Config = ConfigFactory.load("application.conf")
    val logPattern = new Regex(config.getString("task3.logPattern"))
    val logger = CreateLogger(classOf[Task3Mapper])

    // The log line is splitted using spaces ad delimiter
    val logLineDelimiter = config.getString("task3.logLineDelimiter")

    val one = new IntWritable(1)

    /**
     * The Map function produces the key-value pair with the following format:
     *
     * key: <Log type>
     * value: <Regex Instance>
     *
     * @param key     original map key
     * @param value   log line
     * @param context Hadoop context to write key-value pairs
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      logger.info("[TASK 3] Starting map to analyze log lines...")

      val tokens = value.toString.split(logLineDelimiter)

      logger.info("[TASK 3] Log line splitted in tokens...")

      tokens.foreach(token => {
        val logLevel = logPattern.findFirstIn(token).getOrElse(null)
        if (logLevel != null) {

          logger.info(s"[TASK 3] Found log type: $logLevel")

          // The key is composed by the log type being analyzed, the value is the current Regex instance.
          // The length of the array of values (Regex instances) corresponding to each key will be used by the reducer to identify
          // the distribution of log types across all the logs
          context.write(new Text(logLevel), one)

          logger.info("[TASK 3] Context written for current log line [MAP]")
        }
      })
    }
  }

  /**
   * Reducer for Task3, it takes as input the output of the Map task,
   * and produces the distribution of each log type in each predefined time interval
   */
  class Task3Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    val config: Config = ConfigFactory.load("application.conf")
    val logger = CreateLogger(classOf[Task3Reducer])
    val csvDelimiter = config.getString("task3.csvDelimiter")

    /**
     * The Reduce function aggregates the key-value pairs passed by the Mapper in order to count
     * the number of Regex Instances for each log type and for each analyzed time interval
     *
     * key: <Log type>
     * value: <Number of detected Regex Instances>, <List of detected Regex Instances>
     *
     * @param key     The key produced by the Mapper [<Log type>]
     * @param values  The list of Regex Instances for each key
     * @param context Hadoop context to write key-value pairs
     */
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      logger.info("[TASK 3] Starting reducer to analyze log lines...")

      val instancesCount: Int = values.asScala.map(v => v.get()).toList.sum

      logger.info(s"[TASK 3] Occurences of ${key.toString} log type sum done...")

      context.write(key, new IntWritable(instancesCount))

      logger.info("[TASK 3] Context written for current log line [REDUCER]")
    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job = Job.getInstance(configuration, "task3")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task3Mapper])
    job.setReducerClass(classOf[Task3Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}