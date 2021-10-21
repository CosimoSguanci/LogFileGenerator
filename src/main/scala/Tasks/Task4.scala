package Tasks

import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.util.matching.Regex

/**
 * Task4: show, for each log message type, the maximum number of characters found in the Regex string instances injected by the log generator
 */
object Task4 {

  /**
   * Mapper for Task4, it takes as input shards of logs and read them line by line.
   */
  class Task4Mapper extends Mapper[Object, Text, Text, Text] {

    val config: Config = ConfigFactory.load("application.conf")
    val logPattern = new Regex(config.getString("task4.logPattern"))
    val logger = CreateLogger(classOf[Task4Mapper])

    // The log line is splitted using spaces ad delimiter
    val logLineDelimiter = config.getString("task4.logLineDelimiter")

    /**
     * The Map function produces the key-value pair with the following format:
     *
     * key: <Log type>
     * value: <Regex Instance>
     *
     *
     * @param key     original map key
     * @param value   log line
     * @param context Hadoop context to write key-value pairs
     */
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      logger.info("[TASK 4] Starting map to analyze log lines...")

      val tokens = value.toString.split(logLineDelimiter)

      logger.info("[TASK 4] Log line splitted in tokens...")

      tokens.foreach(token => {
        val logLevel = logPattern.findFirstIn(token).getOrElse(null)
        if (logLevel != null) {

          logger.info(s"[TASK 4] Found log type: $logLevel")

          // The key is composed by the log type being analyzed, the value is the current Regex instance.
          // The array of strings associated to a certian log message type will be used by the reducer to compute the 
          // maximum number of characters for each log message type
          context.write(new Text(logLevel), new Text(tokens(tokens.length - 1)))

          logger.info("[TASK 4] Context written for current log line [MAP]")
        }
      })
    }
  }

  /**
   * Reducer for Task4, it takes as input the output of the Map task,
   * and produces the maximum number of characters in string instances for each
   * log message type
   */
  class Task4Reducer extends Reducer[Text, Text, Text, Text] {

    val logger = CreateLogger(classOf[Task4Reducer])
    
    /**
     * The Reduce function aggregates the key-value pairs passed by the Mapper in order to determine
     * the maximum number of characters for instances of strings, divided by log message type. The format of
     * the output is the following:
     *
     * key: <Log type>
     * value: <Maximum number of characters in string instances>
     *
     * @param key     The key produced by the Mapper [<Log type>]
     * @param values  The list of Regex Instances for each key
     * @param context Hadoop context to write key-value pairs
     */
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      logger.info("[TASK 4] Starting reducer to analyze log lines...")
      
      val strings: List[String] = values.asScala.map(v => v.toString).toList

      logger.info("[TASK 4] Converted Texts to Strings [Regex instances]...")
      
      val maxNumberOfCharacters = strings.map(s => s.length).max

      logger.info(s"[TASK 4] Max number of characters for ${key.toString} message type computed...")
      
      context.write(key, new Text(maxNumberOfCharacters.toString))

      logger.info("[TASK 4] Context written for current log line [REDUCER]")
    }
  }

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load("application.conf")
    val csvDelimiter = config.getString("task4.csvDelimiter")

    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", csvDelimiter)
    val job = Job.getInstance(configuration, "task4")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task4Mapper])
    job.setReducerClass(classOf[Task4Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setNumReduceTasks(1)
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}