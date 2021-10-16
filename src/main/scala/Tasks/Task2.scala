package Tasks

import Tasks.Utils.DecreasingIntComparator
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

object Task2 {

  class Task2Mapper extends Mapper[Object, Text, Text, Text] {

    val logPattern = new Regex("(ERROR)")

    val config: Config = ConfigFactory.load("application.conf")

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      val tokens = value.toString.split(" ")

      val time: LocalTime = LocalTime.parse(tokens(0))

      val timeIntervals = config.getObjectList("task2.timeIntervals").asScala.toList

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
            // It is an error log line
            context.write(new Text(k), new Text(tokens(tokens.length - 1)))
          }

        })
      }
    }
  }

  class Task2Reducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

      val stringInstances = values.asScala.map(v => v.toString).toList

      val csvString = stringInstances.mkString(",")
      val num = stringInstances.length

      val value = new Text()
      value.set(s"$num,$csvString")
      context.write(key, value)

    }
  }

  class Task2SortMapper extends Mapper[Object, Text, IntWritable, Text] {


    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val stringValues = value.toString
      val values = stringValues.split(',').toList
      val newKey = values(1)
      val newValues = values.filter(v => !v.equals(newKey))
      context.write(new IntWritable(newKey.toInt), new Text(newValues.mkString(",")))
    }
  }

  class Task2SortReducer extends Reducer[IntWritable, Text, IntWritable, Text] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[Text, Text, IntWritable, Text]#Context): Unit = {

      val stringInstances: List[String] = values.asScala.map(v => v.toString).toList


      stringInstances.foreach(entry => {
        context.write(key, new Text(entry))
      })

    }
  }


  def main(args: Array[String]): Unit = {
    // JOB 1
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job = Job.getInstance(configuration, "task2")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task2Mapper])
    job.setReducerClass(classOf[Task2Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.waitForCompletion(true)

    // JOB 2
    val configuration2 = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job2 = Job.getInstance(configuration, "task2 sort")
    job2.setJarByClass(this.getClass)
    job2.setMapperClass(classOf[Task2SortMapper])
    job2.setReducerClass(classOf[Task2SortReducer])
    job2.setOutputKeyClass(classOf[IntWritable])
    job2.setOutputValueClass(classOf[Text]);
    job2.setSortComparatorClass(classOf[DecreasingIntComparator])
    FileInputFormat.addInputPath(job2, new Path(args(1)))
    FileOutputFormat.setOutputPath(job2, new Path(args(2)))
    System.exit(if (job2.waitForCompletion(true)) 0 else 1)
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