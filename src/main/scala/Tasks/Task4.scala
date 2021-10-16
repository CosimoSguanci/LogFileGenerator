package Tasks

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

object Task4 {

  class Task4Mapper extends Mapper[Object, Text, Text, Text] {
    
    val logPattern = new Regex("(DEBUG)|(INFO)|(WARN)|(ERROR)")

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val tokens = value.toString.split(" ")

      tokens.foreach(t => {
        val logLevel = logPattern.findFirstIn(t).getOrElse(null)
        if (logLevel != null) {
          context.write(new Text(logLevel), new Text(tokens(tokens.length - 1)))
        }

      })
    }
  }

  class Task4Reducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val strings: List[String] = values.asScala.map(v => v.toString).toList
      val maxNumberOfCharacters = strings.map(s => s.length).max
      context.write(key, new Text(maxNumberOfCharacters.toString))

    }
  }

  def main(args: Array[String]): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",");
    val job = Job.getInstance(configuration, "task4")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[Task4Mapper])
    job.setReducerClass(classOf[Task4Reducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}