package Tasks


import Tasks.Task2.Task2Mapper
import org.mockito.Mockito.{mock, verify}

import java.io.IOException
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec


class Task2Test extends AnyFlatSpec with Matchers {
  it should "correctly map values" in {
    val mapper = new Task2Mapper()
    val mockContext = mock(classOf[Mapper[Object, Text, Text, Text]#Context])
    val text = new Text("11:44:27.040 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(")
    mapper.map(new LongWritable(1L), text, mockContext)

    // We verify that the context.write function has been called by the mapper with the right parameters
    verify(mockContext).write(new Text("11:44:25 - 11:44:45.999"), new Text(";kNI&V%v<c#eSDK@lPY("))
  }
}