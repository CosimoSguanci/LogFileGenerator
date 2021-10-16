package Tasks.Utils

import org.apache.hadoop.io.{IntWritable, WritableComparator}

object DecreasingIntComparator {
  WritableComparator.define(classOf[DecreasingIntComparator], new IntWritable.Comparator())
}

class DecreasingIntComparator extends IntWritable.Comparator {
  override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = -super.compare(b1, s1, l1, b2, s2, l2)
}
