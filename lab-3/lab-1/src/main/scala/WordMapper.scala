import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  private final val one = new IntWritable(1)
  private val word = new Text()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    value.toString.toLowerCase
      .replaceAll("[^a-z\\s]", "")
      .split("\\s+")
      .filter(_.nonEmpty)
      .foreach { token =>
        word.set(token)
        context.write(word, one)
      }
  }
}
