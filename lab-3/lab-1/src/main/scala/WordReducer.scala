import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class WordReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, 
                     values: java.lang.Iterable[IntWritable],
                     context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    
    val valuesIter = values.asScala
    
    val sum = valuesIter.map(_.get()).sum
    
    context.write(key, new IntWritable(sum))
  }
}