/*
* LivePerson copyrights will be here...
*/
package simple.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountReducer.class);

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    context.write(key, new IntWritable(sum));
  }
}
