/*
* LivePerson copyrights will be here...
*/
package testing.nounitest;

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
public class WordCountNoTestsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountNoTestsReducer.class);

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
    context.setStatus(String.format("Going to process: %s", key.toString()));
    int sum = 0;
    //Count number of occurrences
    for (IntWritable val : values) {
      sum += val.get();
    }
    //Write results
    context.write(key, new IntWritable(sum));
  }
}
