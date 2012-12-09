/*
* LivePerson copyrights will be here...
*/
package simple.logparse;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simple.wordcount.WordCountMapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class LogParseMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
  private final static IntWritable one = new IntWritable(1);
  private Text level = new Text(); //Reuse of the Text object - prevent creation of millions objects
  private static final int LOG_LEVEL = 2;

  private enum Status {
    TOTAL_LINES, NON_EMPTY_LINE, BIGGER_THAN_LOG_SIZE
  }

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    context.getCounter(Status.TOTAL_LINES).increment(1);
    String line = value.toString();

    if (!line.isEmpty()) {
      context.getCounter(Status.NON_EMPTY_LINE).increment(1);
      String[] split = line.split(" ");
      if (split.length > LOG_LEVEL) {
        level.set(split[LOG_LEVEL]);
        context.write(level, value);
        context.getCounter(Status.BIGGER_THAN_LOG_SIZE).increment(1);
      }
    }
  }
}
