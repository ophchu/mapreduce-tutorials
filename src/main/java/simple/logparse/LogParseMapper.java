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

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class LogParseMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
  private final static IntWritable one = new IntWritable(1);
  private Text level = new Text(); //Reuse of the Text object - prevent creation of millions objects
  private int logLevelPos;
  private static final int LOG_LEVEL_POS_DEFAULT = 4;


  //Counters names
  private enum Status {
    TOTAL_LINES, NON_EMPTY_LINE, EMPTY_LINE, LEGAL_LINE, NON_LEGAL_LINE
  }

  //This is the right way to init mapper.
  //Constructor isn't good as it might reuse the object.
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    logLevelPos = context.getConfiguration().getInt(LogParseJob.LOG_LEVEL_POS, LOG_LEVEL_POS_DEFAULT);
  }

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //Count total lines
    context.getCounter(Status.TOTAL_LINES).increment(1);
    String line = value.toString();

    if (!line.isEmpty()) {
      //Count non-empty lines
      context.getCounter(Status.NON_EMPTY_LINE).increment(1);
      String[] split = line.split(" ");
      if (split.length > logLevelPos) {
        level.set(split[logLevelPos]);
        //Write the log level as key and the line as value
        context.write(level, value);
        //Count number of 'legal' lines
        context.getCounter(Status.LEGAL_LINE).increment(1);
      } else {
        //Count number of 'ilegal' lines
        context.getCounter(Status.NON_LEGAL_LINE).increment(1);
      }
    } else {
      //Count empty lines
      context.getCounter(Status.EMPTY_LINE).increment(1);
    }
  }
}
