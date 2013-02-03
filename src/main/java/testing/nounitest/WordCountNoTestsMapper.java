/*
* LivePerson copyrights will be here...
*/
package testing.nounitest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class WordCountNoTestsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountNoTestsMapper.class);
  private final static IntWritable one = new IntWritable(1);
  private Text wordText = new Text(); //Reuse of the Text object - prevent creation of millions objects

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //Text input format:
    //Mappers by blocks
    //Key == position in file
    //Value == line text
    String[] line = value.toString().split(" ");

    for (String word:line){
      wordText.set(word);
      //Write the word + '1'
      context.write(wordText, one);
    }
  }
}
