/*
* LivePerson copyrights will be here...
*/
package testing.withunittest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 2/3/13, 16:03
 */
public class WordCountWithTestsJobTest {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWithTestsJobTest.class);

  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapRedDriver;

  @BeforeMethod
  public void setUp() {
   mapRedDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>(new WordCountWithTestsMapper(), new WordCountWithTestsReducer());
  }

  @Test(dataProvider = "simpleTest")
  public void simpleTest(String inString, String[] splits, int lineNum, int wordCount) throws IOException {
  }

  @Test(dataProvider = "simpleTest")
  public void countersTest(String inString, String[] splits, int lineNum, int wordCount) throws IOException {
  }

  @DataProvider(name = "simpleTest")
  public Object[][] createSimpleTestData() {
    List<Object[]> res = new ArrayList<Object[]>();
    res.add(new Object[]{"Hello Mapreduce tests", new String[]{"Hello", "Mapreduce", "tests"}, 1, 3});
    res.add(new Object[]{"Hello Mapreduce tests! Nice to meet you!", new String[]{"Hello", "Mapreduce", "tests!", "Nice", "to", "meet", "tou!"}, 1, 7});
    return res.toArray(new Object[res.size()][]);
  }
}
