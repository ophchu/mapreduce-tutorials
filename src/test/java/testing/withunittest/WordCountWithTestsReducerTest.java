/*
* LivePerson copyrights will be here...
*/
package testing.withunittest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
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
public class WordCountWithTestsReducerTest {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWithTestsReducerTest.class);



  @BeforeMethod
  public void setUp() {
  }

  @Test(dataProvider = "simpleTest")
  public void simpleTest() throws IOException {
  }

  @Test(dataProvider = "simpleTest")
  public void countersTest() throws IOException {
  }

  @DataProvider(name = "simpleTest")
  public Object[][] createSimpleTestData() {
    List<Object[]> res = new ArrayList<Object[]>();
    return res.toArray(new Object[res.size()][]);
  }
}
