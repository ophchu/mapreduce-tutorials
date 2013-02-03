/*
* LivePerson copyrights will be here...
*/
package testing.withunittest;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.swing.text.html.HTML;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 2/3/13, 11:49
 */
public class WordCountUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountUtilsTest.class);

  @Test(dataProvider = "SplitData")
  public void testSplitStr(String strToSplit, String[] expected) throws Exception {
    String[] res = WordCountUtils.splitStr(strToSplit);
    assertEquals(res.length, expected.length, "Expected and results should has the same length");

    for (int i = 0; i < expected.length; i++) {
      assertEquals(res[i], expected[i], "Results isn't has expected!");
    }
  }

  @Test (dataProvider = "CountValues")
  public void testCountValues(Iterable it, int expected) throws Exception {
    int res = WordCountUtils.countValues(it);
    assertEquals(res, expected, "Wrong number of values.");
  }

  @DataProvider(name = "SplitData")
  public Object[][] createSplitData() {
    List<Object[]> res = new ArrayList<Object[]>();
    res.add(new Object[]{"Hello Mapreduce tests", new String[]{"Hello", "Mapreduce", "tests"}});
    res.add(new Object[]{"", new String[]{""}});
    res.add(new Object[]{"Hello Mapreduce tests, and how are you", new String[]{"Hello", "Mapreduce", "tests,", "and", "how", "are", "you"}});
    return res.toArray(new Object[res.size()][]);
  }

  @DataProvider(name = "CountValues")
  public Object[][] createCountValuesData() throws InstantiationException, IllegalAccessException {
    List<Object[]> res = new ArrayList<Object[]>();
    res.add(new Object[]{createIterable(String.class, 5), 5});
    res.add(new Object[]{createIterable(StringBuilder.class, 20), 20});
    res.add(new Object[]{createIterable(ArrayList.class, 100), 100});
    return res.toArray(new Object[res.size()][]);
  }

  private <T> Iterable<T> createIterable(Class<T> itOnObj, final int numOfValues) throws IllegalAccessException, InstantiationException {
    List list = new ArrayList<T>();

    for (int i = 0; i < numOfValues; i++) {
      list.add(itOnObj.newInstance());
    }
    return list;
  }
}
