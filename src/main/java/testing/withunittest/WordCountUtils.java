/*
* LivePerson copyrights will be here...
*/
package testing.withunittest;

import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 2/3/13, 10:58
 */
public class WordCountUtils {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountUtils.class);

  public static String[] splitStr(String strToSplit){
    String[] split = strToSplit.split(" ");
    return split;
  }

  public static int countValues(Iterable values) {
    int sum = 0;
    for (Object obj:values){
      sum++;
    }
    return sum;
  }
}
