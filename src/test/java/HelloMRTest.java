/*
* LivePerson copyrights will be here...
*/

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/6/12, 13:52
 */
public class HelloMRTest {
  private static final Logger LOG = LoggerFactory.getLogger(HelloMRTest.class);

  @Test
  public void testMain() throws Exception {
     assertTrue("Is that really true?", true);
  }
}
