/*
* LivePerson copyrights will be here...
*/
package testing.itegration;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

/**
 * @author TLV\ophirc
 * @version 0.0.1
 * @since 2/6/13, 17:51
 */
public class WordCountWithTestsIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWithTestsIntegrationTest.class);
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluser;

  @BeforeMethod
    public void setUp() {
     dfsCluster = new MiniDFSCluster();
    }
}
