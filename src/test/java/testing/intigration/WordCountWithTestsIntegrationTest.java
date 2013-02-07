/*
* LivePerson copyrights will be here...
*/
package testing.intigration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import simple.wordcount.WordCountMapper;
import testing.nounitest.WordCountNoTestsJob;
import testing.withunittest.WordCountWithTestsJob;
import testing.withunittest.WordCountWithTestsMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author TLV\ophirc
 * @version 0.0.1
 * @since 2/6/13, 17:51
 */
public class WordCountWithTestsIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWithTestsIntegrationTest.class);
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluster;

  private final Path INPUT = new Path("input");
  private final Path OUTPUT = new Path("output");
  private static final int NUM_DATA_NODE = 4;
  private static final int NUM_TASK_TRACKERS = 4;


  @BeforeMethod
  public void setUp() throws Exception {
    // make sure the log folder exists,
    // otherwise the test fill fail
    new File("test-logs").mkdirs();
    //
    System.setProperty("hadoop.log.dir", "test-logs");
//    System.setProperty("javax.xml.parsers.SAXParserFactory",
//            "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

    Configuration conf = new Configuration();

    //Init dfs cluster
    dfsCluster = new MiniDFSCluster(conf, NUM_DATA_NODE, true, null);
    dfsCluster.getFileSystem().makeQualified(INPUT);
    dfsCluster.getFileSystem().makeQualified(OUTPUT);
    dfsCluster.getFileSystem().copyFromLocalFile(new Path("src/test/resoucers/wordcount.txt"), INPUT);

    //Init mr cluster
    mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri().toString(), NUM_TASK_TRACKERS);
  }


  @Test(dataProvider = "miniClusterTest")
  public void miniClusterRun(String[] inLines, String[] results, int[] resNum) throws ClassNotFoundException, IOException, InterruptedException {
    prepareFiles(inLines);
    //Init job
    WordCountWithTestsJob job = new WordCountWithTestsJob();

    String jobTrakcerName = mrCluster.getJobTrackerRunner().getJobTracker().getHostname() + ":" + mrCluster.getJobTrackerPort();
    String nameNode = mrCluster.getJobTrackerRunner().getJobTracker().getHostname() + ":" + dfsCluster.getNameNodePort();
    job.runJob(INPUT, OUTPUT, jobTrakcerName, nameNode);

    checkResults(results);
  }

  private void prepareFiles(String[] inLines) throws IOException {
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(dfsCluster.getFileSystem().create(INPUT)));
      for (String line : inLines) {
        writer.write(line);
        writer.newLine();
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  private void checkResults(String[] results) throws IOException {
    FileStatus[] paths = dfsCluster.getFileSystem().listStatus(OUTPUT);

    Set<Path> collectedPaths = new HashSet<Path>();
    for (FileStatus path : dfsCluster.getFileSystem().listStatus(paths[0].getPath())) {
      if (!path.getPath().getName().startsWith("_")) {
        collectedPaths.add(path.getPath());
      }
    }

    System.out.println(collectedPaths.toString());
    for (Path path : collectedPaths) {
      dumpResults(path);
    }
  }

  private void dumpResults(Path path) throws IOException {
    BufferedReader reader = null;
    String line;
    try {
      reader = new BufferedReader(new InputStreamReader(dfsCluster.getFileSystem().open(path)));
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @DataProvider(name = "miniClusterTest")
  public Object[][] createSimpleTestData() {
    List<Object[]> res = new ArrayList<Object[]>();
    res.add(new Object[]{new String[]{"Hello Mapreduce tests"}, new String[]{"Hello", "Mapreduce", "tests"}, new int[]{1, 1, 1}});
    return res.toArray(new Object[res.size()][]);
  }

  @AfterMethod
  public void tearDown() {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
  }

}
