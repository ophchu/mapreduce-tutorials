/*
* LivePerson copyrights will be here...
*/
package testing.intigration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;
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
import static org.testng.Assert.assertEquals;

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
    System.setProperty("javax.xml.parsers.SAXParserFactory",
            "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

    Configuration conf = new Configuration();

    //Init dfs cluster
    dfsCluster = new MiniDFSCluster(conf, NUM_DATA_NODE, true, null);
    dfsCluster.getFileSystem().makeQualified(INPUT);
    dfsCluster.getFileSystem().makeQualified(OUTPUT);
    dfsCluster.getFileSystem().copyFromLocalFile(new Path("src/test/resoucers/wordcount.txt"), INPUT);

    //Init mr cluster
    mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri().toString(), NUM_TASK_TRACKERS);
  }


  @Test(dataProvider = "miniClusterTest", enabled = false)
  public void miniClusterRun(String[] inLines, String[] expectedTokens, int[] expectedCount) throws ClassNotFoundException, IOException, InterruptedException {
    prepareFiles(inLines);
    //Init job
    WordCountWithTestsJob job = new WordCountWithTestsJob();

    String jobTrakcerName = mrCluster.getJobTrackerRunner().getJobTracker().getHostname() + ":" + mrCluster.getJobTrackerPort();
    String nameNode = mrCluster.getJobTrackerRunner().getJobTracker().getHostname() + ":" + dfsCluster.getNameNodePort();
    job.runJob(INPUT, OUTPUT, jobTrakcerName, nameNode);

    List<Pair<String, Integer>> results = readResults();

    assertEquals(results.size(), expectedTokens, "Number of results different than number of expected");

    for (int i=0; i<expectedTokens.length; i++){
       //Compare token
      assertEquals(results.get(i).getFirst(), expectedTokens[i], "Token number " + i + " Does not much!");
      assertEquals(results.get(i).getSecond().intValue(), expectedCount[i], "Count number " + i + " Does not much!");
    }
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

  private List<Pair<String, Integer>> readResults() throws IOException {
    List<Pair<String, Integer>> results = new ArrayList<Pair<String, Integer>>();
    Set<Path> collectedPaths = collectResultPaths();
    for (Path path : collectedPaths) {
      collectResults(path, results);
    }
    return results;
  }


  private Set<Path> collectResultPaths() throws IOException {
    FileStatus[] paths = dfsCluster.getFileSystem().listStatus(OUTPUT);

    Set<Path> collectedPaths = new HashSet<Path>();
    for (FileStatus path : dfsCluster.getFileSystem().listStatus(paths[0].getPath())) {
      if (!path.getPath().getName().startsWith("_")) {
        collectedPaths.add(path.getPath());
      }
    }
    return collectedPaths;
  }

  private void collectResults(Path path, List<Pair<String, Integer>> results) throws IOException {
    Text token = new Text();
    IntWritable count = new IntWritable();
    DataInputStream dataInput = null;
    try {
      dataInput = new DataInputStream(dfsCluster.getFileSystem().open(path));

      token.readFields(dataInput);
      count.readFields(dataInput);

      results.add(new Pair<String, Integer>(token.toString(), count.get()));
    } catch (EOFException eof) {

    } finally {
      if (dataInput != null) {
        dataInput.close();
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
