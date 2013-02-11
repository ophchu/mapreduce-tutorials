/*
* LivePerson copyrights will be here...
*/
package testing.intigration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import testing.withunittest.WordCountWithTestsJob;

import java.io.*;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

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
    System.setProperty("hadoop.log.dir", "test-logs");

    Configuration conf = new Configuration();

    //Init dfs cluster
    dfsCluster = new MiniDFSCluster(conf, NUM_DATA_NODE, true, null);
    getFS().makeQualified(INPUT);
    getFS().makeQualified(OUTPUT);

    //Init mr cluster
    mrCluster = new MiniMRCluster(1, getFS().getUri().toString(), NUM_TASK_TRACKERS);
  }


  @Test(dataProvider = "miniClusterTest")
  public void miniClusterRun(int reducersNum, String[] inLines, String[] expectedTokens, int[] expectedCount) throws ClassNotFoundException, IOException, InterruptedException {
    //Load input lines into HDFS files
    prepareInputFiles(inLines);

    //Run the job
    WordCountWithTestsJob job = new WordCountWithTestsJob();

    //Init HDFS and JobTracker addresses
    String jobTrakcerName = String.format("%s:%d", mrCluster.getJobTrackerRunner().getJobTracker().getHostname(), mrCluster.getJobTrackerPort());
    String nameNode = String.format("hdfs://%s:%d", mrCluster.getJobTrackerRunner().getJobTracker().getHostname(), dfsCluster.getNameNodePort());
    //Run job
    job.runJob(INPUT, OUTPUT, reducersNum, jobTrakcerName, nameNode);

    //Read results from HDFS output dir
    Map<String, Integer> results = readResults(reducersNum);

    //Check that the number of results words equals to the number of expected
    assertEquals(results.size(), expectedTokens.length, "Number of results different than number of expected");

    for (int i = 0; i < expectedTokens.length; i++) {
      String expectedWord = expectedTokens[i];

      //Make sure the word exists in the result.
      assertNotNull(results.get(expectedWord), String.format("Expected word: [%s] does not exists in the results", expectedWord));


      //Check the number of appearances
      assertEquals(results.get(expectedWord).intValue(), expectedCount[i], String.format("For word: [%s]", expectedWord));
    }
  }

  /**
   * Load data into HDFS files
   *
   * @param inLines lines to be written into HDFS
   * @throws IOException
   */
  private void prepareInputFiles(String[] inLines) throws IOException {
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(getFS().create(INPUT)));
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

  /**
   * Read results from the HDFS output directory
   *
   * @param reducersNum
   * @return map of word and number of occurrences as calculated by the wordcount MR job
   * @throws IOException
   */
  private Map<String, Integer> readResults(int reducersNum) throws IOException {
    Map<String, Integer> results = new HashMap<String, Integer>();
    Set<Path> collectedPaths = collectResultPaths();
    assertEquals(collectedPaths.size(), reducersNum, "Number of results files should be equals to the number of reducers");
    for (Path path : collectedPaths) {
      collectResults(path, results);
    }
    return results;
  }

  /**
   * Read the words and their occurrences as its appear in the results file.
   * As part of the test it make sure no duplications exists.
   *
   * @param path    result file
   * @param results map of the full results
   * @throws IOException
   */
  private void collectResults(Path path, Map<String, Integer> results) throws IOException {
    String line;
    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new InputStreamReader(getFS().open(path)));

      //Read the line
      while ((line = reader.readLine()) != null) {
        //Parse by the default chr (i.e. '\t')
        String[] pair = line.split("\t");

        //Collect results AND make sure no duplications
        assertNull(results.put(pair[0], Integer.parseInt(pair[1])), "Return isn't null, we got duplications in the MR results!");
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Collected all results files. We expected that there will be file per reducer.
   *
   * @return set (i.e. not duplications) of paths to the results files.
   * @throws IOException
   */
  private Set<Path> collectResultPaths() throws IOException {
    FileStatus[] paths = getFS().listStatus(OUTPUT);

    Set<Path> collectedPaths = new HashSet<Path>();
    FileStatus[] dataFiles = getFS().listStatus(paths[0].getPath(), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part-r-");
      }
    });
    for (FileStatus path : dataFiles) {
      collectedPaths.add(path.getPath());
    }
    return collectedPaths;
  }

  private FileSystem getFS() throws IOException {
    return dfsCluster.getFileSystem();
  }

  @DataProvider(name = "miniClusterTest")
  public Object[][] createSimpleTestData() {
    List<Object[]> res = new ArrayList<Object[]>();
    res.add(new Object[]{5, new String[]{"Hello Mapreduce tests"}, new String[]{"Hello", "Mapreduce", "tests"}, new int[]{1, 1, 1}});
    res.add(new Object[]{5, new String[]{"Hello Mapreduce and tests", "Hello and again", "Hello and Nice to have you again"},
            new String[]{"Hello", "Mapreduce", "tests", "again", "and", "Nice", "to", "have", "you"},
            new int[]{3, 1, 1, 2, 3, 1, 1, 1, 1}});
    return res.toArray(new Object[res.size()][]);
  }

  @AfterMethod
  public void tearDown() {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster!=null){
      dfsCluster.shutdown();
    }

  }

}
