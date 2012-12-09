/*
* LivePerson copyrights will be here...
*/
package simple.logparse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simple.wordcount.WordCountJob;
import simple.wordcount.WordCountMapper;
import simple.wordcount.WordCountReducer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class LogParseJob {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountJob.class);

  public final static String JOB_DATE_FORMAT_STRING = "yyyyMMdd-HHmmss";

  public static void main(String[] args) throws Exception {
    //Initializing job
    Configuration conf = new Configuration();

    //Set the job's name
    Job job = new Job(conf, "logparse");


    //Init input format
    FileInputFormat.addInputPath(job, new Path(args[0])); //Can be directory or file, local or hdfs ("hdfs://my-cluster:54310/user/ophchu")
    job.setInputFormatClass(TextInputFormat.class);

    //Init mapper
    job.setMapperClass(LogParseMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    //Init reducer
    job.setReducerClass(LogParseReducer.class);
//    job.setReducerClass(WordCountReducer.class);

    //Init output format
    job.setOutputFormatClass(TextOutputFormat.class);
    DateFormat df = new SimpleDateFormat(JOB_DATE_FORMAT_STRING);
    FileOutputFormat.setOutputPath(job, new Path(args[1], df.format(new Date()))); //Create unique directory

    job.waitForCompletion(true);
  }
}
