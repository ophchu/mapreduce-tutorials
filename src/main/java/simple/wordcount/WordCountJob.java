/*
* LivePerson copyrights will be here...
*/
package simple.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simple.logparse.LogParseReducer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ophirc
 * @version 1.0.0
 * @since 12/9/12, 13:43
 */
public class WordCountJob {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountJob.class);

  public final static String JOB_DATE_FORMAT_STRING = "yyyyMMdd-HHmmss";

  public static void main(String[] args) throws Exception {
    //Initializing job
    //Configuration created with the default params (in here - local) and override by environment.
    //Will be overrided by dist env.
    Configuration conf = new Configuration();

    //Set the job's name
    Job job = new Job(conf, "wordcount");

    //Init input format to be text input format
    job.setInputFormatClass(TextInputFormat.class);
    //Can be directory or file, local or hdfs ("hdfs://my-cluster:54310/user/ophchu")
    FileInputFormat.addInputPath(job, new Path(args[0]));


    //Init mapper
    //Number of mapper set by the input format
    job.setMapperClass(WordCountMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    //Init partitioners
    job.setPartitionerClass(HashPartitioner.class);

    //Init reducer
    job.setReducerClass(WordCountReducer.class);

    //User to set the number of reducers!
    job.setNumReduceTasks(5);

    //Init output format
    job.setOutputFormatClass(TextOutputFormat.class);

    //Create unique directory
    DateFormat df = new SimpleDateFormat(JOB_DATE_FORMAT_STRING);
    FileOutputFormat.setOutputPath(job, new Path(args[1], df.format(new Date())));

    //Run the job
    job.waitForCompletion(true);
  }
}
