import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

// file system 
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// box classes import 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// mapreduce imports
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PrimeSumDriver 
{
   public static void main(String[] args)throws IOException
   {
      JobConf j = new JobConf(PrimeSumDriver.class);
      
      j.setJobName("PrimeSum");
      j.setMapperClass(PrimeSumMapper.class);
      j.setReducerClass(PrimeSumReducer.class);

      j.setOutputKeyClass(Text.class);
      j.setOutputValueClass(IntWritable.class);

      j.setInputFormat(TextInputFormat.class);
      j.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPath(j,new Path(args[0]));
      FileOutputFormat.setOutputPath(j,new Path(args[1]));

      JobClient.runJob(j);
   } 
}
