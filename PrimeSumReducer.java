// exceptions import
import java.io.IOException;

// import box classes
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// import reducer class
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class PrimeSumReducer extends Reducer<Text ,IntWritable ,Text ,IntWritable> 
{
    public void reduce(Text key,Iterable<IntWritable> value,Context content)throws IOException
    {
       int sum=0;
       if(key.equals("PRIME"))
       {
         for(IntWritable i : value )
            sum += i.get();   
       }

       context.write(key ,new IntWritable(sum) );
    }
}