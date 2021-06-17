// exception handling 
import java.io.IOException;

// box classes import
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// import mapper class
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PrimeSumMapper extends Mapper<LongWritable ,Text ,Text ,IntWritable> 
{
   
   private boolean CheckPrime(int numberToCheck) 
   {
      int remainder;
      for (int i = 2; i <= numberToCheck / 2; i++) 
      {
         remainder = numberToCheck % i;
         if (remainder == 0) 
            return false;
         
      }
      return true;
	}

    public void map(LongWritable key ,Text value ,Context context) throws IOException
    {
       String str[] = value.toString().split(",");
       
       for(String val : str)
       {
         int num = Integer.parseInt(val);
         
         if(CheckPrime(num));
            context.write(new Text("PRIME") ,new IntWritable(num));
         
         else
           context.write(new Text("Composite") ,new IntWritable(num));
         
       }
    }
}
