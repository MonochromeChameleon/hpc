import customwritables.IntQuartet;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MoviePairCombiner extends Reducer<IntQuartet, IntWritable, IntQuartet, IntWritable> {
    
    private IntWritable result = new IntWritable();
    
    @Override
    public void reduce(IntQuartet key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Make this a float (even though it's an integer value) so that we don't get integer division later on.
        int sum = 0; 
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        result.set(sum);
        context.write(key, result);
    }
}