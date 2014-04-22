package similarity;

import customwritables.MoviePair;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner to aggregate the total number of instances of each pair of movies.
 */
public class SimilarityCombiner extends Reducer<MoviePair, IntWritable, MoviePair, IntWritable> {
    
    private IntWritable result = new IntWritable();
    
    @Override
    public void reduce(MoviePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Count up the number of corresponding rows at the mapper output.
        int sum = 0; 
        for (IntWritable value : values) {
            sum += value.get();
        }
        
        result.set(sum);
        context.write(key, result);
    }
}