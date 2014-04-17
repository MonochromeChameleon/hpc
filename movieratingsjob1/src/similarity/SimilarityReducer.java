package similarity;

import customwritables.IntIntPair;
import inputformats.MoviePairRow;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<MoviePairRow, IntWritable, IntIntPair, FloatWritable> {
    
    private IntIntPair movies = new IntIntPair();
    private FloatWritable result = new FloatWritable();
    
    @Override
    public void reduce(MoviePairRow key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Make this a float (even though it's an integer value) so that we don't get integer division later on.
        float numberOfSharedTags = 0; 
        for (IntWritable value : values) {
            numberOfSharedTags += value.get();
        }
        
        int movie1NumberOfTags = key.getMovie1NumberOfTags().get();
        int movie2NumberOfTags = key.getMovie2NumberOfTags().get();
        
        movies.set(key.getMovie1Id(), key.getMovie2Id());
        
        float jaccard = numberOfSharedTags / ((movie1NumberOfTags + movie2NumberOfTags) - numberOfSharedTags);
        if (jaccard >= 0.1) {
            result.set(jaccard);
            context.write(movies, result);
        }
    }
}