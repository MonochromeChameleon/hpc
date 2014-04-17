package similarity;

import customwritables.MoviePair;
import customwritables.MovieSimilarity;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<MoviePair, IntWritable, MovieSimilarity, NullWritable> {
    
    private FloatWritable similarity = new FloatWritable();
    private MovieSimilarity result = new MovieSimilarity();
    
    @Override
    public void reduce(MoviePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Make this a float (even though it's an integer value) so that we don't get integer division later on.
        float numberOfSharedTags = 0; 
        for (IntWritable value : values) {
            numberOfSharedTags += value.get();
        }
        
        int movie1NumberOfTags = key.getMovie1().getNumberOfTags().get();
        int movie2NumberOfTags = key.getMovie2().getNumberOfTags().get();
        
        float jaccard = numberOfSharedTags / ((movie1NumberOfTags + movie2NumberOfTags) - numberOfSharedTags);

        if (jaccard >= 0.1) {
            similarity.set(jaccard);
            result.set(key.getMovie1(), key.getMovie2(), similarity);
            context.write(result, NullWritable.get());
        }
    }
}