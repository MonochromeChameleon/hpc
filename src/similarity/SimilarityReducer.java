package similarity;

import customwritables.MoviePair;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for the third job, receiving a (MoviePair, Iterable<IntWritable>) input pair, and outputting a MoviePair 
 * custom writable and a FloatWritable (i.e. similarity coefficient) for each pair of related films.
 */
public class SimilarityReducer extends Reducer<MoviePair, IntWritable, MoviePair, FloatWritable> {
    
    private FloatWritable similarity = new FloatWritable();
    
    @Override
    public void reduce(MoviePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Make this a float (even though it's an integer value) so that we don't get integer division later on.
        float numberOfSharedTags = 0; 
        for (IntWritable value : values) {
            // Aggregate the total number of shared tags (i.e. the number of input rows for this pair of movies)
            numberOfSharedTags += value.get();
        }
        
        int movie1NumberOfTags = key.getMovie1().getNumberOfTags().get();
        int movie2NumberOfTags = key.getMovie2().getNumberOfTags().get();
        
        // Calculate the jaccard similarity coefficient
        float jaccard = numberOfSharedTags / ((movie1NumberOfTags + movie2NumberOfTags) - numberOfSharedTags);

        // Filter on a threshold similarity level to remove films that only share a small proportion of their total tags
        if (jaccard >= 0.1) {
            similarity.set(jaccard);
            // The output of the movie pair and similarity is equivalent to a MovieSimilarity custom writable.
            context.write(key, similarity);
        }
    }
}