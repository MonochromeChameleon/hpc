import customwritables.IntIntPair;
import customwritables.IntQuartet;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MoviePairReducer extends Reducer<IntQuartet, IntWritable, IntIntPair, FloatWritable> {
    
    private IntIntPair movies = new IntIntPair();
    private FloatWritable result = new FloatWritable();
    
    @Override
    public void reduce(IntQuartet key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Make this a float (even though it's an integer value) so that we don't get integer division later on.
        float numberOfSharedTags = 0; 
        for (IntWritable value : values) {
            numberOfSharedTags += value.get();
        }
        
        int movie1Id = key.getFirst().get();
        int movie2Id = key.getThird().get();
        
        int movie1NumberOfTags = key.getSecond().get();
        int movie2NumberOfTags = key.getFourth().get();
        
        movies.set(movie1Id, movie2Id);
        
        float jaccard = numberOfSharedTags / (movie1NumberOfTags + movie2NumberOfTags);
        if (jaccard >= 0.1) {
            result.set(jaccard);
            context.write(movies, result);
        }
    }
}