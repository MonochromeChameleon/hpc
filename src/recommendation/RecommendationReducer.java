package recommendation;

import customwritables.Movie;
import customwritables.MovieSimilarity;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hwg30
 */
public class RecommendationReducer extends Reducer<Movie, MovieSimilarity, Text, NullWritable> {
    Text txt = new Text();    
    
    @Override
    public void reduce(Movie key, Iterable<MovieSimilarity> values, Context context) throws IOException, InterruptedException {
        
        txt.set("Recommendations based on " + key.getName());
        context.write(txt, null);
        
        for (MovieSimilarity value : values) {
            if (value.getMovie1().equals(key)) {
                txt.set("\t" + value.getMovie2().getName() + "\t(" + value.getSimilarity() + ")");
                context.write(txt, null);
            } else {
                txt.set("\t" + value.getMovie1().getName() + "\t(" + value.getSimilarity() + ")");
                context.write(txt, null);
            }
        }
    }
}