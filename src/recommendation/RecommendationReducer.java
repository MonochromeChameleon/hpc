package recommendation;

import customwritables.Movie;
import customwritables.MovieSimilarity;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This takes a list of MovieSimilarity custom writables for each movie which matches the search term, and writes a 
 * formatted recommendation output.
 */
public class RecommendationReducer extends Reducer<Movie, MovieSimilarity, Text, NullWritable> {
    Text txt = new Text();    
    
    @Override
    public void reduce(Movie key, Iterable<MovieSimilarity> values, Context context) throws IOException, InterruptedException {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append("Recommendations based on ");
        sb.append(key.getName());
        sb.append(":\n\t");
        
        for (MovieSimilarity value : values) {
            if (value.getMovie1().equals(key)) {
                sb.append(value.getMovie2().getName());
            } else {
                sb.append(value.getMovie1().getName());
            }
            
            sb.append("\t(");
            sb.append(value.getSimilarity());
            sb.append(")\n");
        }
        txt.set(sb.toString());
        context.write(txt, null);
    }
}