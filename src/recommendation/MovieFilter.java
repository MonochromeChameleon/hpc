package recommendation;

import customwritables.Movie;
import customwritables.MovieSimilarity;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Identify movies whose names match the desired search term and emit that pair of movies and their similarity, keyed on
 * the matching movie.
 */
public class MovieFilter extends Mapper<NullWritable, MovieSimilarity, Movie, MovieSimilarity> {

    @Override
    public void map(NullWritable key, MovieSimilarity similarity, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String targetMovie = conf.get("targetMovie");
        
        if (similarity.getMovie1().getName().toString().contains(targetMovie)) {
            context.write(similarity.getMovie1(), similarity);
        }
        // It may be that both films in the pair have similar names and hence match the search term, so we should check
        // and emit both if necessary
        if (similarity.getMovie2().getName().toString().contains(targetMovie)) {
            context.write(similarity.getMovie2(), similarity);
        }
    }
}
