package recommendation;

import customwritables.Movie;
import customwritables.MovieSimilarity;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Identify movies whose names match the desired search term and emit the appropriate IDs. Or something...
 */
public class MovieFilter extends Mapper<NullWritable, MovieSimilarity, Movie, MovieSimilarity> {

    @Override
    public void map(NullWritable key, MovieSimilarity moviePair, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String targetMovie = conf.get("targetMovie");
        
        if (moviePair.getMovie1().getName().toString().contains(targetMovie)) {
            context.write(moviePair.getMovie1(), moviePair);
        }
        if (moviePair.getMovie2().getName().toString().contains(targetMovie)) {
            context.write(moviePair.getMovie2(), moviePair);
        }
    }
}
