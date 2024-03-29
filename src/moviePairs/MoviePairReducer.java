package moviePairs;


import customwritables.MoviePair;
import customwritables.TagMovie;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.NullWritable;

/**
 * Reducer for the second job, receiving a (Tag, Iterable<MovieOrTag>) input pair, and outputting MoviePair custom 
 * writables for each instance of two films that share a tag.
 */
public class MoviePairReducer extends Reducer<Text, TagMovie, MoviePair, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<TagMovie> values, Context context) throws IOException, InterruptedException {

        MoviePair moviePair = new MoviePair();

        ArrayList<String> checkedMovies = new ArrayList<>();
        
        Iterator<TagMovie> valuesIterator = values.iterator();
        List<TagMovie> valueListClone1 = new ArrayList<>();
        List<TagMovie> valueListClone2 = new ArrayList<>();
        
        // This is ugly, but the nested iterators fail unless we create two distinct (i.e. not object-equal)
        // iterators before we handle our nested loop.
        while (valuesIterator.hasNext()) {
            TagMovie value = valuesIterator.next();
            valueListClone1.add(new TagMovie(key.toString(), value.getMovie().getMovieId().get(), value.getMovie().getNumberOfTags().get(), value.getMovie().getName().toString()));
            valueListClone2.add(new TagMovie(key.toString(), value.getMovie().getMovieId().get(), value.getMovie().getNumberOfTags().get(), value.getMovie().getName().toString()));
        }
        
        // Iterate over the first list of movies.
        Iterator<TagMovie> movie1Iterator = valueListClone1.iterator();
        while (movie1Iterator.hasNext()) {
            TagMovie movie1 = movie1Iterator.next();
            
            // Ensure that we don't handle both sides of a pair (i.e. 1 & 2 vs. 2 & 1)
            // We do this before the inner loop to avoid pairing movies with themselves
            checkedMovies.add(movie1.getMovie().getMovieId().toString());
            
            // Iterate over the second list of movies
            Iterator<TagMovie> movie2Iterator = valueListClone2.iterator();
            while (movie2Iterator.hasNext()) {
                TagMovie movie2 = movie2Iterator.next();
                
                // Skip any movie ids that are already handled.
                if (checkedMovies.contains(movie2.getMovie().getMovieId().toString())) {
                    continue;
                }
                
                // If we have reached this point, we know that we have a previously unprocessed pair of movies that
                // share a tag.
                moviePair.set(movie1.getMovie(), movie2.getMovie());
                context.write(moviePair, NullWritable.get());
            }
        }
    }
}
