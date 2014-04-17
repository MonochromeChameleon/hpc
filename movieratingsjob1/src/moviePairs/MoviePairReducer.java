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

public class MoviePairReducer extends Reducer<Text, TagMovie, MoviePair, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<TagMovie> values, Context context) throws IOException, InterruptedException {

        MoviePair movieAndTagCountPair = new MoviePair();

        ArrayList<String> checkedMovies = new ArrayList<>();
        
        Iterator<TagMovie> valuesIterator = values.iterator();
        List<TagMovie> valueListClone1 = new ArrayList<>();
        List<TagMovie> valueListClone2 = new ArrayList<>();
        
        while (valuesIterator.hasNext()) {
            // This is ugly, but the nested iterators fail unless we create two distinct (i.e. not object-equal)
            // iterators before we handle our nested loop.
            TagMovie value = valuesIterator.next();
            valueListClone1.add(new TagMovie(key.toString(), value.getMovieId().get(), value.getNumberOfTags().get()));
            valueListClone2.add(new TagMovie(key.toString(), value.getMovieId().get(), value.getNumberOfTags().get()));
        }
        
        Iterator<TagMovie> movie1Iterator = valueListClone1.iterator();
        while (movie1Iterator.hasNext()) {
            TagMovie movie1 = movie1Iterator.next();
            
            // Ensure that we don't handle both sides of a pair (i.e. 1 & 2 vs. 2 & 1)
            // We do this before the inner loop to avoid pairing movies with themselves
            checkedMovies.add(movie1.getMovieId().toString());
            
            Iterator<TagMovie> movie2Iterator = valueListClone2.iterator();
            while (movie2Iterator.hasNext()) {
                TagMovie movie2 = movie2Iterator.next();
                
                // Skip any movie ids that are already handled.
                if (checkedMovies.contains(movie2.getMovieId().toString())) {
                    continue;
                }
                
                movieAndTagCountPair.set(movie1.getMovieId(), movie1.getNumberOfTags(), movie2.getMovieId(), movie2.getNumberOfTags());
                context.write(movieAndTagCountPair, NullWritable.get());
            }
        }
    }
}
