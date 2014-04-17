package moviePairs;


import inputformats.MoviePairRow;
import inputformats.TagCountRow;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.NullWritable;

public class MoviePairReducer extends Reducer<Text, TagCountRow, MoviePairRow, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<TagCountRow> values, Context context) throws IOException, InterruptedException {

        MoviePairRow movieAndTagCountPair = new MoviePairRow();

        ArrayList<String> checkedMovies = new ArrayList<>();
        
        Iterator<TagCountRow> valuesIterator = values.iterator();
        List<TagCountRow> valueListClone1 = new ArrayList<>();
        List<TagCountRow> valueListClone2 = new ArrayList<>();
        
        while (valuesIterator.hasNext()) {
            // This is ugly, but the nested iterators fail unless we create two distinct (i.e. not object-equal)
            // iterators before we handle our nested loop.
            TagCountRow value = valuesIterator.next();
            valueListClone1.add(new TagCountRow(key.toString(), value.getMovieId().get(), value.getNumberOfTags().get()));
            valueListClone2.add(new TagCountRow(key.toString(), value.getMovieId().get(), value.getNumberOfTags().get()));
        }
        
        Iterator<TagCountRow> movie1Iterator = valueListClone1.iterator();
        while (movie1Iterator.hasNext()) {
            TagCountRow movie1 = movie1Iterator.next();
            
            // Ensure that we don't handle both sides of a pair (i.e. 1 & 2 vs. 2 & 1)
            // We do this before the inner loop to avoid pairing movies with themselves
            checkedMovies.add(movie1.getMovieId().toString());
            
            Iterator<TagCountRow> movie2Iterator = valueListClone2.iterator();
            while (movie2Iterator.hasNext()) {
                TagCountRow movie2 = movie2Iterator.next();
                
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
