package tagCount;


import customwritables.Movie;
import customwritables.TagMovie;
import customwritables.MovieOrTag;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for the first job, receiving a (movie Id, Iterable<MovieOrTag>) input pair, and outputting TagMovie custom
 * writables as the joined output from the two input tasks.
 */
public class TagCountReducer extends Reducer<IntWritable, MovieOrTag, TagMovie, NullWritable> {
    
    TagMovie output = new TagMovie();
    IntWritable numberOfTags = new IntWritable();
    Movie movie = new Movie();
    Text movieName = new Text();

    @Override
    public void reduce(IntWritable movieId, Iterable<MovieOrTag> tags, Context context) throws IOException, InterruptedException {

        // The input list will be all of the Tag entries as well as the movie name entry.
        Set<Text> tagSet = new TreeSet<>();
        Iterator<MovieOrTag> iter = tags.iterator();
        while (iter.hasNext()) {
            MovieOrTag mot = iter.next();
            Text tag = mot.getTag();
            
            if (StringUtils.isEmpty(tag.toString())) {
                // If the value to which we have iterated does not have a tag name, then it must be the movie details 
                // instead.
                movieName = new Text(mot.getName());
            } else {
                // Otherwise, we can add the tag value to the list of tags.
                tagSet.add(new Text(tag));
            }
        }

        numberOfTags.set(tagSet.size());
        movie.set(movieId, numberOfTags, movieName);

        for (Text tag : tagSet) {
            // For each tag, we ouput that tag and its associated movie.
            output.set(tag, movie);
            context.write(output, NullWritable.get());
        }
    }
}
