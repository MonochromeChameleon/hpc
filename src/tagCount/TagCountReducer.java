package tagCount;


import customwritables.Movie;
import customwritables.TagMovie;
import customwritables.MovieOrTag;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TagCountReducer extends Reducer<IntWritable, MovieOrTag, TagMovie, NullWritable> {
    
    TagMovie output = new TagMovie();
    IntWritable numberOfTags = new IntWritable();
    Movie movie = new Movie();
    Text movieName = new Text();

    @Override
    public void reduce(IntWritable movieId, Iterable<MovieOrTag> tags, Context context) throws IOException, InterruptedException {

        Set<Text> tagSet = new TreeSet<>();
        Iterator<MovieOrTag> iter = tags.iterator();
        while (iter.hasNext()) {
            MovieOrTag mot = iter.next();
            Text tag = mot.getTag();
            if (StringUtils.isEmpty(tag.toString())) {
                movieName = new Text(mot.getName());
            } else {
                tagSet.add(new Text(tag));
            }
        }
        numberOfTags.set(tagSet.size());
        movie.set(movieId, numberOfTags, movieName);

        for (Text tag : tagSet) {
            output.set(tag, movie);
            context.write(output, NullWritable.get());
        }
    }
}
