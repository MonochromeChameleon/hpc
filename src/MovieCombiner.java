
import com.google.common.collect.Lists;
import customwritables.MovieAndTags;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieCombiner extends Reducer<IntWritable, MovieAndTags, IntWritable, MovieAndTags> {

    private final ArrayWritable allTags = new ArrayWritable(Text.class);
    private final MovieAndTags mandt = new MovieAndTags();
    
    @Override
    public void reduce(IntWritable key, Iterable<MovieAndTags> values, Context context) throws IOException, InterruptedException {

        Set<Writable> tags = new HashSet<>();
        Iterator<MovieAndTags> iter = values.iterator();
        while (iter.hasNext()) {
            MovieAndTags mt = iter.next();
            List<Writable> foo = Lists.newArrayList(mt.getTags().get());
            tags.addAll(foo);
        }
        
        allTags.set(tags.toArray(new Writable[] {}));
        
        mandt.set(key, allTags);
                
        context.write(key, mandt);
    }
}
