package tagCount;


import customwritables.*;
import inputformats.TagRow;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TagCountReducer extends Reducer<IntWritable, TagRow, IntWritable, TextIntPair> {

    @Override
    public void reduce(IntWritable movieId, Iterable<TagRow> tags, Context context) throws IOException, InterruptedException {

        Set<Text> tagSet = new TreeSet<>();
        Iterator<TagRow> iter = tags.iterator();
        while (iter.hasNext()) {
            Text tag = iter.next().getTag();
            tagSet.add(new Text(tag));
        }
        for (Text tag : tagSet) {
            context.write(movieId, new TextIntPair(tag.toString(), tagSet.size()));
        }
    }
}
