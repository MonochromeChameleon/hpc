package tagCount;


import inputformats.TagCountRow;
import inputformats.TagRow;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TagCountReducer extends Reducer<IntWritable, TagRow, TagCountRow, NullWritable> {
    
    TagCountRow output = new TagCountRow();
    IntWritable numberOfTags = new IntWritable();

    @Override
    public void reduce(IntWritable movieId, Iterable<TagRow> tags, Context context) throws IOException, InterruptedException {

        Set<Text> tagSet = new TreeSet<>();
        Iterator<TagRow> iter = tags.iterator();
        while (iter.hasNext()) {
            Text tag = iter.next().getTag();
            tagSet.add(new Text(tag));
        }
        numberOfTags.set(tagSet.size());
        for (Text tag : tagSet) {
            output.set(tag, movieId, numberOfTags);
            context.write(output, NullWritable.get());
        }
    }
}
