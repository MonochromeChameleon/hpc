
import customwritables.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<IntWritable, Text, IntWritable, TextIntPair> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<Text> tags = new TreeSet<Text>();
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            Text tag = iter.next();
            tags.add(new Text(tag));
        }
        for (Text tag : tags) {
            context.write(key, new TextIntPair(tag.toString(), tags.size()));
        }
    }
}
