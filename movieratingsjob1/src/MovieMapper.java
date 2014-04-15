
import customwritables.*;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable movID = new IntWritable();
    private Text tags = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String dump = value.toString();
        if (StringUtils.ordinalIndexOf(dump, "::", 3) > -1) {
            int movieStartIndex = StringUtils.ordinalIndexOf(dump, "::", 1) + 2;
            int movieLastIndex = StringUtils.ordinalIndexOf(dump, "::", 2);
            int tagStartIndex = StringUtils.ordinalIndexOf(dump, "::", 2) + 1;
            int tagLastIndex = StringUtils.ordinalIndexOf(dump, "::", 3);
            String strMovieID = dump.substring(movieStartIndex, movieLastIndex);
            int movieID = Integer.parseInt(strMovieID);
            String tag = dump.substring(tagStartIndex, tagLastIndex).toLowerCase();
            tag.replaceAll("[\\p{ASCII}]|\\d", "");
            if (tag.startsWith(":")) {
                String modTag = tag.substring(1);
                tags.set(modTag);
            } else {
                tags.set(tag);
            }
            movID.set(movieID);
        }
        context.write(movID, tags);
    }
}
