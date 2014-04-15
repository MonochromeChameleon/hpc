
import customwritables.IntIntPair;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

// mapper issues -- job fails with array index exception -- looks like it can't find the tab
// in the input file (output from job 1). 
// Could be treating the tab as a delimiter (ie running the mapper twice for each line)
public class TagsMapper extends Mapper<LongWritable, Text, Text, IntIntPair> {

    private IntIntPair movNoTags = new IntIntPair();
    private Text tag = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String dump = value.toString();

        if (StringUtils.ordinalIndexOf(dump, ",", 1) > -1) {

            String[] dumped = dump.split("\t");

            String movie = dumped[0];

            // lose square brackets
            String rest = dumped[1].substring(1, dumped[1].length() - 1);

            // find tag in substring
            String thisTag = rest.substring(0, rest.lastIndexOf(','));

            // find number of tags for movie
            String movieTags = rest.substring(rest.lastIndexOf(',') + 1, rest.length());

            tag.set(thisTag);
            movNoTags.set(movie, movieTags);

            context.write(tag, movNoTags);
        }
    }
}
