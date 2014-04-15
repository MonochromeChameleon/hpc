
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

// mapper issues -- job fails with array index exception -- looks like it can't find the tab
// in the input file (output from job 1). 
// Could be treating the tab as a delimiter (ie running the mapper twice for each line)
public class TagsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text movNoTags = new Text();
    private Text tag = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String dump = value.toString();

        if (StringUtils.ordinalIndexOf(dump, ",", 1) > -1) {

            String[] dumped = dump.split("\t");

            String movie = dumped[0];

            // lose square brackets
            String rest = dumped[1].substring(1, dumped[1].length() - 2);

            // find tag in substring
            String thisTag = rest.substring(0, rest.lastIndexOf(','));

            // find number of tags for movie
            String movieTags = rest.substring(rest.lastIndexOf(',') + 1, rest.length() - 1);

            String movieNumTags = movie + "," + movieTags;


            /*      first attempt: string index out of bounds error

             String dump = value.toString();
             int movieLastIndex = StringUtils.ordinalIndexOf(dump,"\t",1);
             String movie = dump.substring(0,movieLastIndex);

             int tagsFirstIndex = StringUtils.ordinalIndexOf(dump,",",1) +1;
             int tagsLastIndex = StringUtils.ordinalIndexOf(dump,"]",1);
             String movieTags = dump.substring(tagsFirstIndex,tagsLastIndex);

             String movieNumTags = movie + "," + movieTags;

             int tagFirstIndex = StringUtils.ordinalIndexOf(dump,"[",1) + 1;
             int tagLastIndex = StringUtils.ordinalIndexOf(dump,",",1);
             String thisTag = dump.substring(tagFirstIndex,tagLastIndex); */

            tag.set(thisTag);
            movNoTags.set(movieNumTags);

            context.write(tag, movNoTags);
        }
    }
}
