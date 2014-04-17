package moviePairs;


import inputformats.TagCountRow;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

public class MoviePairMapper extends Mapper<NullWritable, TagCountRow, Text, TagCountRow> {

    @Override
    public void map(NullWritable key, TagCountRow value, Context context) throws IOException, InterruptedException {
        context.write(value.getTag(), value);
    }
}
