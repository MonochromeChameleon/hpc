package tagCount;


import customwritables.TagRow;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TagCountMapper extends Mapper<NullWritable, TagRow, IntWritable, TagRow> {

    @Override
    public void map(NullWritable key, TagRow tag, Context context) throws IOException, InterruptedException {
        context.write(tag.getMovieId(), tag);
    }
}
