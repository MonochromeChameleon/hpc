package tagCount;


import customwritables.MovieOrTag;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TagCountMapper extends Mapper<NullWritable, MovieOrTag, IntWritable, MovieOrTag> {

    @Override
    public void map(NullWritable key, MovieOrTag tag, Context context) throws IOException, InterruptedException {
        context.write(tag.getMovieId(), tag);
    }
}
