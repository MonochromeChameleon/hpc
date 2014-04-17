package moviePairs;


import customwritables.TagMovie;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

public class MoviePairMapper extends Mapper<NullWritable, TagMovie, Text, TagMovie> {

    @Override
    public void map(NullWritable key, TagMovie value, Context context) throws IOException, InterruptedException {
        context.write(value.getTag(), value);
    }
}
