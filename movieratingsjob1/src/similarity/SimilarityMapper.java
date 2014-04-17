package similarity;

import customwritables.MoviePair;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<NullWritable, MoviePair, MoviePair, IntWritable> {

    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(NullWritable key, MoviePair value, Context context) throws IOException, InterruptedException {
        context.write(value, one);
    }
}