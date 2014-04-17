package similarity;

import inputformats.MoviePairRow;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<NullWritable, MoviePairRow, MoviePairRow, IntWritable> {

    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(NullWritable key, MoviePairRow value, Context context) throws IOException, InterruptedException {
        context.write(value, one);
    }
}