import customwritables.IntQuartet;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MoviePairMapper extends Mapper<Object, Text, IntQuartet, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private IntQuartet data = new IntQuartet();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] details = line.split("\t");
        
        data.set(Integer.parseInt(details[0]), Integer.parseInt(details[1]), Integer.parseInt(details[2]), Integer.parseInt(details[3]));
        context.write(data, one);
    }
}