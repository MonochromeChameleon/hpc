
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings2 {

    public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        //config
        job.setJarByClass(MovieRatings2.class);
        job.setReducerClass(TagsReducer.class);
        job.setMapperClass(TagsMapper.class);

        //input format
        job.setInputFormatClass(TextInputFormat.class);
        //job.setNumReduceTasks(3);

        //reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
