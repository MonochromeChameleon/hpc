
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings {

    public static void runJob(String input, String output, MovieJob jobType) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        
        job.setJarByClass(MovieRatings.class);
        
        jobType.configureJob(job);
        
        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        String[] inputPaths = Arrays.copyOfRange(args, 0, args.length - 1);
        String outputPath = args[args.length - 1];
        
        String joinedInput = StringUtils.join(inputPaths, ",");
        runJob(joinedInput, MovieJob.TagCount.getOutputFileName(), MovieJob.TagCount);
        runJob(MovieJob.TagCount.getOutputFileName(), MovieJob.MoviePairs.getOutputFileName(), MovieJob.MoviePairs);
        runJob(MovieJob.MoviePairs.getOutputFileName(), outputPath, MovieJob.Similarity);
    }
}
