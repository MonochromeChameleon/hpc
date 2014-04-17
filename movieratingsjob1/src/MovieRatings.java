
import tagCount.TagCountReducer;
import tagCount.TagCountMapper;
import customwritables.*;
import inputformats.TagFileInputFormat;
import inputformats.TagRow;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings {

    public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        //config
        job.setJarByClass(MovieRatings.class);
        job.setReducerClass(TagCountReducer.class);
        job.setMapperClass(TagCountMapper.class);

        //input format
        job.setInputFormatClass(TagFileInputFormat.class);
        //job.setNumReduceTasks(3);

        //reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextIntPair.class);
		
        //mapper output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TagRow.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
    
    public static void runJob(String[] input, String output, MovieJob jobType) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        
        job.setJarByClass(MovieRatings.class);
        
        jobType.configureJob(job);
        
        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1], MovieJob.TagCount);
    }
}
