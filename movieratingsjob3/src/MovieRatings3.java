import customwritables.IntIntPair;
import customwritables.IntQuartet;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatings3 {
    
    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MovieRatings3.class);
                
        job.setMapperClass(MoviePairMapper.class);
        // I'm sure Felix said that the Combiner had to have the same signature as the reducer, but this doesn't throw an error...
        job.setCombinerClass(MoviePairCombiner.class);
        job.setReducerClass(MoviePairReducer.class);
        
        //reducer output
        job.setOutputKeyClass(IntIntPair.class);
        job.setOutputValueClass(FloatWritable.class);
        
        job.setMapOutputKeyClass(IntQuartet.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(3);
        
        Path outputPath = new Path(output);
        
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
    
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
    }
}