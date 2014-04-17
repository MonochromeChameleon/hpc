
import customwritables.MovieOrTag;
import inputformats.MovieFileInputFormat;
import inputformats.TagFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import tagCount.TagCountReducer;

public class BuildDataset {

    public static boolean runJob(String input, String output, MovieJob jobType) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        
        job.setJarByClass(BuildDataset.class);
        
        jobType.configureJob(job);
        
        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        return job.waitForCompletion(true);
    }
    
    public static boolean runDataImportJob(String tagsFile, String moviesFile) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        
        job.setJarByClass(BuildDataset.class);
        
        Path tagsPath = new Path(tagsFile);
        Path moviesPath = new Path(moviesFile);
        
        // We need to join the tags onto the movie names. Ideally we would do this later so as to avoid passing around
        // so much data, but this is the only point at which we can reliably construct a sensible join.
        MultipleInputs.addInputPath(job, tagsPath, TagFileInputFormat.class, MovieJob.TagCount.getMapperClass());
        MultipleInputs.addInputPath(job, moviesPath, MovieFileInputFormat.class, MovieJob.TagCount.getMapperClass());
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MovieOrTag.class);
        job.setReducerClass(TagCountReducer.class);
        
        Path outputPath = new Path(MovieJob.TagCount.getOutputFileName());

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        boolean success = runDataImportJob(args[0], args[1]);
        
        if (success) {
            success = runJob(MovieJob.TagCount.getOutputFileName(), MovieJob.MoviePairs.getOutputFileName(), MovieJob.MoviePairs);
        }
        
        if (success) {
            success = runJob(MovieJob.MoviePairs.getOutputFileName(), MovieJob.Similarity.getOutputFileName(), MovieJob.Similarity);
        }
        
        System.exit(success ? 0 : 1);
    }
}
