
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    public static void main(String[] args) throws Exception {
        boolean success = runJob(StringUtils.join(args, ","), MovieJob.TagCount.getOutputFileName(), MovieJob.TagCount);
        
        if (success) {
            success = runJob(MovieJob.TagCount.getOutputFileName(), MovieJob.MoviePairs.getOutputFileName(), MovieJob.MoviePairs);
        }
        
        if (success) {
            success = runJob(MovieJob.MoviePairs.getOutputFileName(), MovieJob.Similarity.getOutputFileName(), MovieJob.Similarity);
        }
        
        System.exit(success ? 0 : 1);
    }
}
