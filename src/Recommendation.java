
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main class for collecting recommendations based on a provided movie search string.
 */
public class Recommendation {

    public static void main(String[] args) throws Exception {
        String output = args[0];
        String targetMovie = args[1].toLowerCase();
        
        Configuration conf = new Configuration();
        conf.set("targetMovie", targetMovie);
        
        Job job = new Job(conf);
        
        job.setJarByClass(BuildDataset.class);
        
        MovieJob.Recommendation.configureJob(job);
        
        Path similarityPath = new Path(MovieJob.Similarity.getOutputFileName());
        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, similarityPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }
}
