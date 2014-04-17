
import inputformats.MoviePairFormat;
import customwritables.MoviePair;
import inputformats.TagCountFormat;
import customwritables.TagMovie;
import inputformats.TagFileInputFormat;
import customwritables.TagRow;
import moviePairs.MoviePairMapper;
import moviePairs.MoviePairReducer;
import tagCount.TagCountReducer;
import tagCount.TagCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import similarity.SimilarityCombiner;
import similarity.SimilarityMapper;
import similarity.SimilarityReducer;

public enum MovieJob {

    // Define available jobs as enum values, each with associated Mapper/Combiner/Reducer 
    // classes, and output key/value classes
    TagCount(TagCountMapper.class,
            null,
            TagCountReducer.class,
            TagFileInputFormat.class,
            IntWritable.class,
            TagRow.class,
            "tags.out"),

    MoviePairs(MoviePairMapper.class,
            null,
            MoviePairReducer.class,
            TagCountFormat.class,
            Text.class,
            TagMovie.class,
            "pairs.out"),
    
    Similarity(SimilarityMapper.class,
            SimilarityCombiner.class,
            SimilarityReducer.class,
            MoviePairFormat.class,
            MoviePair.class,
            IntWritable.class,
            null);
    
    // Internal enum value properties    
    private final Class<? extends Mapper> mapperClass;
    private final Class<? extends Reducer> combinerClass;
    private final Class<? extends Reducer> reducerClass;
    private final Class<? extends FileInputFormat> inputFormatClass;
    private final Class<?> mapOutputKeyClass;
    private final Class<?> mapOutputValueClass;
    private final String outputFileName;

    public String getOutputFileName() {
        return outputFileName;
    }

    private MovieJob(Class<? extends Mapper> mapperClass,
            Class<? extends Reducer> combinerClass,
            Class<? extends Reducer> reducerClass,
            Class<? extends FileInputFormat> inputFormatClass,
            Class<?> mapOutputKeyClass,
            Class<?> mapOutputValueClass,
            String outputFileName) {

        this.mapperClass = mapperClass;
        this.combinerClass = combinerClass;
        this.reducerClass = reducerClass;
        this.inputFormatClass = inputFormatClass;
        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.outputFileName = outputFileName;
    }

    public void configureJob(Job job) {
        // Configure properties for the job to run.
        job.setMapperClass(mapperClass);
        if (combinerClass != null) {
            job.setCombinerClass(combinerClass);
        }
        job.setReducerClass(reducerClass);

        job.setInputFormatClass(inputFormatClass);

        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
    }
}
