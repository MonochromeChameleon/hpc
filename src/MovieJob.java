
import customwritables.Movie;
import inputformats.MoviePairFormat;
import customwritables.MoviePair;
import inputformats.TagCountFormat;
import customwritables.TagMovie;
import inputformats.DataFileInputFormat;
import customwritables.MovieOrTag;
import customwritables.MovieSimilarity;
import inputformats.MovieSimilarityFormat;
import moviePairs.MoviePairReducer;
import tagCount.TagCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import recommendation.MovieFilter;
import recommendation.RecommendationReducer;
import similarity.SimilarityCombiner;
import similarity.SimilarityReducer;

/**
 * Config enum defining the various mapper/combiner/reducer/input format/output format classes for each of the jobs that
 * comprise the overall movie recommendations program.
 */
public enum MovieJob {

    // Define available jobs as enum values, each with associated Mapper/Combiner/Reducer 
    // classes, and output key/value classes
    TagCount(null,
            null,
            TagCountReducer.class,
            DataFileInputFormat.class,
            IntWritable.class,
            MovieOrTag.class,
            "tags.out"),

    MoviePairs(null,
            null,
            MoviePairReducer.class,
            TagCountFormat.class,
            Text.class,
            TagMovie.class,
            "pairs.out"),
    
    Similarity(null,
            SimilarityCombiner.class,
            SimilarityReducer.class,
            MoviePairFormat.class,
            MoviePair.class,
            IntWritable.class,
            "similarity.out"),
    
    Recommendation(MovieFilter.class,
            null,
            RecommendationReducer.class,
            MovieSimilarityFormat.class,
            Movie.class,
            MovieSimilarity.class,
            null);
    
    // Internal enum value properties    
    private final Class<? extends Mapper> mapperClass;
    private final Class<? extends Reducer> combinerClass;
    private final Class<? extends Reducer> reducerClass;
    private final Class<? extends FileInputFormat> inputFormatClass;
    private final Class<?> mapOutputKeyClass;
    private final Class<?> mapOutputValueClass;
    private final String outputFileName;

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    public Class<?> getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public Class<?> getMapOutputValueClass() {
        return mapOutputValueClass;
    }

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
        if (mapperClass != null) {
            job.setMapperClass(mapperClass);
        }
        if (combinerClass != null) {
            job.setCombinerClass(combinerClass);
        }
        if (reducerClass != null) {
            job.setReducerClass(reducerClass);
        }

        job.setInputFormatClass(inputFormatClass);

        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
        
        job.setNumReduceTasks(3);
    }
}
