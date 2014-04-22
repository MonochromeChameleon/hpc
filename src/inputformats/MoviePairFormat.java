package inputformats;

import customwritables.MoviePair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.MoviePairRowReader;

/**
 * This parses the output from the second job, reading each row (movie 1 id, movie 1 tag count, movie 2 id,
 * movie 2 tag count) into a MoviePair custom writable.
 */
public class MoviePairFormat extends InputFormatBase<MoviePair, IntWritable> {

    @Override
    public Class<? extends RecordReader<MoviePair, IntWritable>> getReaderClass() {
        return MoviePairRowReader.class;
    }
}
