package inputformats;

import customwritables.MoviePair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.MoviePairRowReader;

/**
 * This parses the output from the second job, reading each row (movie 1 id, movie 1 tag count, movie 2 id, movie 2 tag count)
 * into a MoviePair custom writable
 */
public class MoviePairFormat extends InputFormatBase<NullWritable, MoviePair> {

    @Override
    public Class<? extends RecordReader<NullWritable, MoviePair>> getReaderClass() {
        return MoviePairRowReader.class;
    }
}
