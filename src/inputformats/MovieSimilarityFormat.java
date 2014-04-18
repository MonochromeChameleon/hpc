package inputformats;

import customwritables.MovieSimilarity;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.MovieSimilarityRowReader;

/**
 * This parses the output from the second job, reading each row (movie 1 id,
 * movie 1 tag count, movie 2 id, movie 2 tag count) into a MoviePair custom
 * writable
 */
public class MovieSimilarityFormat extends InputFormatBase<NullWritable, MovieSimilarity> {

    @Override
    public Class<? extends RecordReader<NullWritable, MovieSimilarity>> getReaderClass() {
        return MovieSimilarityRowReader.class;
    }
}
