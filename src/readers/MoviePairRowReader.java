package readers;

import customwritables.MoviePair;
import org.apache.hadoop.io.IntWritable;

/**
 * Implementation of the RowReaderBase to read in the output of the second job, and return a (MoviePair, IntWritable)
 * key-value pair, allowing us to count up instances of each pairing for determining their similarity.
 */
public class MoviePairRowReader extends RowReaderBase<MoviePair, IntWritable, MoviePair> {
       
    private final IntWritable one = new IntWritable(1);

    @Override
    protected MoviePair initValue() {
        if (internalValue == null) {
            internalValue = new MoviePair();
        }
        return internalValue;
    }

    @Override
    public MoviePair getCurrentKey() {
        return internalValue;
    }

    @Override
    public IntWritable getCurrentValue() {
        return one;
    }
}
