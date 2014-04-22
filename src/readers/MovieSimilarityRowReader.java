package readers;

import customwritables.MovieSimilarity;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of the RowReaderBase to read in the output of the third job, and return a MovieSimilarity, which we 
 * can then filter to a list of appropriate recommendations.
 */
public class MovieSimilarityRowReader extends RowReaderBase<NullWritable, MovieSimilarity, MovieSimilarity> {
 
    @Override
    protected MovieSimilarity initValue() {
        if (internalValue == null) {
            internalValue = new MovieSimilarity();
        }
        return internalValue;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public MovieSimilarity getCurrentValue() {
        return internalValue;
    }
}
