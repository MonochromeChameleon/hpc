package readers;

import customwritables.MovieOrTag;
import org.apache.hadoop.io.IntWritable;

/**
 * Implementation of the RowReaderBase to read in the tags.dat and movies.dat files, returning a (movie id, MovieOrTag)
 * key-value pair.
 */
public class DataFileRowReader extends RowReaderBase<IntWritable,MovieOrTag, MovieOrTag> {

    @Override
    protected MovieOrTag initValue() {
        if (internalValue == null) {
            internalValue = new MovieOrTag();
        }
        return internalValue;
    }

    @Override
    public IntWritable getCurrentKey() {
        return internalValue.getMovieId();
    }

    @Override
    public MovieOrTag getCurrentValue() {
        return internalValue;
    }
}
