package readers;

import customwritables.MoviePair;

/**
 * Modified LineRecordReader
 */
public class MoviePairRowReader extends RowReaderBase<MoviePair> {

    @Override
    protected MoviePair initValue() {
        if (value == null) {
            value = new MoviePair();
        }
        return value;
    }
}
