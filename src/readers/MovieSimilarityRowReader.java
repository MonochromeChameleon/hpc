package readers;

import customwritables.MovieSimilarity;

/**
 * Modified LineRecordReader
 */
public class MovieSimilarityRowReader extends RowReaderBase<MovieSimilarity> {

    @Override
    protected MovieSimilarity initValue() {
        if (value == null) {
            value = new MovieSimilarity();
        }
        return value;
    }
}
