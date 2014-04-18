package readers;

import customwritables.MovieOrTag;

/**
 * Modified LineRecordReader
 */
public class DataFileRowReader extends RowReaderBase<MovieOrTag> {

    @Override
    protected MovieOrTag initValue() {
        if (value == null) {
            value = new MovieOrTag();
        }
        return value;
    }
}
