package readers;

import customwritables.TagMovie;

/**
 * Modified LineRecordReader
 */
public class TagCountRowReader extends RowReaderBase<TagMovie> {

    @Override
    protected TagMovie initValue() {
        if (value == null) {
            value = new TagMovie();
        }
        return value;
    }
}
