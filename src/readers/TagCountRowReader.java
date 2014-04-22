package readers;

import customwritables.TagMovie;
import org.apache.hadoop.io.Text;

/**
 * Implementation of the RowReaderBase to read in the output of the first job, and return a (Text, TagMovie)
 * key-value pair, allowing us to pair up movies that share a tag.
 */
public class TagCountRowReader extends RowReaderBase<Text, TagMovie, TagMovie> {

    @Override
    protected TagMovie initValue() {
        if (internalValue == null) {
            internalValue = new TagMovie();
        }
        return internalValue;
    }

    @Override
    public Text getCurrentKey() {
        return internalValue.getTag();
    }

    @Override
    public TagMovie getCurrentValue() {
        return internalValue;
    }
}
