package inputformats;

import customwritables.TagMovie;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.TagCountRowReader;

/**
 * This parses the output from the first job, reading each row (tag, movie id, movie tag count) into a TagMovie custom
 * writable.
 */
public class TagCountFormat extends InputFormatBase<Text, TagMovie> {

    @Override
    public Class<? extends RecordReader<Text, TagMovie>> getReaderClass() {
        return TagCountRowReader.class;
    }
}
