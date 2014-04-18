package inputformats;

import customwritables.TagMovie;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.TagCountRowReader;

/**
 * This parses the output from the first job, reading each row (tag, movie id, movie tag count) into a TagMovie custom writable
 */
public class TagCountFormat extends InputFormatBase<NullWritable, TagMovie> {

    @Override
    public Class<? extends RecordReader<NullWritable, TagMovie>> getReaderClass() {
        return TagCountRowReader.class;
    }
}
