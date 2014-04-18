package inputformats;

import customwritables.MovieOrTag;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.TagRowReader;

/**
 * This parses the tags.dat file into a TagRow custom writable
 */
public class TagFileInputFormat extends InputFormatBase<NullWritable, MovieOrTag> {

    @Override
    public Class<? extends RecordReader<NullWritable, MovieOrTag>> getReaderClass() {
        return TagRowReader.class;
    }
}
