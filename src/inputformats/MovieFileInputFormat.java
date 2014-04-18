package inputformats;

import customwritables.MovieOrTag;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.MovieRowReader;

/**
 * This parses the tags.dat file into a TagRow custom writable
 */
public class MovieFileInputFormat extends InputFormatBase<NullWritable, MovieOrTag> {

    @Override
    public Class<? extends RecordReader<NullWritable, MovieOrTag>> getReaderClass() {
        return MovieRowReader.class;
    }    
}
