package inputformats;

import customwritables.MovieOrTag;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import readers.DataFileRowReader;

/**
 * This parses the tags.dat and movies.dat files into TagRow custom writables
 */
public class DataFileInputFormat extends InputFormatBase<IntWritable, MovieOrTag> {

    @Override
    public Class<? extends RecordReader<IntWritable, MovieOrTag>> getReaderClass() {
        return DataFileRowReader.class;
    }
}
