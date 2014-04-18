package inputformats;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author Hugh
 * @param <K>
 * @param <V>
 */
public abstract class InputFormatBase<K,V> extends FileInputFormat<K, V> {

    public abstract Class<? extends RecordReader<K, V>> getReaderClass();

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        try {
            return getReaderClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}
