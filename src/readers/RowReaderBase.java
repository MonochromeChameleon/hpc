package readers;

import customwritables.MovieWritableBase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Generic abstraction of the LineRecordReader, extended by the various concrete implementations to allow us to 
 * construct our custom writables directly on import, rather than having to build them inside a mapper class.
 */
public abstract class RowReaderBase<K, V, I extends MovieWritableBase> extends RecordReader<K, V> {

    protected CompressionCodecFactory compressionCodecs = null;
    protected long start;
    protected long pos;
    protected long end;
    protected LineReader in;
    protected int maxLineLength;
    protected I internalValue = null;
    protected Text line = new Text();
    
    protected abstract I initValue();

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }
        if (skipFirstLine) { // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException {
        initValue();

        //key.set(pos);
        int newSize = 0;

        while (pos < end) {
            newSize = in.readLine(line, maxLineLength, Math.max(
                    (int) Math.min(Integer.MAX_VALUE, end - pos),
                    maxLineLength));
            if (newSize == 0) {
                break;
            }
            
            if (internalValue.parseInputLine(line) == null) {
                break;
            }

            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

        }
        if (newSize == 0) {
            internalValue = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public abstract K getCurrentKey();

    @Override
    public abstract V getCurrentValue();

    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
