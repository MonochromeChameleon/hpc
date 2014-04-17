package inputformats;

import customwritables.MoviePair;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * This parses the output from the second job, reading each row (movie 1 id, movie 1 tag count, movie 2 id, movie 2 tag count)
 * into a MoviePair custom writable
 */
public class MoviePairFormat extends FileInputFormat<NullWritable, MoviePair> {

    @Override
    public RecordReader<NullWritable, MoviePair> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new TagCountRowReader();
    }

    /**
     * Modified LineRecordReader
     */
    public class TagCountRowReader extends RecordReader<NullWritable, MoviePair> {

        private CompressionCodecFactory compressionCodecs = null;
        private long start;
        private long pos;
        private long end;
        private LineReader in;
        private int maxLineLength;
        private MoviePair value = null;
        private Text line = new Text();
        
        // internal fields
        private Text tag = new Text();
        private IntWritable movie1Id = new IntWritable();
        private IntWritable movie1NumberOfTags = new IntWritable();
        private IntWritable movie2Id = new IntWritable();
        private IntWritable movie2NumberOfTags = new IntWritable();

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
            if (value == null) {
                value = new MoviePair();
            }

            //key.set(pos);

            int newSize = 0;

            while (pos < end) {
                newSize = in.readLine(line, maxLineLength, Math.max(
                        (int) Math.min(Integer.MAX_VALUE, end - pos),
                        maxLineLength));
                if (newSize == 0) {
                    break;
                }

                // fields:
                // movie1Id    movie1NumberOfTags    movie2Id    movie2NumberOfTags
                String[] fields = line.toString().split("\t");

                // data must be correctly formed
                if (fields == null || fields.length != 4) {
                    break;
                }

                // parse movieId to an integer
                Integer parsedId1 = Integer.parseInt(fields[0]);
                movie1Id.set(parsedId1);
                
                Integer parsedTags1 = Integer.parseInt(fields[1]);
                movie1NumberOfTags.set(parsedTags1);
                
                Integer parsedId2 = Integer.parseInt(fields[2]);
                movie2Id.set(parsedId2);
                
                Integer parsedTags2 = Integer.parseInt(fields[3]);
                movie2NumberOfTags.set(parsedTags2);

                value.set(movie1Id, movie1NumberOfTags, movie2Id, movie2NumberOfTags);

                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }

            }
            if (newSize == 0) {
                value = null;
                return false;
            } else {
                return true;
            }
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public MoviePair getCurrentValue() {
            return value;
        }

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

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
}
