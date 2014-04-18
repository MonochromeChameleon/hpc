package readers;

import customwritables.MovieOrTag;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Modified LineRecordReader
 */
public class TagRowReader extends RowReaderBase<MovieOrTag> {

    // internal fields
    private Text tag = new Text();
    private IntWritable movieId = new IntWritable();
    private Text name = new Text();

    @Override
    public boolean nextKeyValue() throws IOException {
        if (value == null) {
            value = new MovieOrTag();
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
            // userId::movieId::tag::timestamp
            String[] fields = line.toString().split("::");

            // data must be correctly formed
            if (fields == null || fields.length != 4) {
                break;
            }

            // parse movieId to an integer
            Integer parsedId = Integer.parseInt(fields[1]);
            movieId.set(parsedId);

            String parsedTag = fields[2].toLowerCase();
            parsedTag.replaceAll("[\\p{ASCII}]|\\d", "");
            tag.set(parsedTag);

            value.set(tag, movieId, name); // name is always undefined when we read this, but it allows us to join later

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
    public MovieOrTag getCurrentValue() {
        return value;
    }
}
