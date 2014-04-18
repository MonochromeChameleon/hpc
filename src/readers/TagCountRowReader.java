package readers;

import customwritables.Movie;
import customwritables.TagMovie;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * Modified LineRecordReader
 */
public class TagCountRowReader extends RowReaderBase<TagMovie> {

    // internal fields
    private Text tag = new Text();
    private Movie movie = new Movie();

    @Override
    public boolean nextKeyValue() throws IOException {
        if (value == null) {
            value = new TagMovie();
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
            // tag    movieId    numberOfTags    name
            String[] fields = line.toString().split("\t");

            // data must be correctly formed
            if (fields == null || fields.length != 4) {
                break;
            }

            tag.set(fields[0]);

            // parse movieId to an integer
            Integer parsedId = Integer.parseInt(fields[1]);
            Integer parsedNumberOfTags = Integer.parseInt(fields[2]);
            movie.set(parsedId, parsedNumberOfTags, fields[3]);

            value.set(tag, movie);

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
    public TagMovie getCurrentValue() {
        return value;
    }
}
