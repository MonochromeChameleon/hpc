package readers;

import customwritables.MovieOrTag;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Modified LineRecordReader
 */
public class MovieRowReader extends RowReaderBase<MovieOrTag> {

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
            // movieId::name (year)::genre|genre|genre...
            String[] fields = line.toString().split("::");

            // data must be correctly formed
            if (fields == null || fields.length != 3) {
                break;
            }

            // parse movieId to an integer
            Integer parsedId = Integer.parseInt(fields[0]);
            movieId.set(parsedId);

            String parsedName = fields[1].toLowerCase();
            parsedName.replaceAll("[\\p{ASCII}]|\\d", "");

            // Remove the year from the end of the movie name
            int yearIndex = parsedName.lastIndexOf("(");
            String nameWithoutYear = parsedName.substring(0, yearIndex - 1);

            // Fix e.g. "Firm, the" because nobody will search for that...
            if (nameWithoutYear.endsWith(", the")) {
                nameWithoutYear = "the " + nameWithoutYear.substring(0, nameWithoutYear.lastIndexOf(","));
            }

            name.set(nameWithoutYear);

            value.set(tag, movieId, name); // tag is always empty but we join later

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
