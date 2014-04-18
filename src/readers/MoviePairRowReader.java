package readers;

import customwritables.Movie;
import customwritables.MoviePair;
import java.io.IOException;

/**
 * Modified LineRecordReader
 */
public class MoviePairRowReader extends RowReaderBase<MoviePair> {

    // internal fields
    private Movie movie1 = new Movie();
    private Movie movie2 = new Movie();

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
            // movie1Id    movie1NumberOfTags    movie1Name    movie2Id    movie2NumberOfTags    movie2Name
            String[] fields = line.toString().split("\t");

            // data must be correctly formed
            if (fields == null || fields.length != 6) {
                break;
            }

            // parse movieId to an integer
            Integer parsedId1 = Integer.parseInt(fields[0]);
            Integer parsedTags1 = Integer.parseInt(fields[1]);
            String name1 = fields[2];
            movie1.set(parsedId1, parsedTags1, name1);

            Integer parsedId2 = Integer.parseInt(fields[3]);
            Integer parsedTags2 = Integer.parseInt(fields[4]);
            String name2 = fields[5];
            movie2.set(parsedId2, parsedTags2, name2);

            value.set(movie1, movie2);

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
    public MoviePair getCurrentValue() {
        return value;
    }
}
