package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

/**
 * This is the output custom writable for the first job, storing a instance of a tag and a movie id, as well as the total
 * number of tags associated with that movie.
 * It is also the input writable for the second job, wherein we pair up movies that share a tag, while keeping track of
 * the total number of tags associated with each of those movies.
 */
public class TagMovie implements MovieWritableBase<TagMovie> {

    private Text tag;
    private Movie movie;
    
    public TagMovie() {
        set(new Text(), new Movie());
    }
    
    public TagMovie(String tag, int movieId, int numberOfTags, String name) {
        set(new Text(tag), new Movie(movieId, numberOfTags, name));
    }
    
    public TagMovie(Text tag, Movie movie) {
        set(tag, movie);
    }

    public final void set(Text tag, Movie movie) {
        this.tag = tag;
        this.movie = movie;
    }
    
    @Override
    public TagMovie parseInputLine(Text line) {
        // fields:
        // tag    movieId    numberOfTags    name
        String[] fields = line.toString().split("\t");

        // data must be correctly formed
        if (fields == null || fields.length != 4) {
            return null;
        }

        tag.set(fields[0]);

        // parse movieId to an integer
        Integer parsedId = Integer.parseInt(fields[1]);
        Integer parsedNumberOfTags = Integer.parseInt(fields[2]);
        movie.set(parsedId, parsedNumberOfTags, fields[3]);

        return this;
    }
    
    public Text getTag() {
        return tag;
    }

    public Movie getMovie() {
        return movie;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tag.write(out);
        movie.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag.readFields(in);
        movie.readFields(in);
    }

    @Override
    public String toString() {
        return tag + "\t" + movie;
    }

    @Override
    public int compareTo(TagMovie t) {
        int cmp = tag.compareTo(t.getTag());
        if (cmp != 0) {
            return cmp;
        }
        return movie.compareTo(t.getMovie());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result + ((movie == null) ? 0 : movie.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TagMovie) {
            TagMovie t = (TagMovie) obj;
            return tag.equals(t.getTag()) && movie.equals(t.getMovie());
        }
        return false;
    }
}
