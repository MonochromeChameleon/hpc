package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Custom writable for storing the details of a Movie. This forms the underlying data structure for passing Movies 
 * around the other jobs.
 */
public class Movie implements WritableComparable<Movie> {

    private IntWritable movieId;
    private IntWritable numberOfTags;
    private Text name;

    public Movie() {
        set(new IntWritable(), new IntWritable(), new Text());
    }

    public Movie(int movieId, int numberOfTags, String name) {
        set(movieId, numberOfTags, name);
    }

    public final void set(IntWritable movieId, IntWritable numberOfTags, Text name) {
        this.movieId = movieId;
        this.numberOfTags = numberOfTags;
        this.name = name;
    }

    public final void set(int movieId, int numberOfTags, String name) {
        set(new IntWritable(movieId), new IntWritable(numberOfTags), new Text(name));
    }

    public IntWritable getMovieId() {
        return movieId;
    }

    public IntWritable getNumberOfTags() {
        return numberOfTags;
    }

    public Text getName() {
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        movieId.write(out);
        numberOfTags.write(out);
        name.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieId.readFields(in);
        numberOfTags.readFields(in);
        name.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((movieId == null) ? 0 : movieId.hashCode());
        result = prime * result + ((numberOfTags == null) ? 0 : numberOfTags.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Movie) {
            Movie m = (Movie) o;
            return movieId.equals(m.getMovieId()) && numberOfTags.equals(m.getNumberOfTags()) && name.equals(m.getName());
        }
        return false;
    }

    @Override
    public String toString() {
        return movieId + "\t" + numberOfTags + "\t" + name;
    }

    @Override
    public int compareTo(Movie m) {
        int cmp = movieId.compareTo(m.getMovieId());
        if (cmp != 0) {
            return cmp;
        }
        cmp = numberOfTags.compareTo(m.getNumberOfTags());
        if (cmp != 0) {
            return cmp;
        }
        return name.compareTo(m.getName());
    }
}
