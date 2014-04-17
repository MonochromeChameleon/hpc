package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is used for receiving the input data from the provided .dat files - either tags.dat or movies.dat
 */
public class MovieOrTag implements WritableComparable<MovieOrTag> {

    private Text tag;
    private IntWritable movieId;
    private Text name;
    
    public MovieOrTag() {
        set(new Text(), new IntWritable(), new Text());
    }
    
    public MovieOrTag(String tag, int movieId, String name) {
        set(new Text(tag), new IntWritable(movieId), new Text(name));
    }
    
    public MovieOrTag(Text tag, IntWritable movieId, Text name) {
        set(tag, movieId, name);
    }

    public final void set(Text tag, IntWritable movieId, Text name) {
        this.tag = tag;
        this.movieId = movieId;
        this.name = name;
    }
    
    public Text getTag() {
        return tag;
    }

    public IntWritable getMovieId() {
        return movieId;
    }
    
    public Text getName() {
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tag.write(out);
        movieId.write(out);
        name.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag.readFields(in);
        movieId.readFields(in);
        name.readFields(in);
    }

    @Override
    public String toString() {
        return tag + "\t" + movieId + "\t" + name;
    }

    @Override
    public int compareTo(MovieOrTag t) {
        int cmp = tag.compareTo(t.getTag());
        if (cmp != 0) {
            return cmp;
        }
        cmp = movieId.compareTo(t.getMovieId());
        if (cmp != 0) {
            return cmp;
        }
        return name.compareTo(t.getName());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result + ((movieId == null) ? 0 : movieId.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MovieOrTag) {
            MovieOrTag t = (MovieOrTag) obj;
            return tag.equals(t.getTag()) && movieId.equals(t.getMovieId()) && name.equals(t.getName());
        }
        return false;
    }
}
