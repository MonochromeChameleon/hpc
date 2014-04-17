package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is the output custom writable for the first job, storing a instance of a tag and a movie id, as well as the total
 * number of tags associated with that movie.
 * It is also the input writable for the second job, wherein we pair up movies that share a tag, while keeping track of
 * the total number of tags associated with each of those movies.
 */
public class TagMovie implements WritableComparable<TagMovie> {

    private Text tag;
    private IntWritable movieId;
    private IntWritable numberOfTags;
    
    public TagMovie() {
        set(new Text(), new IntWritable(), new IntWritable());
    }
    
    public TagMovie(String tag, int movieId, int numberOfTags) {
        set(new Text(tag), new IntWritable(movieId), new IntWritable(numberOfTags));
    }
    
    public TagMovie(Text tag, IntWritable movieId, IntWritable numberOfTags) {
        set(tag, movieId, numberOfTags);
    }

    public void set(Text tag, IntWritable movieId, IntWritable numberOfTags) {
        this.tag = tag;
        this.movieId = movieId;
        this.numberOfTags = numberOfTags;
    }
    
    public Text getTag() {
        return tag;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }

    public IntWritable getMovieId() {
        return movieId;
    }

    public void setMovieId(IntWritable movieId) {
        this.movieId = movieId;
    }

    public IntWritable getNumberOfTags() {
        return numberOfTags;
    }

    public void setNumberOfTags(IntWritable numberOfTags) {
        this.numberOfTags = numberOfTags;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tag.write(out);
        movieId.write(out);
        numberOfTags.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag.readFields(in);
        movieId.readFields(in);
        numberOfTags.readFields(in);
    }

    @Override
    public String toString() {
        return tag + "\t" + movieId + "\t" + numberOfTags;
    }

    @Override
    public int compareTo(TagMovie t) {
        int cmp = tag.compareTo(t.getTag());
        if (cmp != 0) {
            return cmp;
        }
        cmp = movieId.compareTo(t.getMovieId());
        if (cmp != 0) {
            return cmp;
        }
        return numberOfTags.compareTo(t.getNumberOfTags());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result + ((movieId == null) ? 0 : movieId.hashCode());
        result = prime * result + ((numberOfTags == null) ? 0 : numberOfTags.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TagMovie) {
            TagMovie t = (TagMovie) obj;
            return tag.equals(t.getTag()) && movieId.equals(t.getMovieId()) && numberOfTags.equals(t.getNumberOfTags());
        }
        return false;
    }
}
