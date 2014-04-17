package inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author hwg30
 */
public class TagRow implements WritableComparable<TagRow> {

    private Text tag;
    private IntWritable movieId;
    
    public TagRow() {
        set(new Text(), new IntWritable());
    }
    
    public TagRow(String tag, int movieId) {
        set(new Text(tag), new IntWritable(movieId));
    }
    
    public TagRow(Text tag, IntWritable movieId) {
        set(tag, movieId);
    }

    public void set(Text tag, IntWritable movieId) {
        this.tag = tag;
        this.movieId = movieId;
    }
    
    public void set(String tag, int movieId) {
        this.tag = new Text(tag);
        this.movieId = new IntWritable(movieId);
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

    @Override
    public void write(DataOutput out) throws IOException {
        tag.write(out);
        movieId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag.readFields(in);
        movieId.readFields(in);
    }

    @Override
    public String toString() {
        return "[" + tag + "\t" + movieId + "]";
    }

    @Override
    public int compareTo(TagRow t) {
        int cmp = tag.compareTo(t.getTag());
        if (cmp != 0) {
            return cmp;
        }
        return movieId.compareTo(t.getMovieId());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result + ((movieId == null) ? 0 : movieId.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TagRow) {
            TagRow t = (TagRow) obj;
            return tag.equals(t.getTag()) && movieId.equals(t.getMovieId());
        }
        return false;
    }
}
