package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is the ouput writable for the second job, representing a pair of related movies and the number of tags associated
 * with each of them.
 * It is also the input writable for the third job, where we count the number of rows in order to determine the number of
 * shared tags for each pair.
 */
public class MoviePair implements WritableComparable<MoviePair> {

    private IntWritable movie1Id;
    private IntWritable movie1NumberOfTags;
    private IntWritable movie2Id;
    private IntWritable movie2NumberOfTags;
    
    public MoviePair() {
        set(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable());
    }
    
    public MoviePair(String tag, int movie1Id, int movie1NumberOfTags, int movie2Id, int movie2NumberOfTags) {
        set(new IntWritable(movie1Id), new IntWritable(movie1NumberOfTags), new IntWritable(movie2Id), new IntWritable(movie2NumberOfTags));
    }
    
    public MoviePair(Text tag, IntWritable movie1Id, IntWritable movie1NumberOfTags, IntWritable movie2Id, IntWritable movie2NumberOfTags) {
        set(movie1Id, movie1NumberOfTags, movie2Id, movie2NumberOfTags);
    }

    public void set(IntWritable movie1Id, IntWritable movie1NumberOfTags, IntWritable movie2Id, IntWritable movie2NumberOfTags) {
        this.movie1Id = movie1Id;
        this.movie1NumberOfTags = movie1NumberOfTags;
        this.movie2Id = movie2Id;
        this.movie2NumberOfTags = movie2NumberOfTags;
    }
    
    public IntWritable getMovie1Id() {
        return movie1Id;
    }

    public void setMovie1Id(IntWritable movie1Id) {
        this.movie1Id = movie1Id;
    }

    public IntWritable getMovie1NumberOfTags() {
        return movie1NumberOfTags;
    }

    public void setMovie1NumberOfTags(IntWritable movie1NumberOfTags) {
        this.movie1NumberOfTags = movie1NumberOfTags;
    }

    public IntWritable getMovie2Id() {
        return movie2Id;
    }

    public void setMovie2Id(IntWritable movie2Id) {
        this.movie2Id = movie2Id;
    }

    public IntWritable getMovie2NumberOfTags() {
        return movie2NumberOfTags;
    }

    public void setMovie2NumberOfTags(IntWritable movie2NumberOfTags) {
        this.movie2NumberOfTags = movie2NumberOfTags;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        movie1Id.write(out);
        movie1NumberOfTags.write(out);
        movie2Id.write(out);
        movie2NumberOfTags.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie1Id.readFields(in);
        movie1NumberOfTags.readFields(in);
        movie2Id.readFields(in);
        movie2NumberOfTags.readFields(in);
    }

    @Override
    public String toString() {
        return movie1Id + "\t" + movie1NumberOfTags + "\t" + movie2Id + "\t" + movie2NumberOfTags;
    }

    @Override
    public int compareTo(MoviePair t) {
        int cmp = movie1Id.compareTo(t.getMovie1Id());
        if (cmp != 0) {
            return cmp;
        }
        cmp = movie1NumberOfTags.compareTo(t.getMovie1NumberOfTags());
        if (cmp != 0) {
            return cmp;
        }
        cmp = movie2Id.compareTo(t.getMovie2Id());
        if (cmp != 0) {
            return cmp;
        }
        return movie2NumberOfTags.compareTo(t.getMovie2NumberOfTags());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((movie1Id == null) ? 0 : movie1Id.hashCode());
        result = prime * result + ((movie1NumberOfTags == null) ? 0 : movie1NumberOfTags.hashCode());
        result = prime * result + ((movie2Id == null) ? 0 : movie2Id.hashCode());
        result = prime * result + ((movie2NumberOfTags == null) ? 0 : movie2NumberOfTags.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MoviePair) {
            MoviePair t = (MoviePair) obj;
            return movie1Id.equals(t.getMovie1Id()) && 
                    movie1NumberOfTags.equals(t.getMovie1NumberOfTags()) && 
                    movie2Id.equals(t.getMovie2Id()) && 
                    movie2NumberOfTags.equals(t.getMovie2NumberOfTags());
        }
        return false;
    }
}
