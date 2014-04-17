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

    private Movie movie1;
    private Movie movie2;
    
    public MoviePair() {
        set(new Movie(), new Movie());
    }
    
    public MoviePair(Movie movie1, Movie movie2) {
        set(movie1, movie2);
    }

    public void set(Movie movie1, Movie movie2) {
        this.movie1 = movie1;
        this.movie2 = movie2;
    }
    
    public Movie getMovie1() {
        return movie1;
    }

    public Movie getMovie2() {
        return movie2;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        movie1.write(out);
        movie2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie1.readFields(in);
        movie2.readFields(in);
    }

    @Override
    public String toString() {
        return movie1 + "\t" + movie2;
    }

    @Override
    public int compareTo(MoviePair t) {
        int cmp = movie1.compareTo(t.getMovie1());
        if (cmp != 0) {
            return cmp;
        }
        return movie2.compareTo(t.getMovie2());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((movie1 == null) ? 0 : movie1.hashCode());
        result = prime * result + ((movie2 == null) ? 0 : movie2.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MoviePair) {
            MoviePair t = (MoviePair) obj;
            return movie1.equals(t.getMovie1()) && 
                    movie2.equals(t.getMovie2());
        }
        return false;
    }
}
