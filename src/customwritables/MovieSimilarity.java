package customwritables;

import java.io.*;

import org.apache.hadoop.io.*;

public class MovieSimilarity implements WritableComparable<MovieSimilarity> {

    private Movie movie1;
    private Movie movie2;
    private FloatWritable similarity;

    public MovieSimilarity() {
        set(new Movie(), new Movie(), new FloatWritable());
    }

    public final void set(Movie movie1, Movie movie2, FloatWritable similarity) {
        this.movie1 = movie1;
        this.movie2 = movie2;
        this.similarity = similarity;
    }

    public Movie getMovie1() {
        return movie1;
    }

    public Movie getMovie2() {
        return movie2;
    }

    public FloatWritable getSimilarity() {
        return similarity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        movie1.write(out);
        movie2.write(out);
        similarity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie1.readFields(in);
        movie2.readFields(in);
        similarity.readFields(in);
    }

    @Override
    public int hashCode() {
        return (movie1.hashCode() * 163) + (movie2.hashCode()* 37) + similarity.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MovieSimilarity) {
            MovieSimilarity m = (MovieSimilarity) o;
            return movie1.equals(m.getMovie1()) && movie2.equals(m.getMovie2()) && similarity.equals(m.getSimilarity());
        }
        return false;
    }

    @Override
    public String toString() {
        return movie1 + "\t" + movie2 + "\t" + similarity;
    }

    @Override
    public int compareTo(MovieSimilarity m) {
        int cmp = movie1.compareTo(m.getMovie1());
        if (cmp != 0) {
            return cmp;
        }
        cmp = movie2.compareTo(m.getMovie2());
        if (cmp != 0) {
            return cmp;
        }
        return similarity.compareTo(m.getSimilarity());
    }
}
