package customwritables;

import java.io.*;

import org.apache.hadoop.io.*;

public class MovieSimilarity implements MovieWritableBase<MovieSimilarity> {

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
    
    @Override
    public MovieSimilarity parseInputLine(Text line) {
        // fields:
        // movie1Id    numberOfTags    name    movie2Id    numberOfTags    name    similarity
        String[] fields = line.toString().split("\t");

        // data must be correctly formed
        if (fields == null || fields.length != 7) {
            return null;
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

        Float parsedSimilarity = Float.parseFloat(fields[6]);
        similarity.set(parsedSimilarity);

        return this;
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
