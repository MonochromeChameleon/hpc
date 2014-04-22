package customwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * This is the input writable for the recommendation job, where we filter the movie similarities file by movie name,
 * and return a formatted list of relevant recommendations.
 */
public class MovieSimilarity implements MovieWritableBase<MovieSimilarity> {

    private MoviePair pair;
    private FloatWritable similarity;

    public MovieSimilarity() {
        set(new MoviePair(), new FloatWritable());
    }

    public final void set(MoviePair pair, FloatWritable similarity) {
        this.pair = pair;
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
        
        pair.parseInputArray(fields);

        Float parsedSimilarity = Float.parseFloat(fields[6]);
        similarity.set(parsedSimilarity);

        return this;
    }

    public MoviePair getPair() {
        return pair;
    }

    public FloatWritable getSimilarity() {
        return similarity;
    }
    
    // Utility accessors

    public Movie getMovie1() {
        return pair.getMovie1();
    }
    
    public Movie getMovie2() {
        return pair.getMovie2();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        pair.write(out);
        similarity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pair.readFields(in);
        similarity.readFields(in);
    }

    @Override
    public int hashCode() {
        return (pair.hashCode() * 37) + similarity.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof MovieSimilarity) {
            MovieSimilarity m = (MovieSimilarity) o;
            return pair.equals(m.getPair()) && similarity.equals(m.getSimilarity());
        }
        return false;
    }

    @Override
    public String toString() {
        return pair + "\t" + similarity;
    }

    @Override
    public int compareTo(MovieSimilarity m) {
        int cmp = pair.compareTo(m.getPair());
        if (cmp != 0) {
            return cmp;
        }
        return similarity.compareTo(m.getSimilarity());
    }
}
