package customwritables;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.*;

public class MovieAndTags implements WritableComparable<MovieAndTags> {

    private ArrayWritable tags;
    private IntWritable movie;

    public MovieAndTags() {
        set(new IntWritable(), new ArrayWritable(Text.class));
    }

    public MovieAndTags(int movie, String... tags) {
        set(new IntWritable(movie), new ArrayWritable(tags));
    }
    
    public void set(IntWritable movie, String... tags) {
        this.movie = movie;
        this.tags = new ArrayWritable(tags);
    }

    public void set(IntWritable movie, ArrayWritable tags) {
        this.tags = tags;
        this.movie = movie;
    }

    public ArrayWritable getTags() {
        return tags;
    }

    public IntWritable getMovie() {
        return movie;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        tags.write(out);
        movie.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tags.readFields(in);
        movie.readFields(in);

    }

    @Override
    public String toString() {
        Iterable<String> tagsToString = Iterables.transform(Lists.newArrayList(tags.get()), new Function<Writable, String>() {

            @Override
            public String apply(Writable f) {
                return f.toString();
            }
        });
        
        
        return "[" + StringUtils.join(tagsToString.iterator(), " ") + "," + movie + "]";
    }

    @Override
    public int compareTo(MovieAndTags st) {
        int cmp = movie.compareTo(st.getMovie());
        if (cmp != 0) {
            return cmp;
        }
        //TODO
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + ((movie == null) ? 0 : movie.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MovieAndTags) {
            MovieAndTags st = (MovieAndTags) obj;
            return movie.equals(st.getMovie()) && tags.equals(st.getTags());
        }
        return false;
    }
}