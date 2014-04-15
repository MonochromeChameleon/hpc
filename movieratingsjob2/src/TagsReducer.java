
import customwritables.IntIntPair;
import customwritables.IntQuartet;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TagsReducer extends Reducer<Text, IntIntPair, IntQuartet, Text> {

    @Override
    public void reduce(Text key, Iterable<IntIntPair> values, Context context) throws IOException, InterruptedException {

        IntQuartet movieAndTagCountPair = new IntQuartet();
        Text t = new Text();

        ArrayList<String> checkedMovies = new ArrayList<>();
        
        Iterator<IntIntPair> valuesIterator = values.iterator();
        List<IntIntPair> valueListClone1 = new ArrayList<>();
        List<IntIntPair> valueListClone2 = new ArrayList<>();
        
        while (valuesIterator.hasNext()) {
            // This is ugly, but the nested iterators fail unless we create two distinct (i.e. not object-equal)
            // iterators before we handle our nested loop.
            IntIntPair value = valuesIterator.next();
            valueListClone1.add(new IntIntPair(value.getFirst().get(), value.getSecond().get()));
            valueListClone2.add(new IntIntPair(value.getFirst().get(), value.getSecond().get()));
        }
        
        Iterator<IntIntPair> movie1Iterator = valueListClone1.iterator();
        while (movie1Iterator.hasNext()) {
            IntIntPair movie1 = movie1Iterator.next();
            
            // Ensure that we don't handle both sides of a pair (i.e. 1 & 2 vs. 2 & 1)
            // We do this before the inner loop to avoid pairing movies with themselves
            checkedMovies.add(movie1.getFirst().toString());
            
            Iterator<IntIntPair> movie2Iterator = valueListClone2.iterator();
            while (movie2Iterator.hasNext()) {
                IntIntPair movie2 = movie2Iterator.next();
                
                // Skip any movie ids that are already handled.
                if (checkedMovies.contains(movie2.getFirst().toString())) {
                    continue;
                }
                
                movieAndTagCountPair.set(movie1.getFirst(), movie1.getSecond(), movie2.getFirst(), movie2.getSecond());
                context.write(movieAndTagCountPair, key);
            }
        }
    }
}
