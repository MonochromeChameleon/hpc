import customwritables.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;

public class TagsReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        
		Text moviePair = new Text();

		ArrayList<String> checkedMovies = new ArrayList<String>();
		String m1 = null;
		String m2 = null;

		for (Text movie1 : values) {
			m1 = movie1.toString();
			for (Text movie2 : values) {
				m2 = movie2.toString();
				if (m1.equals(m2)){
					//if two movies are the same do nothing
				if (checkedMovies.contains(m2)){
					//if this pair has already been processed do nothing	
				}
				} else {
					String pair = m1 + "-" + m2;
					moviePair.set(pair);
					context.write(moviePair, key);
				}
			}
			checkedMovies.add(m1);
		}

	}
}

