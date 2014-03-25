package customwritables;
import java.io.*;
import org.apache.hadoop.io.*;

public class TextIntPair implements WritableComparable<TextIntPair> {

	private Text text;
	private IntWritable iw;  
	
	public TextIntPair() {
		set(new Text(), new IntWritable());
	}

	public TextIntPair(String text, int iw) {
		set(new Text(text), new IntWritable(iw));
	}

	public void set(Text text, IntWritable iw) {
		this.text = text;
		this.iw = iw;

	}

	public Text getText() {
		return text;
	}

	public IntWritable getIw() {
		return iw;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
		iw.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		iw.readFields(in);
		
	}
	
	@Override
	public String toString() {
		return "["+ text + "," + iw + "]";
	}

	@Override
	public int compareTo(TextIntPair st) {
		int cmp = text.compareTo(st.getText());
		if (cmp != 0) {
			return cmp;
		}
		return iw.compareTo(st.getIw());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		result = prime * result + ((text == null) ? 0 : text.hashCode());
		result = prime * result + ((iw == null) ? 0 : iw.hashCode());
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextIntPair) {
			TextIntPair st = (TextIntPair) obj;
			return text.equals(st.getText()) && iw.equals(st.getIw());
		}
		return false;
	}
}