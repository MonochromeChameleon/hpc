package customwritables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface extending the WritableComparable hadoop interface to enforce the ability to parse a text line into an
 * instance of this writable.
 */
public interface MovieWritableBase<W> extends WritableComparable<W> {
    public abstract W parseInputLine(Text line);
}
