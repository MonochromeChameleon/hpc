package customwritables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author Hugh
 */
public interface MovieWritableBase<W> extends WritableComparable<W> {
    public abstract W parseInputLine(Text line);
}
