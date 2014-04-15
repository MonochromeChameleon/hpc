package customwritables;

import java.io.*;

import org.apache.hadoop.io.*;

public class IntQuartet implements WritableComparable<IntQuartet> {

    private IntWritable first;
    private IntWritable second;
    private IntWritable third;
    private IntWritable fourth;

    public IntQuartet() {
        set(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable());
    }

    public IntQuartet(int first, int second, int third, int fourth) {
        set(first, second, third, fourth);
    }
    
    public final void set(IntWritable first, IntWritable second, IntWritable third, IntWritable fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public final void set(int first, int second, int third, int fourth) {
        set(new IntWritable(first), new IntWritable(second), new IntWritable(third), new IntWritable(fourth));
    }

    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }
    
    public IntWritable getThird() {
        return third;
    }
    
    public IntWritable getFourth() {
        return fourth;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        third.write(out);
        fourth.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        third.readFields(in);
        fourth.readFields(in);
    }

    @Override
    public int hashCode() {
        return (first.hashCode() * 163) + (second.hashCode() * 37) + (third.hashCode() * 13) + fourth.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IntQuartet) {
            IntQuartet tp = (IntQuartet) o;
            return first.equals(tp.first) && second.equals(tp.second) && third.equals(tp.third) && fourth.equals(tp.fourth);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second + "\t" + third + "\t" + fourth;
    }

    @Override
    public int compareTo(IntQuartet tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        cmp = second.compareTo(tp.second);
        if (cmp != 0) {
            return cmp;
        }
        cmp = third.compareTo(tp.third);
        if (cmp != 0) {
            return cmp;
        }
        return fourth.compareTo(tp.fourth);
    }
}
