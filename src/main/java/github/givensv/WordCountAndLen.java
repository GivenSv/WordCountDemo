package github.givensv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WordCountAndLen implements Writable {
    private IntWritable count;
    private IntWritable length;

    private void set(IntWritable count, IntWritable length) {
        this.count = count;
        this.length = length;
    }

    public WordCountAndLen(){
        set(new IntWritable(), new IntWritable());
    }

    public WordCountAndLen(int count, int length){
        set(new IntWritable(count), new IntWritable(length));
    }

    public IntWritable getCount(){
        return count;
    }

    public IntWritable getLength() {
        return length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        count.write(out);
        length.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        count.readFields(in);
        length.readFields(in);
    }
}
