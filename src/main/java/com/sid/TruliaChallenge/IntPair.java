package com.sid.TruliaChallenge;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by srathi on 12/29/16.
 */
public class IntPair implements WritableComparable<IntPair> {

    private IntWritable first;
    private IntWritable second;

    public IntPair() {
        set(new IntWritable(), new IntWritable());
    }

    public IntPair(int first, int second) {
        set(new IntWritable(first), new IntWritable(second));
    }

    public IntPair(IntWritable first, IntWritable second) {
        set(first, second);
    }

    public void set(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

@Override
public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        }

@Override
public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
        }

@Override
public boolean equals(Object o) {
        if (o instanceof IntPair) {
            IntPair tp = (IntPair) o;
        return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
        }

@Override
public String toString() {
        return first + "\t" + second;
        }

@Override
public int compareTo(IntPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
        return cmp;
        }
        return second.compareTo(tp.second);
        }

}
