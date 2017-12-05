package com.bigdata.homework12.writable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RequestInfo implements Writable {

    private FloatWritable average;

    private LongWritable bytes;

    public RequestInfo() {
        this.average = new FloatWritable();
        this.bytes = new LongWritable();
        this.average.set(0);
        this.bytes.set(0);
    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes.set(bytes);
    }

    public FloatWritable getAverage() {
        return average;
    }

    public void setAverage(Float average) {
        this.average.set(average);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bytes.write(dataOutput);
        average.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bytes.readFields(dataInput);
        average.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.average, this.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RequestInfo)) {
            return false;
        } else {
            RequestInfo requestInfo = (RequestInfo)o;
            return
                    this.bytes.equals(requestInfo.getBytes()) &&
                    Math.abs(this.average.get() - requestInfo.getAverage().get()) < 0.00001;
        }
    }

    @Override
    public String toString() {
        return "" + average + " " + bytes;
    }
}