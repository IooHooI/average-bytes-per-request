package com.bigdata.homework12.writable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RequestInfo implements Writable {

    private FloatWritable average;

    private LongWritable bytes;

    private Text requestId;

    private Text userAgent;

    public RequestInfo() {
        this.average = new FloatWritable();
        this.bytes = new LongWritable();
        this.userAgent = new Text();
        this.requestId = new Text();
        this.average.set(0);
        this.bytes.set(0);
    }

    public Text getRequestId() {
        return requestId;
    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setRequestId(String requestId) {
        this.requestId.set(requestId);
    }

    public void setBytes(Long bytes) {
        this.bytes.set(bytes);
    }

    public Text getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent.set(userAgent);
    }

    public FloatWritable getAverage() {
        return average;
    }

    public void setAverage(Float average) {
        this.average.set(average);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        requestId.write(dataOutput);
        userAgent.write(dataOutput);
        bytes.write(dataOutput);
        average.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        requestId.readFields(dataInput);
        userAgent.readFields(dataInput);
        bytes.readFields(dataInput);
        average.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        // This is used by HashPartitioner, so implement it as per need
        // this one shall hash based on request id
        return requestId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RequestInfo)) {
            return false;
        } else {
            RequestInfo requestInfo = (RequestInfo)o;
            return
                    this.requestId.equals(requestInfo.getRequestId()) &&
                    this.bytes.equals(requestInfo.getBytes()) &&
                    this.userAgent.equals(requestInfo.getUserAgent()) &&
                    Math.abs(this.average.get() - requestInfo.getAverage().get()) < 0.00001;
        }
    }
}
