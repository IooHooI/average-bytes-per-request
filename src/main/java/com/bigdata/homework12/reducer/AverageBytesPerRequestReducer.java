package com.bigdata.homework12.reducer;

import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageBytesPerRequestReducer extends Reducer<IntWritable, RequestInfo, IntWritable, RequestInfo> {
    private int size = 0;

    protected void reduce(IntWritable key, Iterable<RequestInfo> value, Context context) throws IOException, InterruptedException {

        RequestInfo requestInfo = new RequestInfo();
        long bytes = 0;
        for (RequestInfo request: value) {
            bytes += request.getBytes().get();
            size += 1;
        }
        requestInfo.setBytes(bytes);
        requestInfo.setAverage((float)bytes / size);
        context.write(key, requestInfo);
    }
}
