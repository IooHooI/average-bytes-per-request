package com.bigdata.homework12.reducer;

import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageBytesPerRequestReducer extends Reducer<IntWritable, RequestInfo, IntWritable, RequestInfo> {
    protected void reduce(IntWritable key, Iterable<RequestInfo> value, Context context) throws IOException, InterruptedException {
        RequestInfo requestInfo = new RequestInfo();
        int size = 0;
        for (RequestInfo request: value) {
            size++;
            requestInfo.setRequestId(request.getRequestId().toString());
            requestInfo.setBytes(requestInfo.getBytes().get() + request.getBytes().get());
            requestInfo.setAverage(requestInfo.getAverage().get() + request.getBytes().get());
        }
        requestInfo.setAverage(requestInfo.getAverage().get() / size);
        context.write(key, requestInfo);
    }
}
