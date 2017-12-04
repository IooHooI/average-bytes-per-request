package com.bigdata.homework12.reducer;

import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AverageBytesPerRequestReducerTestUT {
    private ReduceDriver<IntWritable, RequestInfo, IntWritable, RequestInfo> reduceDriver;

    @Before
    public void before() {
        AverageBytesPerRequestReducer averageBytesPerRequestReducer = new AverageBytesPerRequestReducer();
        reduceDriver = ReduceDriver.newReduceDriver(averageBytesPerRequestReducer);
    }

    @Test
    public void test_Longest_Word_Reducer_Case_1() {
        List<RequestInfo> requests = new ArrayList<>();
        RequestInfo requestInfo;

        requestInfo = new RequestInfo();
        requestInfo.setRequestId("ip54325432");
        requestInfo.setBytes(1421L);
        requestInfo.setAverage(1421F);
        requests.add(requestInfo);

        requestInfo = new RequestInfo();
        requestInfo.setRequestId("ip54325432");
        requestInfo.setBytes(3453L);
        requestInfo.setAverage(3453F);
        requests.add(requestInfo);

        RequestInfo result = new RequestInfo();
        result.setRequestId("ip54325432");
        result.setBytes(1421L + 3453L);
        result.setAverage((1421L + 3453F)/2);

        reduceDriver
                .withInput(new IntWritable(54325432), requests)
                .withOutput(new IntWritable(54325432), result);
        reduceDriver.runTest();
    }
}