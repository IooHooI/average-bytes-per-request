package com.bigdata.homework12;

import com.bigdata.homework12.mapper.TokenizerMapper;
import com.bigdata.homework12.reducer.AverageBytesPerRequestReducer;
import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MapReduceTestIT {
    private MapReduceDriver<LongWritable, Text, IntWritable, RequestInfo, IntWritable, RequestInfo> mapReduceDriver;

    @Before
    public void before() {
        TokenizerMapper tokenizerMapper = new TokenizerMapper();
        AverageBytesPerRequestReducer averageBytesPerRequestReducer = new AverageBytesPerRequestReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(tokenizerMapper, averageBytesPerRequestReducer);
    }

    @Test
    public void test_Map_Reduce_IT_Case_1() {
        RequestInfo requestInfo = new RequestInfo();
        requestInfo.setAverage(38340F);
        requestInfo.setBytes(38340L);
        mapReduceDriver
                .withInput(new LongWritable(1), new Text(
                        "ip126 - - [24/Apr/2011:11:38:43 -0400] \"GET /dec/multia/multia.gif HTTP/1.1\" 200 38340 \"http://host2/dec/multia/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1\"\n"))
                .withOutput(new IntWritable(126), requestInfo);
        mapReduceDriver.runTest();
    }
}
