package com.bigdata.homework12.mapper;

import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, RequestInfo> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split(" ");

        IntWritable key1  = new IntWritable(Integer.valueOf(lines[0].substring(2)));
        RequestInfo value1 = new RequestInfo();

        value1.setRequestId(lines[0]);
        value1.setBytes(Long.valueOf(lines[9]));
        value1.setAverage(Float.valueOf(lines[9]));
        value1.setUserAgent(lines[11]);

        context.write(key1, value1);
    }
}