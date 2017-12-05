package com.bigdata.homework12.mapper;

import com.bigdata.homework12.AverageBytesPerRequestJob;
import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

public class TokenizerMapper extends Mapper<LongWritable, Text, IntWritable, RequestInfo> {
    private static Logger logger = Logger.getLogger(TokenizerMapper.class.getName());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.MOZILLA).increment(0);
        context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.OPERA).increment(0);
        context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.IE).increment(0);
        context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.OTHER).increment(0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split(" ");
        IntWritable key1;
        RequestInfo value1;
        try {
            value1 = new RequestInfo();
            key1 = new IntWritable(Integer.valueOf(lines[0].substring(2)));
            value1.setBytes(Long.valueOf(lines[9]));
            value1.setAverage(Float.valueOf(lines[9]));

            processCounters(context, value.toString());
            context.write(key1, value1);
        } catch (NumberFormatException e) {
            logger.severe("The whole string: " + value.toString());
            logger.severe("lines[0].substring(2): " + lines[0].substring(2));
            logger.severe("lines[0]: " + lines[0]);
            logger.severe("lines[9]: " + lines[9]);
            logger.severe("lines[11]: " + lines[11]);
        }
    }

    private void processCounters(Context context, String request) {
        if (request.contains("\"Mozilla")) {
            context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.MOZILLA).increment(1);
        } else if (request.contains("\"Opera")) {
            context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.OPERA).increment(1);
        } else if (request.contains("\"IE")) {
            context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.IE).increment(1);
        } else {
            context.getCounter(AverageBytesPerRequestJob.BrowsersCounter.OTHER).increment(1);
        }
    }
}