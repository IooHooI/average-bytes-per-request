package com.bigdata.homework12;

import com.bigdata.homework12.mapper.TokenizerMapper;
import com.bigdata.homework12.reducer.AverageBytesPerRequestReducer;
import com.bigdata.homework12.writable.RequestInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageBytesPerRequestJob {

    public enum BrowsersCounter {
        MOZILLA,
        OPERA,
        IE,
        OTHER
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: AverageBytesPerRequest <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Average Bytes Per Request");

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setJarByClass(AverageBytesPerRequestJob.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AverageBytesPerRequestReducer.class);
        job.setReducerClass(AverageBytesPerRequestReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RequestInfo.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(RequestInfo.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        Path outputPath = new Path(remainingArgs[1]);
        outputPath.getFileSystem(conf).delete(new Path(remainingArgs[1]),true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
