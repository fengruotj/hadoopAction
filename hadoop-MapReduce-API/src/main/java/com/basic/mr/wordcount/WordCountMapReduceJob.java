package com.basic.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;

/**
 * locate com.basic.mr
 * Created by MasterTj on 2019/1/23.
 * 提交任务 hadoop 提交任务
 * hadoop jar original-hadoop-MapReduce-API-1.0-SNAPSHOT.jar com.basic.mr.wordcount.WordCountMapReduceJob /user/root/hadoop/input/wc /user/root/hadoop/output/
 */
public class WordCountMapReduceJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountMapReduceJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outpath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outpath)){
            fileSystem.delete(outpath,true);
        }

        FileOutputFormat.setOutputPath(job, outpath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
