package com.basic.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * locate com.basic.mr
 * Created by MasterTj on 2019/1/23.
 */
public class TokenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Logger logger= LoggerFactory.getLogger(TokenizerMapper.class);
    private int pos=0;
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        pos++;
        logger.info("pos");
        logger.info("key: "+ key);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
