package com.basic.mr.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * 获取每一个年月下 温度最高的前两天
 *
 * 提交任务 hadoop 提交任务
 * hadoop jar original-hadoop-MapReduce-API-1.0-SNAPSHOT.jar com.basic.mr.TQMapReduceJob /user/root/hadoop/input/wc /user/root/hadoop/output/
 */
public class TQMapReduceJob {
	public static void main(String[] args) throws Exception {

		//1,conf
		Configuration conf = new Configuration(true);
		
		//2,job
		Job job = Job.getInstance(conf);
		job.setJarByClass(TQMapReduceJob.class);
		
		//3,input,output
		
		Path input = new Path("/tq/input");
		FileInputFormat.addInputPath(job, input);
		
		Path output = new Path("/tq/output");
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output );

		//4,map
		job.setMapperClass(TqMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(Text.class);

		//5,reduce
		job.setReducerClass(TqReducer.class);
		job.setNumReduceTasks(2);
		
		//6,other:sort,part..,group...
		job.setPartitionerClass(TqPartitioner.class);
		job.setSortComparatorClass(TqSortComparator.class);
		job.setGroupingComparatorClass(TqGroupingComparator.class);
		
		job.setCombinerClass(TqReducer.class);
		job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);
		//7,submit
		
		job.waitForCompletion(true);
	}
	

}
