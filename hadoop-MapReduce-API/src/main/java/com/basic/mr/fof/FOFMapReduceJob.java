package com.basic.mr.fof;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 获取用户二度关系亲密度排名
 * hadoop jar original-hadoop-MapReduce-API-1.0-SNAPSHOT.jar com.basic.mr.FOFMapReduceJob /user/root/hadoop/input/wc /user/root/hadoop/output/
 */
public class FOFMapReduceJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("sleep", otherArgs[2]);

		Job job = Job.getInstance(conf);
		job.setJarByClass(FOFMapReduceJob.class);

		Path input = new Path(otherArgs[0]);
		FileInputFormat.addInputPath(job, input );

		Path output = new Path(otherArgs[1]);
		if(output.getFileSystem(conf).exists(output)){
			output.getFileSystem(conf).delete(output,true);
		}
		FileOutputFormat.setOutputPath(job, output );

		job.setMapperClass(FOFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FOFReducer.class);

		job.waitForCompletion(true);
	}

}
