package com.basic.mr.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TqPartitioner  extends  Partitioner<TQ, Text> {

	@Override
	public int getPartition(TQ key, Text value, int numPartitions) {
		return key.getYear() % numPartitions;
	}
	

}
