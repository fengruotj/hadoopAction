package com.basic.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//自定义分区方法，相同年月的为一个分区
public class TqGroupingComparator  extends WritableComparator {
	
	
	public TqGroupingComparator() {
		super(TQ.class,true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
			
			TQ t1 = (TQ)a;
			TQ t2 = (TQ)b;

			int c1=Integer.compare(t1.getYear(), t2.getYear());
			if(c1==0){
				return Integer.compare(t1.getMonth(), t2.getMonth());
			}			
			return c1;
	}
	
}
