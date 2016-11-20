package com.mapreduce.ajay;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values)
			sum++;
		output.write(key, new LongWritable(sum));
	}
}
