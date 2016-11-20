package com.mapreduce.ajay;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable offset, Text record, Context output) throws IOException, InterruptedException {
		output.write(new Text("count"), new LongWritable(1));
	}
}
