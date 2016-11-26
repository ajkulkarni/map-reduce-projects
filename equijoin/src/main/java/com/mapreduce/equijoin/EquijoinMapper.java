package com.mapreduce.equijoin;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EquijoinMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
	public void map(LongWritable offset, Text value, Context output) throws IOException, InterruptedException {
		String line = value.toString();
		FloatWritable key = new FloatWritable(Float.parseFloat(line.split(",")[1].trim()));
		output.write(key, value);
	}
}
