package com.mapreduce.equijoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Equijoin extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new Equijoin(), args));
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());

		job.setMapperClass(EquijoinMapper.class);
		job.setReducerClass(EquijoinReducer.class);

		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setJarByClass(Equijoin.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
