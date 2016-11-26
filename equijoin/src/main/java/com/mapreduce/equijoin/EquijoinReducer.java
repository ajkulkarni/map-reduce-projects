package com.mapreduce.equijoin;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EquijoinReducer extends Reducer<FloatWritable, Text, Text, NullWritable> {
	public void reduce(FloatWritable key, Iterable<Text> values, Context output)
			throws IOException, InterruptedException {

		LinkedList<Text> table1 = new LinkedList<Text>();
		LinkedList<Text> table2 = new LinkedList<Text>();
		String tableId = "";

		for (Text value : values) {
			String tableName = value.toString().split(",")[0].trim().toLowerCase();
			if (tableId.equals(""))
				tableId += tableName;
			if (tableId.equals(tableName))
				table1.add(new Text(value));
			else
				table2.add(new Text(value));
		}

		if (table1.size() > 0 && table2.size() > 0) {
			for (Text table1Row : table1) {
				for (Text table2Row : table2) {
					Text row = new Text();
					row.append(table1Row.getBytes(), 0, table1Row.getLength());
					row.append(new Text(", ").getBytes(), 0, 2);
					row.append(table2Row.getBytes(), 0, table2Row.getLength());
					output.write(row, null);
				}
			}

		}
	}
}
