package com.siva.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Read the line
		String line = value.toString();

		// if line contains "hyderabad", then print it
		if (line.contains("hyderabad")) {
			context.write(value, NullWritable.get());
		}
		
	}
}















