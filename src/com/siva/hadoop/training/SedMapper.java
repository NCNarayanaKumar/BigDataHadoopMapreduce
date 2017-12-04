package com.siva.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Read the line
		String line = value.toString();

		Configuration conf = context.getConfiguration();
		String input = conf.get("sed.input");
		String output = conf.get("sed.output");

		// if line contains "hyderabad", then replace with "banglore"
		if (line.contains(input)) {
			String replaceAll = line.replaceAll(input, output);
			context.write(new Text(replaceAll), NullWritable.get());
		} else {
			context.write(value, NullWritable.get());
		}

	}
}






