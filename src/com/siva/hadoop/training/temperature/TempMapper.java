package com.siva.hadoop.training.temperature;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String date = line.substring(6, 14);
		
		float maxTemp = Float.parseFloat(line.substring(38, 42).trim());
		float minTemp = Float.parseFloat(line.substring(45, 48).trim());

		// Configuration conf = context.getConfiguration();
		// String input = conf.get("temp.input");

		/*
		if (maxTemp > 40) {
			context.write(new Text("Hot Day: " + date), new FloatWritable(maxTemp));
		} else if (minTemp < 10) {
			context.write(new Text("Cool Day: " + date), new FloatWritable(minTemp));
		}
		*/
		context.write(new Text(date), new FloatWritable(maxTemp));
	}

}
