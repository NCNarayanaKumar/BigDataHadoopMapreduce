package com.siva.hadoop.training.sedkeyvalue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends Mapper<Text, Text, Text, Text> {

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String input = conf.get("sed-arg1");
		String output = conf.get("sed-arg2");

		if (key.toString().contains(input)) {
			context.write(new Text(key.toString().replaceAll(input, output)), value);
		} else {
			context.write(key, value);
		}

	}
}
