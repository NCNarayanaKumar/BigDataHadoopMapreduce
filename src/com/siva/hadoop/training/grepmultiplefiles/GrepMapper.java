package com.siva.hadoop.training.grepmultiplefiles;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().contains(context.getConfiguration().get("grep-arg"))) {
			context.write(value, NullWritable.get());
		}
	};

}
