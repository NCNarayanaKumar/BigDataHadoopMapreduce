package com.siva.hadoop.training.comparefiles;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CompareFilesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String string = value.toString();
		String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		String content = filename + "->" + string;
		context.write(key, new Text(content));
	};
}
