package com.siva.hadoop.training.xmlfiles;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("===========================");
		System.out.println("key: " + key);
		System.out.println("value: " + value);
		System.out.println("===========================");
		
		String line = value.toString();
		String grep = context.getConfiguration().get("grep-arg");
		if (grep == null) {
			context.write(value, NullWritable.get());
		} else if (line.contains(grep)) {
			context.write(value, NullWritable.get());
		}
	};

}








