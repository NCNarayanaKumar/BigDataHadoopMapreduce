package com.siva.hadoop.training.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class URICountReducer extends Reducer<URICountKey, IntWritable, Text, IntWritable> {

	private final Text URI = new Text();

	@Override
	protected void reduce(URICountKey uri, java.lang.Iterable<IntWritable> values, Context context) throws java.io.IOException,
			InterruptedException {
		URI.set(uri.getUri());
		for (IntWritable value : values) {
			context.write(URI, value);
		}
	};

}
