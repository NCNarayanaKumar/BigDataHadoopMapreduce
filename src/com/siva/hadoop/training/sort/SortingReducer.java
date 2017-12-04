package com.siva.hadoop.training.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<Text, LongWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
		while (value.iterator().hasNext()) {
			context.write(key, NullWritable.get());
		}
	};
}
