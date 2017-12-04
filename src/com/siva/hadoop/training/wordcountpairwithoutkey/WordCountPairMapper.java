package com.siva.hadoop.training.wordcountpairwithoutkey;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountPairMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		String pair1 = words[0];
		String pair, pair2;
		for (int i = 1; i < words.length; i++) {
			pair2 = words[i];
			pair = pair1 + " " + pair2;
			pair1 = pair2;
			context.write(new Text(pair), one);
		}
	};
}
