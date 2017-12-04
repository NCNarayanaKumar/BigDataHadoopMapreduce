package com.siva.hadoop.training.biagram;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BiaGramMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1.read the line
		String line = value.toString();

		// 2.split the line into words
		String[] words = line.split(" ");

		// 3.assign count(1) to two conscutive words
		for (int i = 0; i < words.length - 1; i++) {
			String word1 = words[i];
			String word2 = words[i + 1];
			String biaGram = word1 + " " + word2;
			context.write(new Text(biaGram), new LongWritable(1));
		}

	}
}
