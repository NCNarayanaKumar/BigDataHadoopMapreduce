package com.siva.hadoop.training.wordcountperfileusingsetup;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountPerFileUsingSetupMapper extends Mapper<LongWritable, Text, WordCountPerFileKey, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private String fileName;

	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	};

	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
		String content = value.toString();
		StringTokenizer strTock = new StringTokenizer(content, " ");
		while (strTock.hasMoreTokens()) {
			WordCountPerFileKey wcKey = new WordCountPerFileKey();
			wcKey.setFileName(fileName);
			wcKey.setWord(strTock.nextToken());
			context.write(wcKey, one);
		}
	};

	protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, WordCountPerFileKey, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
	};

}
