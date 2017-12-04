package com.siva.hadoop.training.maxwordinafilewithcustomkey;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxLengthWordInaFilesKey1Reducer extends Reducer<Text, Text, MaxLengthWordInaFilesKey, NullWritable> {
	String maxWord;
	String fileName;

	protected void setup(Context context) throws IOException, InterruptedException {
		maxWord = "";
		fileName = "";
	};

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		for (Text value : values) {
			String word = value.toString();
			if (word.length() >= maxWord.length()) {
				maxWord = word;
				fileName = key.toString();
			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		MaxLengthWordInaFilesKey bk = new MaxLengthWordInaFilesKey();
		bk.setFileName(fileName);
		bk.setWord(maxWord);
		bk.setLength(maxWord.length());
		context.write(bk, NullWritable.get());
	}
}
