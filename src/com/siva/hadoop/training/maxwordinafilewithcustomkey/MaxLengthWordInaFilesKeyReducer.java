package com.siva.hadoop.training.maxwordinafilewithcustomkey;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxLengthWordInaFilesKeyReducer extends Reducer<Text, Text, MaxLengthWordInaFilesKey, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String maxWord = "";
		for (Text value : values) {
			String word = value.toString();
			if (word.length() >= maxWord.length()) {
				maxWord = word;
			}
		}

		MaxLengthWordInaFilesKey bk = new MaxLengthWordInaFilesKey();
		bk.setFileName(key.toString());
		bk.setWord(maxWord);
		bk.setLength(maxWord.length());
		context.write(bk, NullWritable.get());
	}

}
