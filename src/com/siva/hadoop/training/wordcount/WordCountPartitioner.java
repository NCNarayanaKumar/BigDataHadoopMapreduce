package com.siva.hadoop.training.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		String word = key.toString().toLowerCase();
		if (word.length() == 0)
			return 0;
		int partition = Math.abs(word.charAt(0) - 'a') % noOfReducers;
		return partition;
	}
}
