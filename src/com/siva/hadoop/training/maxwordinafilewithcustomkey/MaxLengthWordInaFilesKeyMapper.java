package com.siva.hadoop.training.maxwordinafilewithcustomkey;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MaxLengthWordInaFilesKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
	String maxWord;
	String fileName;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		maxWord = "";
		InputSplit inputSplit = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) inputSplit;
		Path path = fileSplit.getPath();
		String name = path.getName();
		Path parent = path.getParent();
		fileName = path.toString();
		System.out.println("path: " + path);
		System.out.println("name: " + name);
		System.out.println("parent: " + parent);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] words = line.split(" ");
		// finding the max word in a line
		for (String word : words) {
			if (word.length() >= maxWord.length()) {
				maxWord = word;
			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(new Text(fileName), new Text(maxWord));

	}

}
