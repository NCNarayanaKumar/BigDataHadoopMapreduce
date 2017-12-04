package com.siva.hadoop.training.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orienit.kalyan.hadoop.training.wordcount.WordCountMapper;
import com.orienit.kalyan.hadoop.training.wordcount.WordCountReducer;

public class TestWordCount {

	private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mrDriver;

	@Before
	public void setup() {
		WordCountMapper wpcm = new WordCountMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(wpcm);

		WordCountReducer wpcr = new WordCountReducer();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(wpcr);

		mrDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
		mrDriver.setMapper(wpcm);
		mrDriver.setReducer(wpcr);
	}

	@Test
	public void testMapper() throws IOException {
		LongWritable key = new LongWritable(0);
		Text val = new Text("I am going");
		mapDriver.withInput(key, val);

		List<Pair<Text, LongWritable>> output = mapDriver.run();
		
		Pair<Text, LongWritable> pair = output.get(0);
		Assert.assertEquals(pair.getFirst().toString(), "I");
		Assert.assertEquals(pair.getSecond().get(), 1l);
		
		Pair<Text, LongWritable> pair1 = output.get(1);
		Assert.assertEquals(pair1.getFirst().toString(), "am");
		Assert.assertEquals(pair1.getSecond().get(), 1l);
		
		Pair<Text, LongWritable> pair2 = output.get(2);
		Assert.assertEquals(pair2.getFirst().toString(), "going");
		Assert.assertEquals(pair2.getSecond().get(), 1l);
	}

	
	@Test
	public void testReducer() throws IOException {
		Text key = new Text("I");
		
		List<LongWritable> list = new ArrayList<LongWritable>();
		list.add(new LongWritable(1));
		list.add(new LongWritable(1));
		list.add(new LongWritable(1));
		list.add(new LongWritable(1));
		list.add(new LongWritable(1));
		list.add(new LongWritable(1));
		reduceDriver.withInputKey(key);
		reduceDriver.withInputValues(list);
		
		List<Pair<Text, LongWritable>> output = reduceDriver.run();
		Pair<Text, LongWritable> pair = output.get(0);
		Text actualKey = new Text("I");
		Assert.assertEquals(pair.getFirst().toString(), actualKey.toString());
		Assert.assertEquals(pair.getSecond().get(), 6l);
	}

	@Test
	public void testMR() throws IOException {
		LongWritable key1 = new LongWritable(0);
		Text val1 = new Text("I am going");
		
		LongWritable key2 = new LongWritable(11);
		Text val2 = new Text("to hyd");
		
		LongWritable key3 = new LongWritable(18);
		Text val3 = new Text("I am learning");
		
		LongWritable key4 = new LongWritable(29);
		Text val4 = new Text("hadoop course");
		
		mrDriver.addInput(key1, val1);
		mrDriver.addInput(key2, val2);
		mrDriver.addInput(key3, val3);
		mrDriver.addInput(key4, val4);
		
		List<Pair<Text, LongWritable>> output = mrDriver.run();
		
		Pair<Text, LongWritable> pair = output.get(0);
		Text actualKey = new Text("I");
		Assert.assertEquals(pair.getFirst().toString(), actualKey.toString());
		Assert.assertEquals(pair.getSecond().get(), 2l);
		
		Pair<Text, LongWritable> pair1 = output.get(1);
		Text actualKey1 = new Text("am");
		Assert.assertEquals(pair1.getFirst().toString(), actualKey1.toString());
		Assert.assertEquals(pair1.getSecond().get(), 2l);
		
		Pair<Text, LongWritable> pair2 = output.get(2);
		Text actualKey3 = new Text("course");
		Assert.assertEquals(pair2.getFirst().toString(), actualKey3.toString());
		Assert.assertEquals(pair2.getSecond().get(), 1l);
	}

}



