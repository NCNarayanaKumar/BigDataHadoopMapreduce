package com.siva.hadoop.training.mongodb.job;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

public class SampleMR extends MongoTool {

	public SampleMR() throws UnknownHostException {
		setConf(new Configuration());

		MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
		MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);

		MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/test.in");
		MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/test.out");

		MongoConfigUtil.setMapper(getConf(), TokenizerMapper.class);
		MongoConfigUtil.setReducer(getConf(), IntSumReducer.class);

		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);

		MongoConfigUtil.setOutputKey(getConf(), Text.class);
		MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);
	}

	public static void main(final String[] pArgs) throws Exception {
		System.exit(ToolRunner.run(new SampleMR(), pArgs));
	}

	public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> implements
			org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {

			System.out.println("key: " + key);
			System.out.println("value: " + value);

			final StringTokenizer itr = new StringTokenizer(value.get("x").toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}

		@Override
		public void configure(JobConf paramJobConf) {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public void map(Object key, BSONWritable value, OutputCollector<Text, IntWritable> output, Reporter paramReporter)
				throws IOException {
			System.out.println("key: " + key);
			System.out.println("value: " + value);

			final StringTokenizer itr = new StringTokenizer(value.getDoc().get("x").toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, BSONWritable> implements
			org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, BSONWritable> {

		private final IntWritable result = new IntWritable();

		private BSONWritable reduceResult;

		public IntSumReducer() {
			super();
			reduceResult = new BSONWritable();
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			BasicBSONObject output = new BasicBSONObject();
			output.put("sum", sum);
			reduceResult.setDoc(output);
			context.write(key, reduceResult);

		}

		@Override
		public void configure(JobConf paramJobConf) {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, BSONWritable> output, Reporter paramReporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				IntWritable val = (IntWritable) values.next();
				sum += val.get();
			}
			result.set(sum);
			BasicBSONObject boutput = new BasicBSONObject();
			boutput.put("sum", sum);
			reduceResult.setDoc(boutput);
			output.collect(key, reduceResult);
		}
	}

}