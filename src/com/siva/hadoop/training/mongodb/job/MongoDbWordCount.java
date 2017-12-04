package com.siva.hadoop.training.mongodb.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x :
 * "eliot is here" } ) db.in.insert( { x : "who is here" } ) =
 */
public class MongoDbWordCount extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(MongoDbWordCount.class);

	public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

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
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private final IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		final Configuration conf = getConf();
		MongoConfigUtil.setInputURI(conf, "mongodb://localhost/test.in");
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/test.out");

		System.out.println("Conf: " + conf);

		final Job job = new Job(conf, "word count");

		job.setJarByClass(MongoDbWordCount.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setCombinerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MongoDbWordCount(), args);
	}
}
