package com.siva.hadoop.training.wordcountpairwithoutkey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountPairJob implements Tool {

	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job wordCountJob = new Job(getConf());
		wordCountJob.setJobName("OrienIT Word Count");
		wordCountJob.setJarByClass(this.getClass());
		wordCountJob.setMapperClass(WordCountPairMapper.class);
		wordCountJob.setReducerClass(WordCountPairReducer.class);
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(LongWritable.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(LongWritable.class);
		// wordCountJob.setInputFormatClass(TextInputFormat.class);
		// wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return wordCountJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountPairJob(), args);
		/*
		 * String[] sedArgs = {args[0],args[2]}; Configuration conf1 = new
		 * Configuration(); conf1.set("sed-arg1", "Hyderabad");
		 * conf1.set("sed-arg2", "Bangalore"); ToolRunner.run(conf1, new
		 * SedJob(), sedArgs);
		 */
	}

}
