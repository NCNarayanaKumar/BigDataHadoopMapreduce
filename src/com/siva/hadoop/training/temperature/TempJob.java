package com.siva.hadoop.training.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.io.LongWritable;

public class TempJob implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job tempJob = new Job(getConf());

		// setting the job name
		tempJob.setJobName("Orien IT Temp Job");

		// to call this as a jar
		tempJob.setJarByClass(this.getClass());

		// setting custom mapper class
		tempJob.setMapperClass(TempMapper.class);

		// setting custom reducer class
		tempJob.setReducerClass(TempReducer.class);

		// setting custom combiner class
		// wordCountJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// tempJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// wordCountJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		tempJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		tempJob.setMapOutputValueClass(FloatWritable.class);

		// setting reducer output key class: K3
		tempJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		tempJob.setOutputValueClass(FloatWritable.class);

		// setting the input format class ,i.e for K1, V1
		tempJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		tempJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(tempJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(tempJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return tempJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations

		// Configuration conf = new Configuration();
		// conf.set("temp.input", args[2]);

		int status = ToolRunner.run(new Configuration(), new TempJob(), args);
		System.out.println("My Status: " + status);
	}

}