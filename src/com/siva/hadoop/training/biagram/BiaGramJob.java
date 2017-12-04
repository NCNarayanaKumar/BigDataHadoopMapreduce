package com.siva.hadoop.training.biagram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BiaGramJob implements Tool {
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
		Job biaGramJob = new Job(getConf());

		// setting the job name
		biaGramJob.setJobName("Orien IT BiaGram Job");

		// to call this as a jar
		biaGramJob.setJarByClass(this.getClass());

		// setting custom mapper class
		biaGramJob.setMapperClass(BiaGramMapper.class);

		// setting custom reducer class
		biaGramJob.setReducerClass(BiaGramReducer.class);

		// setting custom combiner class
		// biaGramJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// wordCountJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// biaGramJob.setPartitionerClass(WordCountPartitioner.class);

		// setting custom Sort Comparator
		biaGramJob.setSortComparatorClass(BiaGramComparator.class);
		
		biaGramJob.setGroupingComparatorClass(BiaGramComparator.class);

		// setting mapper output key class: K2
		biaGramJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		biaGramJob.setMapOutputValueClass(LongWritable.class);

		// setting reducer output key class: K3
		biaGramJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		biaGramJob.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		biaGramJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		biaGramJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(biaGramJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(biaGramJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return biaGramJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new BiaGramJob(), args);
		System.out.println("My Status: " + status);
	}

}
