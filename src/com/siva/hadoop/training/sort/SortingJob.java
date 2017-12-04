package com.siva.hadoop.training.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortingJob implements Tool {

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
		Job wordCountJob = new Job(getConf()); // initailizing the job
												// configuration
		wordCountJob.setJobName("Sorting Job"); // setting the job name
		wordCountJob.setJarByClass(this.getClass()); // to call this as a jar

		wordCountJob.setMapperClass(SortingMapper.class); // setting mapper
															// class
		wordCountJob.setReducerClass(SortingReducer.class); // setting reducer
															// class

		wordCountJob.setMapOutputKeyClass(Text.class); // setting mapper output
														// key class: K2
		wordCountJob.setMapOutputValueClass(LongWritable.class); // setting
																	// mapper
																	// output
																	// value
																	// class: V2

		wordCountJob.setOutputKeyClass(Text.class); // setting reducer output
													// key class: K3
		wordCountJob.setOutputValueClass(NullWritable.class); // setting reducer
																// output value
																// class: V3

		wordCountJob.setInputFormatClass(TextInputFormat.class); // setting the
																	// input
																	// format
																	// class
																	// ,i.e for
																	// K1, V1
		wordCountJob.setOutputFormatClass(TextOutputFormat.class); // setting
																	// the
																	// output
																	// format
																	// class

		FileInputFormat.addInputPath(wordCountJob, new Path(args[0])); // setting
																		// the
																		// input
																		// path
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1])); // setting
																			// the
																			// output
																			// path

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true); // delete the
																	// output
																	// folder if
																	// exists

		return wordCountJob.waitForCompletion(true) ? 0 : -1; // to execute the
																// job

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SortingJob(), args);
	}

}
