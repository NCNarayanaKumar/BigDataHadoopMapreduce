package com.siva.hadoop.training.comparefiles;

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

public class CompareFilesJob implements Tool {

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
		Job compareJob = new Job(getConf()); // initailizing the job
												// configuration
		compareJob.setJobName("Compare files Job"); // setting the job name
		compareJob.setJarByClass(this.getClass()); // to call this as a jar

		compareJob.setMapperClass(CompareFilesMapper.class); // setting mapper
																// class
		compareJob.setReducerClass(CompareFilesReducer.class); // setting
																// reducer class

		compareJob.setMapOutputKeyClass(LongWritable.class); // setting mapper
																// output key
																// class: K2
		compareJob.setMapOutputValueClass(Text.class); // setting mapper output
														// value class: V2

		compareJob.setOutputKeyClass(Text.class); // setting reducer output key
													// class: K3
		compareJob.setOutputValueClass(Text.class); // setting reducer output
													// value class: V3

		compareJob.setInputFormatClass(TextInputFormat.class); // setting the
																// input format
																// class ,i.e
																// for K1, V1
		compareJob.setOutputFormatClass(TextOutputFormat.class); // setting the
																	// output
																	// format
																	// class

		FileInputFormat.addInputPaths(compareJob, args[0]); // setting the input
															// path
		FileOutputFormat.setOutputPath(compareJob, new Path(args[1])); // setting
																		// the
																		// output
																		// path

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true); // delete the
																	// output
																	// folder if
																	// exists

		return compareJob.waitForCompletion(true) ? 0 : -1; // to execute the
															// job

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new CompareFilesJob(), args);
	}

}
