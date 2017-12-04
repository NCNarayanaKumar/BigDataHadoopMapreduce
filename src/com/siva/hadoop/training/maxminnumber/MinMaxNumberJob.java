package com.siva.hadoop.training.maxminnumber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinMaxNumberJob implements Tool {

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
		Job minMaxNumberJob = new Job(getConf()); // initailizing the job
													// configuration
		minMaxNumberJob.setJobName("MinMaxNumber Job"); // setting the job name
		minMaxNumberJob.setJarByClass(this.getClass()); // to call this as a jar

		minMaxNumberJob.setMapperClass(MinMaxNumberMapper.class); // setting
																	// mapper
																	// class
		minMaxNumberJob.setReducerClass(MinMaxNumberReducer.class); // setting
																	// reducer
																	// class

		minMaxNumberJob.setMapOutputKeyClass(Text.class); // setting mapper
															// output key class:
															// K2
		minMaxNumberJob.setMapOutputValueClass(Text.class); // setting mapper
															// output value
															// class: V2

		minMaxNumberJob.setOutputKeyClass(Text.class); // setting reducer output
														// key class: K3
		minMaxNumberJob.setOutputValueClass(Text.class); // setting reducer
															// output value
															// class: V3

		minMaxNumberJob.setInputFormatClass(TextInputFormat.class); // setting
																	// the input
																	// format
																	// class
																	// ,i.e for
																	// K1, V1
		minMaxNumberJob.setOutputFormatClass(TextOutputFormat.class); // setting
																		// the
																		// output
																		// format
																		// class

		FileInputFormat.addInputPath(minMaxNumberJob, new Path(args[0])); // setting
																			// the
																			// input
																			// path
		FileOutputFormat.setOutputPath(minMaxNumberJob, new Path(args[1])); // setting
																			// the
																			// output
																			// path

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true); // delete the
																	// output
																	// folder if
																	// exists

		return minMaxNumberJob.waitForCompletion(true) ? 0 : -1; // to execute
																	// the job

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("no.of.columns", args[2]);
		ToolRunner.run(conf, new MinMaxNumberJob(), args);
	}

}
