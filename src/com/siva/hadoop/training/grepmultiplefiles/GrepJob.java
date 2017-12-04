package com.siva.hadoop.training.grepmultiplefiles;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GrepJob implements Tool {

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
		Job grepJob = new Job(getConf());
		grepJob.setJobName("OrienIT Grep Count");
		grepJob.setJarByClass(this.getClass());

		grepJob.setMapperClass(GrepMapper.class);
		grepJob.setNumReduceTasks(0);

		grepJob.setMapOutputKeyClass(Text.class);
		grepJob.setMapOutputValueClass(NullWritable.class);

		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(NullWritable.class);

		grepJob.setInputFormatClass(TextInputFormat.class);
		grepJob.setOutputFormatClass(TextOutputFormat.class);

		String multiplepaths = StringUtils.join(args, ",", 0, args.length - 1);

		FileInputFormat.setInputPaths(grepJob, multiplepaths);
		FileOutputFormat.setOutputPath(grepJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return grepJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		conf1.set("grep-arg", "Hyderabad");
		ToolRunner.run(conf1, new GrepJob(), args);
	}

}
