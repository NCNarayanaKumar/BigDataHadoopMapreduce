package com.siva.hadoop.training.phoneretrieval;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PhoneRetrievalJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job phoneJob = new Job(getConf());
		phoneJob.setJobName("OrienIT Phone JOb");
		phoneJob.setJarByClass(this.getClass());

		phoneJob.setNumReduceTasks(0);

		phoneJob.setMapperClass(PhoneRetrievalMapper.class);
		phoneJob.setOutputKeyClass(Text.class);
		phoneJob.setOutputValueClass(Text.class);

		phoneJob.setInputFormatClass(KeyValueTextInputFormat.class);

		Path lookupPath = new Path(args[1]);
		URI[] uris = new URI[1];
		uris[0] = lookupPath.toUri();

		DistributedCache.setCacheFiles(uris, phoneJob.getConfiguration());

		FileInputFormat.setInputPaths(phoneJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(phoneJob, new Path(args[2]));

		Path outputpath = new Path(args[2]);
		outputpath.getFileSystem(getConf()).delete(outputpath, true);

		return phoneJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PhoneRetrievalJob(), args);
	}

}
