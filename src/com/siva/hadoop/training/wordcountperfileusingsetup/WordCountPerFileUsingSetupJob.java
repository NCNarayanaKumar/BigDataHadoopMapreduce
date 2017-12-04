package com.siva.hadoop.training.wordcountperfileusingsetup;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountPerFileUsingSetupJob implements Tool {

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Job wordCountPerFileJob = new Job(getConf());
		wordCountPerFileJob.setJobName("OrienIT Word Count Per File");
		wordCountPerFileJob.setJarByClass(this.getClass());

		wordCountPerFileJob.setMapperClass(WordCountPerFileUsingSetupMapper.class);
		wordCountPerFileJob.setMapOutputKeyClass(WordCountPerFileKey.class);
		wordCountPerFileJob.setMapOutputValueClass(IntWritable.class);

		wordCountPerFileJob.setReducerClass(WordCountPerFileUsingSetupReducer.class);
		wordCountPerFileJob.setOutputKeyClass(WordCountPerFileKey.class);
		wordCountPerFileJob.setOutputValueClass(IntWritable.class);
		wordCountPerFileJob.setInputFormatClass(TextInputFormat.class);

		File file = new File(args[0]);
		String inpath = "";
		if (file.isDirectory()) {
			Collection<File> list = FileUtils.listFiles(file, null, true);
			inpath = StringUtils.join(list, ",");
		}
		FileInputFormat.setInputPaths(wordCountPerFileJob, inpath);
		FileOutputFormat.setOutputPath(wordCountPerFileJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return wordCountPerFileJob.waitForCompletion(true) == true ? 0 : -1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountPerFileUsingSetupJob(), args);
	}

}
