package com.siva.hadoop.training.xmlfiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XmlJob implements Tool {

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
		Job xmlJob = new Job(getConf());
		xmlJob.setJobName("OrienIT Grep Count");
		xmlJob.setJarByClass(this.getClass());

		xmlJob.setMapperClass(XmlMapper.class);
		xmlJob.setNumReduceTasks(0);

		xmlJob.setMapOutputKeyClass(Text.class);
		xmlJob.setMapOutputValueClass(NullWritable.class);

		xmlJob.setOutputKeyClass(Text.class);
		xmlJob.setOutputValueClass(NullWritable.class);

		xmlJob.setInputFormatClass(MyXmlInputFormat.class);
		xmlJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(xmlJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(xmlJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);

		return xmlJob.waitForCompletion(true) == true ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<a>");
		conf.set("xmlinput.end", "</a>");
		conf.set("grep-arg", "Test1");
		ToolRunner.run(conf, new XmlJob(), args);
	}

}

















