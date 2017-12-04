package com.siva.hadoop.training.phoneretrieval;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PhoneRetrievalMapper extends Mapper<Text, Text, Text, Text> {

	private FSDataInputStream stream;

	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		FileSystem fileSystem = FileSystem.get(cacheFiles[0], conf);
		stream = fileSystem.open(new Path(cacheFiles[0]));
	};

	@Override
	protected void map(Text key, Text value, Context context) throws java.io.IOException, InterruptedException {
		stream.seek(0);

		String line;
		while ((line = stream.readLine()) != null) {
			// input1 is file, input2 is dcfile
			if (key.toString().contains(line)) {
				context.write(key, value);
			}
		}
	};

	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		stream.close();
	};
}










