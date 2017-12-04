package com.siva.hadoop.training.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadCompression {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://localhost:9000/demo/demooutput-bz/part-r-00000.bz2";
		Configuration conf = new Configuration();
		conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZipCodec");
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);

			in.seek(0); // go back to the start of the file
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
