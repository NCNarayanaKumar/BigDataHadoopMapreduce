package com.siva.hadoop.training.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ImageProcessing {
	public static void main(String[] args) throws Exception {
		// String uri = args[0];
		String uri = "hdfs://localhost:8020/demo/phoenix.png";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		FSDataOutputStream out = fs.create(new Path("hdfs://localhost:8020/demo/phoenix1.png"));
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);

			in.seek(0); // go back to the start of the file
			IOUtils.copyBytes(in, out, 4096, false);

		} finally {
			IOUtils.closeStream(in);
		}
	}
}
