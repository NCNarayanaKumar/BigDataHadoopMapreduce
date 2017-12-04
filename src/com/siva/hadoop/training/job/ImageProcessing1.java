package com.siva.hadoop.training.job;

import java.awt.image.BufferedImage;
import java.net.URI;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ImageProcessing1 {
	public static void main(String[] args) throws Exception {
		// String uri = args[0];
		String uri = "hdfs://localhost:8020/demo/images/world.jpg";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		FSDataOutputStream out = fs.create(new Path("hdfs://localhost:8020/demo/tmp/images/world.jpg"));
		try {
			in = fs.open(new Path(uri));
			BufferedImage read = ImageIO.read(in);

			in.seek(0); // go back to the start of the file
			ImageIO.write(read, "JPG", out);

		} finally {
			IOUtils.closeStream(in);
		}
	}
}
