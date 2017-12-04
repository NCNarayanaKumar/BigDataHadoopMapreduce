package com.siva.hadoop.training.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

public class MultipleFilesWriter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Writer writer = null;
		Path inputDir = new Path(args[0]);
		Path seqFile = new Path(args[1]);

		int BUFFER_SIZE = 10 * 1024 * 1024;

		try {
			writer = SequenceFile.createWriter(FileSystem.get(conf), conf, seqFile, Text.class, BytesWritable.class, CompressionType.BLOCK);
			FileSystem fs = inputDir.getFileSystem(conf);
			BytesWritable content = new BytesWritable();
			Text fileName = new Text();
			byte[] data = new byte[BUFFER_SIZE];
			for (FileStatus file : fs.listStatus(inputDir)) {
				Path path = file.getPath();
				String name = path.getName();

				if (file.getLen() > Integer.MAX_VALUE) {
					throw new Exception("A file is too large. " + name);
				}

				FSDataInputStream stream = fs.open(path);
				String fileExtension = "";
				int lastIndexOf = name.lastIndexOf(".");
				if (lastIndexOf != -1)
					fileExtension = name.substring(lastIndexOf);

				System.out.println(fileExtension);

				if (fileExtension.equals(".txt") || fileExtension.equals("")) {
					stream.read(data);
				} else if (fileExtension.equals(".doc")) {
					POIFSFileSystem pfs = new POIFSFileSystem(stream);
					HWPFDocument doc = new HWPFDocument(pfs);
					WordExtractor we = new WordExtractor(doc);
					data = we.getText().getBytes();
				} else if (fileExtension.equals(".pdf")) {
					PDDocument pddDocument = PDDocument.load(stream);
					PDFTextStripper textStripper = new PDFTextStripper();
					data = textStripper.getText(pddDocument).getBytes();
					pddDocument.close();
				}

				content.set(data, 0, data.length);
				fileName.set(name);
				writer.append(fileName, content);
				stream.close();
			}
			writer.close();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MultipleFilesWriter(), args);
	}

}
