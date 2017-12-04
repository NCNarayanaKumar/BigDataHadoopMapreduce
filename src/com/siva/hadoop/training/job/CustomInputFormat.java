package com.siva.hadoop.training.job;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.siva.hadoop.training.wordcountperfile.WordCountPerFileKey;

public class CustomInputFormat implements InputFormat<WordCountPerFileKey, Text> {

	@Override
	public RecordReader<WordCountPerFileKey, Text> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	boolean isSplitable() {
		return false;
	}
}
