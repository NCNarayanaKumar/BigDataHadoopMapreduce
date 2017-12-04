package com.siva.hadoop.training.biagram;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class BiaGramComparator implements RawComparator<Text> {

	@Override
	public int compare(Text key1, Text key2) {
		String biaGram1 = key1.toString();
		String[] words1 = biaGram1.split(" ");

		String biaGram2 = key2.toString();
		String[] words2 = biaGram2.split(" ");

		int diff = words1[0].compareTo(words2[0]);
		if (diff == 0) {
			diff = -words1[1].compareTo(words2[1]);
		}

		return diff;
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
	}

}
