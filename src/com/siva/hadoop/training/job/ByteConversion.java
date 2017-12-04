package com.siva.hadoop.training.job;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteConversion {
	public static void main(String[] args) {
		System.out.println(Bytes.toBytes(12).toString());
	}
}
