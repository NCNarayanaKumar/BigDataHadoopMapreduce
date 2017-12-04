package com.siva.hadoop.training.job;

import org.apache.commons.codec.digest.DigestUtils;

public class MD5Check {
	public static void main(String[] args) {
		String md5Hex = null;
		for (int i = 0; i < 10; i++) {
			md5Hex = DigestUtils.md5Hex(i + "word");
			System.out.println(md5Hex);
		}
		// System.out.println(md5Hex);

	}
}
