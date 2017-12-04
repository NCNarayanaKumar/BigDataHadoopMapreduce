package com.siva.hadoop.training.job;

public class HashCodeExample {
	public static void main(String[] args) {
		String x = "abc";
		String y = "abcd";
		String z = "abcde";
		String[] val = { "examples", "features", "separate" };

		for (int i = 0; i < val.length; i++) {
			System.out.println(val[i] + " : " + val[i].hashCode() % 26);
			System.out.println(val[i] + " : " + Math.abs(val[i].hashCode()) % 26);

		}
		System.out.println(x.hashCode() + " : " + y.hashCode() + " : " + z.hashCode());
	}
}
