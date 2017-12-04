package com.siva.hadoop.training.job;

public class PartionExample {
	public static void main(String[] args) {
		String word = "@alyan".toLowerCase();
		char firstChar = word.charAt(0);
		int diff = Math.abs(firstChar - 'a') % 26;
		System.out.println(diff);
	}
}
