package com.siva.hadoop.training.job;

public class Delimeter {
	public static void main(String[] args) {
		String line = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245";
		String[] fields = line.split(" ");
		for (int i = 0; i < fields.length; i++) {
			System.out.println(fields[i]);
		}
		System.out.println("no of fields : " + fields.length);
	}
}
