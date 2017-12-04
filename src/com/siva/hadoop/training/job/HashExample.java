package com.siva.hadoop.training.job;

public class HashExample {
	public static void main(String[] args) {
		String name = "kalyan";
		for (int i = 0; i < 10; i++) {
			String myname = name + i;
			System.out.println("Name: " + myname + " : hash value: " + myname.hashCode() % 4);
		}
	}
}
