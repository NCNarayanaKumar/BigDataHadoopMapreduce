package com.siva.hadoop.training.job;

import java.util.StringTokenizer;

public class StringSplit {
	public static void main(String[] args) {
		String message = "i am learning hadoop course at orien it";
		String[] split = message.split(" ");
		for (String word : split) {
			System.out.println(word + ",1");
		}
		System.out.println("=======================================");
		StringTokenizer st = new StringTokenizer(message, " ");
		while (st.hasMoreTokens()) {
			String word = st.nextToken();
			System.out.println(word + ",1");

		}
	}
}
