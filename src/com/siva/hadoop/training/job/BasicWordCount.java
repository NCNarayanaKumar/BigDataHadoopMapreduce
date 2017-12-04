package com.siva.hadoop.training.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class BasicWordCount {
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		System.out.println("Start time: " + start);
		File file = new File("/home/hadoop/kalyan_largefile");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line;
		Map<String, Integer> map = new TreeMap<String, Integer>();
		while ((line = reader.readLine()) != null) {
			// System.out.println(line);
			String[] words = line.split(" ");
			for (String word : words) {
				Integer ival = map.get(word);
				if (ival != null) {
					int mycount = ival + 1;
					map.put(word, mycount);
				} else {
					map.put(word, 1);
				}
			}
		}
		Set<Entry<String, Integer>> entrySet = map.entrySet();
		for (Entry<String, Integer> entry : entrySet) {
			System.out.println("<word:count> " + entry.getKey() + " : " + entry.getValue());
		}
		reader.close();
		long end = System.currentTimeMillis();
		System.out.println("time diff : " + (end - start));
	}
}
