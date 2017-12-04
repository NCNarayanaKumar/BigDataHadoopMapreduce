package com.siva.hadoop.training.job;

import java.util.LinkedHashSet;
import java.util.Set;

public class RemoveDuplicates {
	public static void main(String[] args) {
		String[] files = { "myfile1.txt", "file1.txt", "file2.txt", "file3.txt", "file4.txt", "file1.txt", "myfile1.txt" };
		String allfiles = removeDuplicates(files);

		System.out.println(allfiles.substring(1));
	}

	private static String removeDuplicates(String[] files) {
		Set<String> files1 = new LinkedHashSet<String>();
		for (String file : files) {
			files1.add(file);
		}
		String allfiles = "";
		for (String file : files1) {
			allfiles = allfiles + "," + file;
		}
		return allfiles;
	}
}
