package com.siva.hadoop.training.hdfs;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;

public class HadoopCommands {
	public static void main(String[] argv) throws Exception {
		FsShell shell = new FsShell();
		int res;
		try {
			res = ToolRunner.run(shell, argv);
		} finally {
			shell.close();
		}
		System.exit(res);
	}
}
