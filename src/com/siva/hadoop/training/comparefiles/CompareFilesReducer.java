package com.siva.hadoop.training.comparefiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompareFilesReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> list = new ArrayList<String>();

		for (Text value : values) {
			list.add(value.toString());
		}
		if (list.size() == 2) {
			String file1_data = list.get(0);
			int file1_index = file1_data.indexOf("->");
			String file1_name = file1_data.substring(0, file1_index);
			String file1_content = file1_data.substring(file1_index + 2);

			String file2_data = list.get(1);
			int file2_index = file2_data.indexOf("->");
			String file2_name = file2_data.substring(0, file2_index);
			String file2_content = file2_data.substring(file2_index + 2);

			if (!file1_content.equals(file2_content)) {
				context.write(new Text(file1_data), new Text(file2_data));
			}
		}
	};
}
